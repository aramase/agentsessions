package conformance_test

import (
	"context"
	"errors"
	"math"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/harnesswire"
	"github.com/aramase/agentsessions/wire"
)

type argumentHarness struct{ args map[string]any }

func (argumentHarness) Describe(context.Context) (api.Descriptor, error) {
	return api.Descriptor{ID: "tool-args", Capabilities: api.Capabilities{Resumability: api.ResumabilityStatelessReplay}}, nil
}

func (h argumentHarness) Run(ctx context.Context, _ *api.Start, sink api.EventSink) error {
	_, err := sink.ToolCall(ctx, api.ToolCall{ID: "call-1", Tool: "read", Args: h.args, Mediation: api.MediationControllerMediated, IdempotencyKey: "key-1"})
	return err
}

// A permissive wire conversion would drop a channel to nil or turn a NaN into a string before
// the controller could validate it. The real gRPC bridge must fail before intent or execution.
func TestWireToolArgumentsRejectedBeforeEffects(t *testing.T) {
	for _, test := range []struct {
		name  string
		value any
	}{
		{"channel", make(chan int)},
		{"nonfinite", math.NaN()},
		{"struct", struct{ Name string }{"private"}},
		{"unsafe integer", int64(9007199254740993)},
	} {
		t.Run(test.name, func(t *testing.T) {
			log := eventlog.AsStore(eventlog.New())
			attempts := 0
			c, err := controller.New(log, nil, controller.WithToolExecutor(func(context.Context, api.ToolCall) (api.ToolResult, error) {
				attempts++
				return api.ToolResult{}, nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			h := wireHarnessFrom(t, argumentHarness{args: map[string]any{"secret": test.value}})
			err = c.Exec(t.Context(), h, []api.Message{*api.TextMessage("user", "read")}, 0)
			if err == nil || attempts != 0 {
				t.Fatalf("invalid remote args accepted: attempts=%d err=%v", attempts, err)
			}
			recs, err := log.Read(1)
			if err != nil {
				t.Fatal(err)
			}
			for _, rec := range recs {
				if rec.Event.Kind == api.EventToolCall || rec.Event.Kind == api.EventToolResult || rec.Event.Kind == api.EventEnd {
					t.Fatal("invalid args recorded tool effects or successful END")
				}
			}
		})
	}
}

// A raw remote harness can bypass streamSink validation. Reject invalid protobuf numbers before
// AsMap turns them into ordinary strings at the receiving call boundary.
func TestRawWireToolArgumentsRejectedBeforeEffects(t *testing.T) {
	for _, value := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		t.Run(structpb.NewNumberValue(value).String(), func(t *testing.T) {
			lis := bufconn.Listen(1 << 20)
			server := grpc.NewServer()
			v1.RegisterHarnessServer(server, rawToolServer{value: value})
			go server.Serve(lis)
			t.Cleanup(server.Stop)
			conn, err := grpc.NewClient("passthrough:///raw-harness", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { conn.Close() })
			attempts := 0
			c, err := controller.New(eventlog.AsStore(eventlog.New()), nil, controller.WithToolExecutor(func(context.Context, api.ToolCall) (api.ToolResult, error) { attempts++; return api.ToolResult{}, nil }))
			if err != nil {
				t.Fatal(err)
			}
			err = c.Exec(t.Context(), harnesswire.NewClientHarness(v1.NewHarnessClient(conn)), []api.Message{*api.TextMessage("user", "read")}, 0)
			if err == nil || attempts != 0 {
				t.Fatalf("invalid raw args accepted: attempts=%d err=%v", attempts, err)
			}
		})
	}
}

type rawToolServer struct {
	v1.UnimplementedHarnessServer
	value float64
}

func (s rawToolServer) Connect(stream v1.Harness_ConnectServer) error {
	start, err := stream.Recv()
	if err != nil {
		return err
	}
	err = stream.Send(&v1.Event{
		ExecutionId: start.GetExecutionId(), Kind: v1.EventKind_EVENT_TOOL_CALL,
		Body: &v1.Event_Tool{Tool: &v1.ToolCall{Id: "call-1", Tool: "read", Mediation: v1.Mediation_MEDIATION_CONTROLLER_MEDIATED, IdempotencyKey: "key-1", Args: &structpb.Struct{Fields: map[string]*structpb.Value{"number": structpb.NewNumberValue(s.value)}}}},
	})
	if err != nil {
		return err
	}
	if _, err = stream.Recv(); err != nil {
		return err
	}
	return stream.Send(&v1.Event{ExecutionId: start.GetExecutionId(), Kind: v1.EventKind_EVENT_END, Body: &v1.Event_End{End: &v1.HarnessEnd{State: "COMPLETED"}}})
}

type errorThenCompletedServer struct {
	v1.UnimplementedHarnessServer
	reported *v1.Error
}

func (s errorThenCompletedServer) Connect(stream v1.Harness_ConnectServer) error {
	start, err := stream.Recv()
	if err != nil {
		return err
	}
	if err := stream.Send(&v1.Event{
		ExecutionId: start.GetExecutionId(), Kind: v1.EventKind_EVENT_ERROR,
		Body: &v1.Event_Error{Error: s.reported},
	}); err != nil {
		return err
	}
	return stream.Send(&v1.Event{
		ExecutionId: start.GetExecutionId(), Kind: v1.EventKind_EVENT_END,
		Body: &v1.Event_End{End: &v1.HarnessEnd{State: "COMPLETED"}},
	})
}

func TestUnrecognizedWireErrorRetainsCompletionBehavior(t *testing.T) {
	for _, test := range []struct {
		name     string
		reported *v1.Error
	}{
		{"generic error", &v1.Error{Code: int32(codes.Internal), Description: "fixture error"}},
		{"unrelated invalid argument", &v1.Error{Code: int32(codes.InvalidArgument), Description: "fixture error"}},
		{"rejection description with wrong code", &v1.Error{Code: int32(codes.Internal), Description: wire.ErrInvalidToolArgs.Error()}},
		{"missing payload", nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			lis := bufconn.Listen(1 << 20)
			server := grpc.NewServer()
			v1.RegisterHarnessServer(server, errorThenCompletedServer{reported: test.reported})
			go server.Serve(lis)
			t.Cleanup(server.Stop)
			conn, err := grpc.NewClient("passthrough:///error-harness", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { conn.Close() })
			log := eventlog.AsStore(eventlog.New())
			c, err := controller.New(log, nil)
			if err != nil {
				t.Fatal(err)
			}
			h := harnesswire.NewClientHarness(v1.NewHarnessClient(conn))
			if err := c.Exec(t.Context(), h, nil, 0); err != nil {
				t.Fatalf("unrecognized error prevented completion: %v", err)
			}
			recs, err := log.Read(1)
			if err != nil {
				t.Fatal(err)
			}
			last := recs[len(recs)-1].Event
			if last.Kind != api.EventEnd || last.End == nil || last.End.State != "COMPLETED" {
				t.Fatalf("want completed execution, got %+v", last)
			}
			if _, err := c.Replay(t.Context(), h); err != nil {
				t.Fatalf("unrecognized error prevented reconstruction: %v", err)
			}
		})
	}
}

type handledArgumentErrorHarness struct{ reject, interrupt bool }

func (handledArgumentErrorHarness) Describe(context.Context) (api.Descriptor, error) {
	return argumentHarness{}.Describe(context.Background())
}

func (h handledArgumentErrorHarness) Run(ctx context.Context, start *api.Start, sink api.EventSink) error {
	if h.reject {
		_, _ = sink.ToolCall(ctx, api.ToolCall{ID: "call-1", Tool: "read", Args: map[string]any{"value": make(chan int)}, Mediation: api.MediationControllerMediated, IdempotencyKey: "key-1"})
	}
	_ = (argumentHarness{args: map[string]any{"count": 2}}).Run(ctx, start, sink)
	if h.interrupt {
		return errors.New("interrupted")
	}
	return nil
}

type handlesRemoteRunError struct{ remote api.Harness }

func (h handlesRemoteRunError) Describe(ctx context.Context) (api.Descriptor, error) {
	return h.remote.Describe(ctx)
}

func (h handlesRemoteRunError) Run(ctx context.Context, start *api.Start, sink api.EventSink) error {
	_ = h.remote.Run(ctx, start, sink)
	_ = (argumentHarness{args: map[string]any{"count": 2}}).Run(ctx, start, sink)
	return nil
}

func TestHandledInvalidToolArgumentsFailOnBothTransports(t *testing.T) {
	for _, path := range []string{"live", "replay", "resume result", "resume intent"} {
		for _, transport := range []string{"direct", "remote", "handled remote error"} {
			t.Run(path+"/"+transport, func(t *testing.T) {
				log := eventlog.AsStore(eventlog.New())
				if path != "live" {
					original, err := controller.New(log, nil, controller.WithToolExecutor(func(context.Context, api.ToolCall) (api.ToolResult, error) {
						if path == "resume intent" {
							return api.ToolResult{}, errors.New("interrupted")
						}
						return api.ToolResult{Output: map[string]any{"receipt": "recorded"}}, nil
					}))
					if err != nil {
						t.Fatal(err)
					}
					err = original.Exec(t.Context(), handledArgumentErrorHarness{interrupt: path != "replay"}, nil, 0)
					if path == "replay" && err != nil || path != "replay" && err == nil {
						t.Fatalf("fixture execution: %v", err)
					}
				}
				attempts := 0
				c, err := controller.New(log, nil, controller.WithToolExecutor(func(context.Context, api.ToolCall) (api.ToolResult, error) {
					attempts++
					return api.ToolResult{}, nil
				}))
				if err != nil {
					t.Fatal(err)
				}
				var h api.Harness = handledArgumentErrorHarness{reject: true}
				if transport != "direct" {
					h = wireHarnessFrom(t, h)
					if transport == "handled remote error" {
						h = handlesRemoteRunError{remote: h}
					}
				}
				before, err := log.Head()
				if err != nil {
					t.Fatal(err)
				}
				wantErr := controller.ErrReplayDiverged
				switch path {
				case "live":
					wantErr = wire.ErrInvalidToolArgs
					err = c.Exec(t.Context(), h, nil, before)
				case "replay":
					_, err = c.Replay(t.Context(), h)
				default:
					_, err = c.Resume(t.Context(), h)
				}
				if !errors.Is(err, wantErr) || attempts != 0 {
					t.Fatalf("handled rejection escaped: attempts=%d err=%v", attempts, err)
				}
				recs, err := log.Read(before + 1)
				if err != nil {
					t.Fatal(err)
				}
				for _, rec := range recs {
					if rec.Event.Kind != api.EventError && rec.Event.Kind != api.EventExecutionStart {
						t.Fatalf("rejection appended %s", rec.Event.Kind)
					}
				}
			})
		}
	}
}

type nilRejectionSink struct {
	api.EventSink
	reported *error
}

func (s nilRejectionSink) RejectToolCall(err error) error {
	*s.reported = err
	return nil
}

type nilRejectionHarness struct {
	remote   api.Harness
	reported error
}

func (h *nilRejectionHarness) Describe(ctx context.Context) (api.Descriptor, error) {
	return h.remote.Describe(ctx)
}

func (h *nilRejectionHarness) Run(ctx context.Context, start *api.Start, sink api.EventSink) error {
	return h.remote.Run(ctx, start, nilRejectionSink{EventSink: sink, reported: &h.reported})
}

func TestRemoteValidationRejectionCannotBeSuppressedByNilHook(t *testing.T) {
	log := eventlog.AsStore(eventlog.New())
	attempts := 0
	c, err := controller.New(log, nil, controller.WithToolExecutor(func(context.Context, api.ToolCall) (api.ToolResult, error) {
		attempts++
		return api.ToolResult{}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	h := &nilRejectionHarness{remote: wireHarnessFrom(t, handledArgumentErrorHarness{reject: true})}
	err = c.Exec(t.Context(), h, nil, 0)
	if !errors.Is(err, wire.ErrInvalidToolArgs) || !errors.Is(h.reported, wire.ErrInvalidToolArgs) || attempts != 0 {
		t.Fatalf("hook suppressed rejection: attempts=%d reported=%v err=%v", attempts, h.reported, err)
	}
	recs, err := log.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	for _, rec := range recs {
		if rec.Event.Kind == api.EventEnd {
			t.Fatal("nil hook certified successful completion")
		}
	}
}

func TestWireToolRequestChangesFailReplayAndRecovery(t *testing.T) {
	for _, recovery := range []bool{false, true} {
		t.Run(map[bool]string{false: "replay", true: "resume"}[recovery], func(t *testing.T) {
			log := eventlog.AsStore(eventlog.New())
			c, err := controller.New(log, nil, controller.WithToolExecutor(func(context.Context, api.ToolCall) (api.ToolResult, error) {
				if recovery {
					return api.ToolResult{}, errors.New("interrupted")
				}
				return api.ToolResult{Output: map[string]any{"receipt": "recorded"}}, nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			err = c.Exec(t.Context(), wireHarnessFrom(t, argumentHarness{args: map[string]any{"count": 2}}), []api.Message{*api.TextMessage("user", "read")}, 0)
			if !recovery && err != nil || recovery && err == nil {
				t.Fatalf("fixture: %v", err)
			}
			attempts := 0
			fresh, err := controller.New(log, nil, controller.WithToolExecutor(func(context.Context, api.ToolCall) (api.ToolResult, error) { attempts++; return api.ToolResult{}, nil }))
			if err != nil {
				t.Fatal(err)
			}
			h := wireHarnessFrom(t, argumentHarness{args: map[string]any{"count": 3}})
			if recovery {
				_, err = fresh.Resume(t.Context(), h)
			} else {
				_, err = fresh.Replay(t.Context(), h)
			}
			if !errors.Is(err, controller.ErrReplayDiverged) || attempts != 0 {
				t.Fatalf("remote divergence accepted: attempts=%d err=%v", attempts, err)
			}
		})
	}
}
