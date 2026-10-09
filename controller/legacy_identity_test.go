package controller_test

import (
	"context"
	"errors"
	"net"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/canon"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/harnesswire"
	"github.com/aramase/agentsessions/wire"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

var legacyExecutionIDPattern = regexp.MustCompile(`^legacy-[0-9a-f]{64}$`)
var modernExecutionIDPattern = regexp.MustCompile(`^[0-9a-f]{16}$`)

// Consume the real recorded effects before failing. Failing before Model would not exercise
// retry after a served completion, nor prove that the appended ERROR stays out of the stream.
type retryLegacyHarness struct {
	echoagent.Harness
	starts chan<- string
	fail   error
}

func (h retryLegacyHarness) Run(ctx context.Context, start *api.Start, sink api.EventSink) error {
	h.starts <- start.ExecutionID
	if err := h.Harness.Run(ctx, start, sink); err != nil {
		return err
	}
	return h.fail
}

func legacyWireHarness(t *testing.T, har api.Harness) api.Harness {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	v1.RegisterHarnessServer(server, harnesswire.NewServer(har))
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	t.Cleanup(func() { _ = listener.Close() })
	conn, err := grpc.NewClient("passthrough:///legacy-harness",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return harnesswire.NewClientHarness(v1.NewHarnessClient(conn))
}

func TestLegacyCompatibilityIdentityStableAcrossRetryingControllers(t *testing.T) {
	for _, transport := range []string{"local", "wire"} {
		t.Run(transport, func(t *testing.T) {
			log, original := v012Log(t, "crashed-output")
			var observed []eventlog.Record
			modelCalls := 0
			model := func(ctx context.Context, request api.ModelRequest) (api.ModelResponse, error) {
				modelCalls++
				return echoagent.Model(ctx, request)
			}
			opts := []controller.Option{
				controller.WithSessionUID("retry-session"),
				controller.WithObserver(controller.Observer{OnRecord: func(record eventlog.Record) {
					observed = append(observed, record)
				}}),
			}
			starts := make(chan string, 2)
			injected := errors.New("retryable harness failure after recorded completion")
			var ids []string
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			for attempt, failure := range []error{injected, nil} {
				c, err := controller.New(log, model, opts...)
				if err != nil {
					t.Fatal(err)
				}
				var har api.Harness = retryLegacyHarness{starts: starts, fail: failure}
				if transport == "wire" {
					har = legacyWireHarness(t, har)
				}
				resumed, err := c.Resume(ctx, har)
				if !resumed || attempt == 0 && (err == nil || !strings.Contains(err.Error(), injected.Error())) || attempt == 1 && err != nil {
					t.Fatalf("attempt %d Resume = %v, %v", attempt, resumed, err)
				}
				select {
				case id := <-starts:
					ids = append(ids, id)
					if !legacyExecutionIDPattern.MatchString(id) {
						t.Errorf("attempt %d compatibility Start.ExecutionID = %q", attempt, id)
					}
				default:
					t.Fatal("Resume did not reach the harness")
				}
			}
			if ids[0] != ids[1] {
				t.Errorf("retry changed compatibility identity: %q -> %q", ids[0], ids[1])
			}
			if modelCalls != 0 {
				t.Errorf("retry repeated recorded model effect %d times", modelCalls)
			}
			after, err := log.Read(1)
			if err != nil {
				t.Fatal(err)
			}
			if len(after) != len(original)+2 || !reflect.DeepEqual(after[:len(original)], original) {
				t.Fatalf("retry rewrote prefix or repeated effects: %#v", after)
			}
			if after[len(original)].Event.Kind != api.EventError || after[len(original)+1].Event.Kind != api.EventEnd {
				t.Fatalf("retry terminal kinds = %s, %s", after[len(original)].Event.Kind, after[len(original)+1].Event.Kind)
			}
			if !reflect.DeepEqual(observed, after[len(original):]) {
				t.Fatal("observer did not receive the durable ERROR/END records")
			}
			for _, record := range after {
				if record.Event.ExecutionID != "" || wire.EventToProto(record.Event).GetExecutionId() != "" {
					t.Errorf("compatibility ID leaked into journal/wire record %d: %#v", record.Seq, record.Event)
				}
			}
			if err := log.Verify(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestLegacyCompatibilityIdentityIsScopedToHostSessionUID(t *testing.T) {
	original := v012Records(t, "completed")
	var ids []string
	for _, uid := range []string{"host-session-a", "host-session-b"} {
		log := eventlog.AsStore(eventlog.NewFrom(original))
		c, err := controller.New(log, echoagent.Model, controller.WithSessionUID(uid))
		if err != nil {
			t.Fatal(err)
		}
		var starts []api.Start
		if _, err := c.Replay(t.Context(), observedLegacyHarness{&starts}); err != nil {
			t.Fatal(err)
		}
		if len(starts) != 1 {
			t.Fatalf("harness runs = %d", len(starts))
		}
		ids = append(ids, starts[0].ExecutionID)
		if !legacyExecutionIDPattern.MatchString(ids[len(ids)-1]) {
			t.Errorf("UID %q compatibility identity = %q", uid, ids[len(ids)-1])
		}
		after, err := log.Read(1)
		if err != nil || !reflect.DeepEqual(after, original) {
			t.Fatalf("Replay changed identical fixture bytes: %v", err)
		}
	}
	if ids[0] == ids[1] {
		t.Errorf("identical fixture bytes in different host sessions share identity %q", ids[0])
	}
}

// Synthetic records have no effects, so a capturing harness can run both Replay and Resume.
// firstSeq may be non-one to distinguish original record.Seq from a slice index after loading.
func legacyIdentityRecords(t *testing.T, firstSeq int64, events ...api.Event) []eventlog.Record {
	t.Helper()
	var records []eventlog.Record
	prev := ""
	for i, event := range events {
		seq := firstSeq + int64(i)
		hash, err := canon.HashRecord(prev, seq, event)
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, eventlog.Record{Seq: seq, PrevHash: prev, Hash: hash, Fence: 1, Event: event})
		prev = hash
	}
	if err := eventlog.NewFrom(records).Verify(); err != nil {
		t.Fatal(err)
	}
	return records
}

type legacyStartOnlyHarness struct {
	echoagent.Harness
	starts *[]api.Start
}

func (h legacyStartOnlyHarness) Run(_ context.Context, start *api.Start, _ api.EventSink) error {
	*h.starts = append(*h.starts, *start)
	return nil
}

func captureLegacyIdentity(t *testing.T, records []eventlog.Record, resume bool) string {
	t.Helper()
	c, err := controller.New(eventlog.AsStore(eventlog.NewFrom(records)), echoagent.Model,
		controller.WithSessionUID("identity-session"))
	if err != nil {
		t.Fatal(err)
	}
	var starts []api.Start
	har := legacyStartOnlyHarness{starts: &starts}
	if resume {
		if resumed, err := c.Resume(t.Context(), har); !resumed || err != nil {
			t.Fatalf("Resume = %v, %v", resumed, err)
		}
	} else if _, err := c.Replay(t.Context(), har); err != nil {
		t.Fatal(err)
	}
	if len(starts) != 1 {
		t.Fatalf("harness runs = %d", len(starts))
	}
	return starts[0].ExecutionID
}

func TestLegacyCompatibilityIdentityUsesOriginalPositionAndCanonicalContent(t *testing.T) {
	input := api.Event{Kind: api.EventInput, Message: api.TextMessage("user", "same")}
	changedActor := input
	changedActor.Actor.Principal = "different-principal"
	for _, resume := range []bool{false, true} {
		operation := map[bool]string{false: "Replay", true: "Resume"}[resume]
		t.Run(operation, func(t *testing.T) {
			base := captureLegacyIdentity(t, legacyIdentityRecords(t, 1, input), resume)
			if !legacyExecutionIDPattern.MatchString(base) {
				t.Errorf("compatibility identity = %q", base)
			}
			for _, tc := range []struct {
				name  string
				seq   int64
				event api.Event
			}{
				{"same content at different original seq", 42, input},
				{"changed input at same seq", 1, api.Event{Kind: api.EventInput, Message: api.TextMessage("user", "changed")}},
				{"changed canonical event outside message", 1, changedActor},
			} {
				t.Run(tc.name, func(t *testing.T) {
					got := captureLegacyIdentity(t, legacyIdentityRecords(t, tc.seq, tc.event), resume)
					if got == base {
						t.Errorf("changed original seq/content reused identity %q", got)
					}
				})
			}
		})
	}
}

func TestLegacyCompatibilityReplayUsesFirstInputAndResumeUsesLastInput(t *testing.T) {
	first := api.Event{Kind: api.EventInput, Message: api.TextMessage("user", "same")}
	end := api.Event{Kind: api.EventEnd, End: &api.HarnessEnd{State: "COMPLETED"}}
	// Identical inputs at different positions must still identify distinct turns.
	multi := legacyIdentityRecords(t, 1, first, end, first)
	firstID := captureLegacyIdentity(t, legacyIdentityRecords(t, 1, first), false)
	lastID := captureLegacyIdentity(t, legacyIdentityRecords(t, 3, first), true)
	replayID := captureLegacyIdentity(t, multi, false)
	resumeID := captureLegacyIdentity(t, multi, true)
	if replayID != firstID || resumeID != lastID {
		t.Errorf("wrong anchor: Replay=%q first=%q; Resume=%q last=%q", replayID, firstID, resumeID, lastID)
	}
	if replayID == resumeID {
		t.Errorf("first/last INPUT at distinct original positions share identity %q", replayID)
	}
	usage := api.Event{Kind: api.EventUsage, Usage: &api.Usage{Model: "echo", InputTokens: 1}}
	withPreamble := captureLegacyIdentity(t, legacyIdentityRecords(t, 1, usage, first), false)
	inputAtTwo := captureLegacyIdentity(t, legacyIdentityRecords(t, 2, first), false)
	if withPreamble != inputAtTwo {
		t.Errorf("Replay anchored to preamble rather than first INPUT: %q != %q", withPreamble, inputAtTwo)
	}
}

func TestLegacyCompatibilityReplayWithoutInputUsesFirstLegacyRecord(t *testing.T) {
	lifecycle := api.Event{Kind: api.EventLifecycle, Lifecycle: &api.Lifecycle{Kind: api.LifecycleFork}}
	usage := api.Event{Kind: api.EventUsage, Usage: &api.Usage{Model: "echo", InputTokens: 1}}
	errorEvent := api.Event{Kind: api.EventError, Err: &api.Error{Description: "old failure"}}
	base := captureLegacyIdentity(t, legacyIdentityRecords(t, 2, usage), false)
	if !legacyExecutionIDPattern.MatchString(base) {
		t.Errorf("no-input compatibility identity = %q", base)
	}
	withContinuation := captureLegacyIdentity(t, legacyIdentityRecords(t, 1, lifecycle, usage, errorEvent), false)
	if withContinuation != base {
		t.Errorf("no-input Replay used lifecycle/continuation rather than first legacy record: %q != %q", withContinuation, base)
	}
	changed := api.Event{Kind: api.EventUsage, Usage: &api.Usage{Model: "echo", InputTokens: 2}}
	if got := captureLegacyIdentity(t, legacyIdentityRecords(t, 2, changed), false); got == base {
		t.Errorf("changed first legacy record reused fallback identity %q", got)
	}
}

func TestLegacyCompatibilityReplayIdentitySurvivesContinuationAndModernExec(t *testing.T) {
	log, original := v012Log(t, "crashed-output")
	var starts []api.Start
	har := observedLegacyHarness{&starts}
	c, err := v012Controller(log, echoagent.Model)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Replay(t.Context(), har); err != nil {
		t.Fatal(err)
	}
	if resumed, err := c.Resume(t.Context(), har); !resumed || err != nil {
		t.Fatalf("Resume = %v, %v", resumed, err)
	}
	c, err = v012Controller(log, echoagent.Model)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := log.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Replay(t.Context(), har); err != nil {
		t.Fatal(err)
	}
	if len(starts) != 3 {
		t.Fatalf("harness runs = %d", len(starts))
	}
	if !legacyExecutionIDPattern.MatchString(starts[0].ExecutionID) {
		t.Errorf("legacy namespace = %q", starts[0].ExecutionID)
	}
	if starts[0].ExecutionID != starts[1].ExecutionID || starts[0].ExecutionID != starts[2].ExecutionID {
		t.Errorf("single-turn identity changed across Replay/Resume/continuation: %#v", starts)
	}
	if !reflect.DeepEqual(recovered[:len(original)], original) {
		t.Fatal("recovery changed original records")
	}
	for _, record := range recovered {
		if record.Event.ExecutionID != "" {
			t.Errorf("legacy recovery journaled compatibility identity at seq %d", record.Seq)
		}
	}
	if after, err := log.Read(1); err != nil || !reflect.DeepEqual(after, recovered) {
		t.Fatalf("Replay changed recovered journal: %v", err)
	}
	if err := c.Exec(t.Context(), har, []api.Message{*api.TextMessage("user", "modern")}, int64(len(recovered))); err != nil {
		t.Fatal(err)
	}
	modernID := starts[3].ExecutionID
	if !modernExecutionIDPattern.MatchString(modernID) || legacyExecutionIDPattern.MatchString(modernID) {
		t.Errorf("modern namespace changed to %q", modernID)
	}
	after, err := log.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range after[len(recovered):] {
		if record.Event.ExecutionID != modernID {
			t.Errorf("modern journal ID = %q, want Start ID %q", record.Event.ExecutionID, modernID)
		}
	}
	if err := log.Verify(); err != nil {
		t.Fatal(err)
	}
}

func TestLegacyCompatibilityUnscopedRunsFailClosed(t *testing.T) {
	for _, explicitEmpty := range []bool{false, true} {
		for _, tc := range []struct {
			fixture string
			resume  bool
		}{
			{"completed", false},
			{"crashed-output", false},
			{"crashed-input", true},
			{"crashed-output", true},
		} {
			operation := map[bool]string{false: "Replay", true: "Resume"}[tc.resume]
			name := tc.fixture + "/" + operation + "/UID=" + map[bool]string{false: "omitted", true: "empty"}[explicitEmpty]
			t.Run(name, func(t *testing.T) {
				log, original := v012Log(t, tc.fixture)
				modelCalls, observed := 0, 0
				opts := []controller.Option{controller.WithObserver(controller.Observer{OnRecord: func(eventlog.Record) { observed++ }})}
				if explicitEmpty {
					opts = append(opts, controller.WithSessionUID(""))
				}
				c, err := controller.New(log, func(ctx context.Context, request api.ModelRequest) (api.ModelResponse, error) {
					modelCalls++
					return echoagent.Model(ctx, request)
				}, opts...)
				if err != nil {
					t.Fatal(err)
				}
				var starts []api.Start
				har := observedLegacyHarness{&starts}
				if tc.resume {
					resumed, err := c.Resume(t.Context(), har)
					if resumed || !errors.Is(err, controller.ErrMissingSessionUID) {
						t.Errorf("unscoped Resume = %v, %v; want false, ErrMissingSessionUID", resumed, err)
					}
				} else if _, err := c.Replay(t.Context(), har); !errors.Is(err, controller.ErrMissingSessionUID) {
					t.Errorf("unscoped Replay = %v; want ErrMissingSessionUID", err)
				}
				if len(starts) != 0 || modelCalls != 0 || observed != 0 {
					t.Errorf("unscoped run invoked harness/model/appends: %d/%d/%d", len(starts), modelCalls, observed)
				}
				if after, err := log.Read(1); err != nil || !reflect.DeepEqual(after, original) {
					t.Errorf("unscoped run changed journal: %v", err)
				}
			})
		}
	}
}

func TestLegacyCompatibilityCompletedResumeNeedsNoSessionUID(t *testing.T) {
	log, original := v012Log(t, "completed")
	c, err := controller.New(log, echoagent.Model)
	if err != nil {
		t.Fatal(err)
	}
	var starts []api.Start
	if resumed, err := c.Resume(t.Context(), observedLegacyHarness{&starts}); resumed || err != nil {
		t.Fatalf("completed unscoped Resume = %v, %v", resumed, err)
	}
	if len(starts) != 0 {
		t.Fatal("completed Resume invoked harness")
	}
	if after, err := log.Read(1); err != nil || !reflect.DeepEqual(after, original) {
		t.Fatalf("completed unscoped Resume changed journal: %v", err)
	}
}
