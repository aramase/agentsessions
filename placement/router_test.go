package placement_test

import (
	"context"
	"errors"
	"io"
	"maps"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/sqlitelog"
)

const targetActorKey = "ate-target-actor"

// fakeRouter stands in for atenet-router: one h2c address for every session, forwarding each call,
// frame by frame and undecoded, to the actor its ate-target-actor metadata names. It refuses a call
// that names no actor or more than one, which is stricter than atenet-router (that routes on the
// last value), so a call carrying a duplicated header fails here instead of passing by luck.
type fakeRouter struct {
	addr   string
	actors map[string]*grpc.ClientConn

	mu     sync.Mutex
	routed map[string]int
}

func startFakeRouter(t *testing.T, actors map[string]string) *fakeRouter {
	t.Helper()
	r := &fakeRouter{actors: map[string]*grpc.ClientConn{}, routed: map[string]int{}}
	for target, addr := range actors {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		r.actors[target] = conn
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	r.addr = lis.Addr().String()
	srv := grpc.NewServer(grpc.ForceServerCodecV2(rawCodec{}), grpc.UnknownServiceHandler(r.forward))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return r
}

func (r *fakeRouter) forward(_ any, in grpc.ServerStream) error {
	method, _ := grpc.MethodFromServerStream(in)
	md, _ := metadata.FromIncomingContext(in.Context())
	targets := md.Get(targetActorKey)
	if len(targets) != 1 {
		return status.Errorf(codes.InvalidArgument, "want exactly one %s, got %q", targetActorKey, targets)
	}
	conn, ok := r.actors[targets[0]]
	if !ok {
		return status.Errorf(codes.NotFound, "no actor %q", targets[0])
	}
	r.mu.Lock()
	r.routed[targets[0]]++
	r.mu.Unlock()

	ctx := metadata.NewOutgoingContext(in.Context(), metadata.Pairs(targetActorKey, targets[0]))
	out, err := conn.NewStream(ctx, &grpc.StreamDesc{ClientStreams: true, ServerStreams: true}, method, grpc.ForceCodecV2(rawCodec{}))
	if err != nil {
		return err
	}
	go func() {
		for {
			var frame rawFrame
			if err := in.RecvMsg(&frame); err != nil {
				_ = out.CloseSend()
				return
			}
			if err := out.SendMsg(&frame); err != nil {
				return
			}
		}
	}()
	for {
		var frame rawFrame
		if err := out.RecvMsg(&frame); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if err := in.SendMsg(&frame); err != nil {
			return err
		}
	}
}

func (r *fakeRouter) routedCounts() map[string]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return maps.Clone(r.routed)
}

// rawFrame is one gRPC message the router forwards without decoding.
type rawFrame struct{ b []byte }

// rawCodec carries rawFrames as their wire bytes. It names itself "proto" so the content-type the
// harness client and server negotiate passes through unchanged.
type rawCodec struct{}

func (rawCodec) Marshal(v any) (mem.BufferSlice, error) {
	return mem.BufferSlice{mem.SliceBuffer(v.(*rawFrame).b)}, nil
}

func (rawCodec) Unmarshal(data mem.BufferSlice, v any) error {
	v.(*rawFrame).b = data.Materialize()
	return nil
}

func (rawCodec) Name() string { return "proto" }

// actorHarness is a stateless harness that names the actor it runs as in the model request, so the
// recorded output shows which actor served the turn.
type actorHarness struct{ actor string }

func (h actorHarness) Describe(context.Context) (api.Descriptor, error) { return statelessEcho, nil }

func (h actorHarness) Run(ctx context.Context, s *api.Start, sink api.EventSink) error {
	text := ""
	if n := len(s.Inputs); n > 0 {
		text = s.Inputs[n-1].Text()
	}
	_, err := sink.Model(ctx, api.ModelRequest{Model: "echo", Messages: []api.Message{*api.TextMessage("user", h.actor+"|"+text)}})
	return err
}

// Two sessions share the router's address. With both turns' Connect streams open through it at the
// same time, each turn must reach its own session's actor and record only that actor's answer.
func TestConcurrentSessionsThroughOneRouterReachTheirOwnActors(t *testing.T) {
	sessions := []string{"s1", "s2"}
	servers := map[string]*harnessServer{}
	actors := map[string]string{}
	for _, uid := range sessions {
		target := "space/" + uid
		servers[target] = startHarnessServer(t, actorHarness{actor: target})
		actors[target] = servers[target].addr
	}
	router := startFakeRouter(t, actors)

	// Hold every model call until both turns are inside one, so neither stream can finish before
	// the other opens.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var arrived atomic.Int32
	both := make(chan struct{})
	model := func(mctx context.Context, req api.ModelRequest) (api.ModelResponse, error) {
		if arrived.Add(1) == int32(len(sessions)) {
			close(both)
		}
		select {
		case <-both:
		case <-mctx.Done():
			return api.ModelResponse{}, mctx.Err()
		}
		return echoagent.Model(mctx, req)
	}

	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	p := placement.New(&routedBackend{address: router.addr, desc: statelessEcho}, model)

	errs := make(chan error, len(sessions))
	for _, uid := range sessions {
		go func() {
			_, err := p.Exec(ctx, store.Session(uid), uid, []api.Message{*api.TextMessage("user", "from "+uid)}, 0)
			errs <- err
		}()
	}
	for range sessions {
		if err := <-errs; err != nil {
			t.Fatalf("exec through the router: %v", err)
		}
	}

	for _, uid := range sessions {
		target := "space/" + uid
		want := []string{"echo:" + target + "|from " + uid}
		if got := sessionOutputs(t, store.Session(uid)); !slices.Equal(got, want) {
			t.Errorf("session %s recorded %q, want %q", uid, got, want)
		}
		if got := servers[target].seenTargets(); !slices.Equal(got, []string{target}) {
			t.Errorf("actor %s served streams for %q, want only its own", target, got)
		}
	}
	if got, want := router.routedCounts(), map[string]int{"space/s1": 1, "space/s2": 1}; !maps.Equal(got, want) {
		t.Errorf("router routed %v, want %v", got, want)
	}
}

func sessionOutputs(t *testing.T, log *sqlitelog.Log) []string {
	t.Helper()
	recs, err := log.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, r := range recs {
		if r.Event.Kind == api.EventOutput && r.Event.Message != nil {
			out = append(out, r.Event.Message.Text())
		}
	}
	return out
}
