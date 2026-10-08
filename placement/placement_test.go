package placement_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/harness/counteragent"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/harnesswire"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/runtime/local"
	"github.com/aramase/agentsessions/runtime/remote"
	"github.com/aramase/agentsessions/runtime/substrate"
	"github.com/aramase/agentsessions/sqlitelog"
)

// newLocalPlacer builds a Placer over a fresh runtime/local backend and closes the backend's harness
// server when the test ends.
func newLocalPlacer(t *testing.T, h api.Harness, opts ...placement.Option) *placement.Placer {
	t.Helper()
	b := local.New(h)
	t.Cleanup(func() { _ = b.Close() })
	return placement.New(b, echoagent.Model, opts...)
}

// TestPlacerExecRoutesThroughRuntime proves a turn placed via the Placer runs through Runtime.Create
// and the backend-provided harness, binds the controller to a log-minted fence stamped on the
// incarnation, and produces a verifiable journal — the in-process realization of "the controller
// drives the Runtime SPI" instead of a co-located controller.
func TestPlacerExecRoutesThroughRuntime(t *testing.T) {
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	log := store.Session("s")

	p := newLocalPlacer(t, echoagent.Harness{})
	inc, err := p.Exec(context.Background(), log, "s", []api.Message{*api.TextMessage("user", "hi")}, 0)
	if err != nil {
		t.Fatalf("placed exec: %v", err)
	}
	if inc.Runtime != "local" {
		t.Fatalf("turn must be placed on a runtime, got %q", inc.Runtime)
	}
	if inc.FenceToken == 0 {
		t.Fatal("the Placer must stamp the minted fence on the incarnation")
	}
	head, err := log.Head()
	if err != nil {
		t.Fatal(err)
	}
	if head == 0 {
		t.Fatal("the placed turn produced no records")
	}
	if err := log.Verify(); err != nil {
		t.Fatalf("verify after placed exec: %v", err)
	}
}

func TestPlacerStructuredLogging(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}))
	p := newLocalPlacer(t, echoagent.Harness{}, placement.WithLogger(logger))

	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if _, err := p.Exec(
		context.Background(),
		store.Session("logged-session"),
		"logged-session",
		[]api.Message{*api.TextMessage("user", "do-not-log-this")},
		0,
	); err != nil {
		t.Fatal(err)
	}

	records := decodeLogRecords(t, output.Bytes())
	execFinished := findLogRecord(t, records, "placement", "exec", "finish")
	if execFinished["level"] != "DEBUG" || execFinished["outcome"] != "success" {
		t.Fatalf("unexpected completion log: %v", execFinished)
	}
	if execFinished["session_uid"] != "logged-session" || execFinished["runtime"] != "local" {
		t.Fatalf("missing operation identifiers: %v", execFinished)
	}
	if execFinished["duration_ms"] == nil {
		t.Fatalf("missing operation duration: %v", execFinished)
	}
	resolveFinished := findLogRecord(t, records, "placement", "resolve_execution_path", "finish")
	if resolveFinished["decision"] != "accepted" || resolveFinished["harness_id"] != "echo" {
		t.Fatalf("missing placement decision: %v", resolveFinished)
	}
	if strings.Contains(output.String(), "do-not-log-this") {
		t.Fatalf("log contains message contents: %s", output.String())
	}
	for _, record := range records {
		if _, ok := record["fence_token"]; ok {
			t.Fatalf("log contains a fence token: %v", record)
		}
	}

	output.Reset()
	refused := newLocalPlacer(t, memSnapshotHarness{}, placement.WithLogger(logger))
	if _, err := refused.Exec(context.Background(), store.Session("refused"), "refused", nil, 0); !errors.Is(err, placement.ErrUnplaceable) {
		t.Fatalf("expected ErrUnplaceable, got %v", err)
	}
	records = decodeLogRecords(t, output.Bytes())
	execFinished = findLogRecord(t, records, "placement", "exec", "finish")
	if execFinished["level"] != "INFO" || execFinished["outcome"] != "error" || execFinished["error_kind"] != "unplaceable" {
		t.Fatalf("unexpected failure log: %v", execFinished)
	}
	resolveFinished = findLogRecord(t, records, "placement", "resolve_execution_path", "finish")
	if resolveFinished["decision"] != "refused" || resolveFinished["error_kind"] != "unplaceable" {
		t.Fatalf("missing refusal decision: %v", resolveFinished)
	}
}

func TestPlacerPreservesIncarnationAndPairsCloseLogs(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}))
	p := newLocalPlacer(t, echoagent.Harness{},
		placement.WithLogger(logger),
		placement.WithDialer(func(string) (api.Harness, func() error, error) {
			return nil, nil, errors.New("dial failed")
		}),
	)

	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	inc, err := p.Exec(context.Background(), store.Session("dial-failure"), "dial-failure", nil, 0)
	if err == nil {
		t.Fatal("expected dial failure")
	}
	if inc.ID != "dial-failure" || inc.Runtime != "local" {
		t.Fatalf("post-create failure lost incarnation: %+v", inc)
	}

	output.Reset()
	p = newLocalPlacer(t, echoagent.Harness{},
		placement.WithLogger(logger),
		placement.WithDialer(func(string) (api.Harness, func() error, error) {
			return echoagent.Harness{}, func() error { return errors.New("close failed") }, nil
		}),
	)
	if _, err := p.Exec(context.Background(), store.Session("close-failure"), "close-failure", nil, 0); err != nil {
		t.Fatal(err)
	}
	records := decodeLogRecords(t, output.Bytes())
	findLogRecord(t, records, "placement", "close_harness", "start")
	finished := findLogRecord(t, records, "placement", "close_harness", "finish")
	if finished["error_kind"] != "harness_close_failed" || finished["duration_ms"] == nil {
		t.Fatalf("close log does not follow operation schema: %v", finished)
	}
}

func decodeLogRecords(t *testing.T, data []byte) []map[string]any {
	t.Helper()
	var records []map[string]any
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		var record map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatalf("decode log: %v", err)
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan logs: %v", err)
	}
	return records
}

func findLogRecord(t *testing.T, records []map[string]any, component, operation, phase string) map[string]any {
	t.Helper()
	for _, record := range records {
		if record["component"] == component && record["operation"] == operation && record["phase"] == phase {
			return record
		}
	}
	t.Fatalf("missing log component=%q operation=%q phase=%q: %v", component, operation, phase, records)
	return nil
}

// TestPlacerFenceBinding proves the fence the Placer stamps on the incarnation is the SAME token the
// controller appended under: a second placement supersedes the first, so a stale controller bound to
// the first incarnation's fence would be fenced out. Here we assert the returned fence is monotonic
// and matches what the log advanced to.
func TestPlacerFenceBinding(t *testing.T) {
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	log := store.Session("s")
	p := newLocalPlacer(t, echoagent.Harness{})

	inc1, err := p.Exec(context.Background(), log, "s", []api.Message{*api.TextMessage("user", "one")}, 0)
	if err != nil {
		t.Fatal(err)
	}
	head, _ := log.Head()
	inc2, err := p.Exec(context.Background(), log, "s", []api.Message{*api.TextMessage("user", "two")}, head)
	if err != nil {
		t.Fatal(err)
	}
	if inc2.FenceToken <= inc1.FenceToken {
		t.Fatalf("each placement must mint a strictly newer fence: %d then %d", inc1.FenceToken, inc2.FenceToken)
	}
	// The stamped fence is REAL: a controller bound to the first (now superseded) incarnation's fence
	// is fenced out on append. expected_last_seq is current, so the failure isolates the fence.
	head2, _ := log.Head()
	stale, err := controller.New(log, echoagent.Model, controller.WithFence(inc1.FenceToken))
	if err != nil {
		t.Fatal(err)
	}
	if err := stale.Exec(context.Background(), echoagent.Harness{}, []api.Message{*api.TextMessage("user", "stale")}, head2); !errors.Is(err, eventlog.ErrFenced) {
		t.Fatalf("a controller bound to the superseded fence must be fenced out, got %v", err)
	}
	if err := log.Verify(); err != nil {
		t.Fatalf("verify: %v", err)
	}
}

// memSnapshotHarness declares REQUIRES_MEMORY_SNAPSHOT — placeable on substrate, refused on a
// filesystem-only backend. Its Run is a no-op (the gate decision, not execution, is under test).
type memSnapshotHarness struct{}

func (memSnapshotHarness) Describe(context.Context) (api.Descriptor, error) {
	return api.Descriptor{ID: "mem", Capabilities: api.Capabilities{Resumability: api.ResumabilityRequiresMemorySnapshot}}, nil
}
func (memSnapshotHarness) Run(context.Context, *api.Start, api.EventSink) error { return nil }

// stubControl is a no-op substrate.ControlClient so a substrate backend can be driven in tests
// without a real ate-api-server.
type stubControl struct{}

func (stubControl) CreateActor(context.Context, substrate.ActorRef, substrate.ObjectRef) error {
	return nil
}
func (stubControl) ResumeActor(context.Context, substrate.ActorRef, bool) (substrate.ActorInfo, error) {
	return substrate.ActorInfo{}, nil
}
func (stubControl) SuspendActor(context.Context, substrate.ActorRef) (string, error) { return "", nil }
func (stubControl) DeleteActor(context.Context, substrate.ActorRef) error            { return nil }
func (stubControl) GetActor(context.Context, substrate.ActorRef) (substrate.ActorInfo, error) {
	return substrate.ActorInfo{}, nil
}

// TestNeutralityThroughPlacer is the spike §8 criterion driven end-to-end through the Placer's gate:
// a REQUIRES_MEMORY_SNAPSHOT harness is REFUSED on runtime/local (MemorySnapshot=false) with
// ErrUnplaceable — before any compute is provisioned or any record is written — and ACCEPTED on
// substrate (MemorySnapshot=true). Honest degradation is now an end-to-end property of the Placer,
// not a bare CanPlace unit assertion.
func TestNeutralityThroughPlacer(t *testing.T) {
	// local (filesystem-only) must refuse.
	store1, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store1.Close()
	log1 := store1.Session("s")
	localPlacer := newLocalPlacer(t, memSnapshotHarness{})
	if _, err := localPlacer.Exec(context.Background(), log1, "s", nil, 0); !errors.Is(err, placement.ErrUnplaceable) {
		t.Fatalf("local (MemorySnapshot=false) must refuse a REQUIRES_MEMORY_SNAPSHOT harness, got %v", err)
	}
	if h, _ := log1.Head(); h != 0 {
		t.Fatalf("a refused placement must not provision or write to the log, head=%d", h)
	}

	// substrate (MemorySnapshot=true) must ACCEPT: the gate passes (no ErrUnplaceable). The stub
	// substrate returns no dialable address, so the drive fails afterward — that is not a placement
	// refusal, which is exactly the distinction under test.
	store2, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()
	sub := substrate.New(stubControl{}, "space", substrate.ObjectRef{Name: "echo"},
		api.Descriptor{ID: "mem", Capabilities: api.Capabilities{Resumability: api.ResumabilityRequiresMemorySnapshot}})
	subPlacer := placement.New(sub, echoagent.Model)
	if _, err := subPlacer.Exec(context.Background(), store2.Session("s"), "s", nil, 0); errors.Is(err, placement.ErrUnplaceable) {
		t.Fatalf("substrate (MemorySnapshot=true) must accept a REQUIRES_MEMORY_SNAPSHOT harness, got %v", err)
	}
}

// TestSuspendResumeRoundtripThroughSPI proves the §5.1 round-trip: Suspend snapshots via the Runtime
// SPI and records the SnapshotRef in a SUSPEND event on the tamper-evident chain; Resume recovers
// that ref, Restores a fresh incarnation, re-drives with a new fence, and records a RESUME marker —
// no side table. For runtime/local the ref is the session handle (Memory:false).
func TestSuspendResumeRoundtripThroughSPI(t *testing.T) {
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	log := store.Session("s")
	p := newLocalPlacer(t, echoagent.Harness{})

	if _, err := p.Exec(context.Background(), log, "s", []api.Message{*api.TextMessage("user", "hi")}, 0); err != nil {
		t.Fatal(err)
	}

	ref, err := p.Suspend(context.Background(), log, "s")
	if err != nil {
		t.Fatalf("suspend: %v", err)
	}
	if ref.Local != "s" || ref.Memory {
		t.Fatalf("local suspend ref must be the session handle, filesystem-only: %+v", ref)
	}

	// The ref rides the chain: the SUSPEND event carries it (§5.1), no side table.
	recs, _ := log.Read(1)
	var carried *api.SnapshotRef
	for _, r := range recs {
		if r.Event.Kind == api.EventLifecycle && r.Event.Lifecycle != nil && r.Event.Lifecycle.Kind == api.LifecycleSuspend {
			carried = r.Event.Lifecycle.Snapshot
		}
	}
	if carried == nil || carried.Local != "s" {
		t.Fatal("the SUSPEND event must carry the SnapshotRef on the chain (§5.1)")
	}

	// Resume recovers the ref, restores, and records a RESUME marker; the chain stays verifiable.
	if err := p.Resume(context.Background(), log, "s"); err != nil {
		t.Fatalf("resume: %v", err)
	}
	recs, _ = log.Read(1)
	last := recs[len(recs)-1].Event
	if last.Kind != api.EventLifecycle || last.Lifecycle == nil || last.Lifecycle.Kind != api.LifecycleResume {
		t.Fatalf("resume must record a RESUME marker, last=%v", last.Kind)
	}
	if err := log.Verify(); err != nil {
		t.Fatalf("verify after suspend/resume: %v", err)
	}
}

// cloningControl is a substrate control client that supports the ActorSnapshot clone APIs and
// records the calls a fork makes, so the Placer's fork path can be driven without an ate-api-server.
type cloningControl struct {
	stubControl
	calls       []string
	snapshot    string
	failCloneOf string
	// beforeSuspend fires once inside SuspendActor: the only window between the Placer observing the
	// parent head and committing the fork checkpoint, so it is where a racing writer must be injected.
	beforeSuspend func()
	// onFailedClone fires when failCloneOf trips, so a test can simulate the caller going away at
	// exactly the moment the fan-out starts unwinding.
	onFailedClone func()
	// suspendCost makes each suspend take measurable time, so a test can show whether a teardown
	// budget is per-child or shared across the whole fan-out.
	suspendCost time.Duration
	budgets     []time.Duration
}

func (c *cloningControl) SuspendActor(ctx context.Context, a substrate.ActorRef) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	// Teardown runs under its own deadline; the caller's context carries none. Recording the budget
	// each teardown actually sees is what distinguishes a per-child budget from a shared one.
	if dl, ok := ctx.Deadline(); ok {
		c.budgets = append(c.budgets, time.Until(dl))
	}
	// A real suspend streams the actor's RAM image to durable storage, so it consumes real time out
	// of whatever budget the caller gave it.
	if c.suspendCost > 0 {
		select {
		case <-time.After(c.suspendCost):
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	if c.beforeSuspend != nil {
		race := c.beforeSuspend
		c.beforeSuspend = nil
		race()
	}
	c.calls = append(c.calls, "suspend:"+a.Name)
	return c.snapshot, nil
}

func (c *cloningControl) ResumeActor(_ context.Context, a substrate.ActorRef, boot bool) (substrate.ActorInfo, error) {
	c.calls = append(c.calls, fmt.Sprintf("resume:%s:boot=%v", a.Name, boot))
	// Loopback so any test that drives past placement fails its dial immediately (connection
	// refused) instead of burning a TCP timeout on an unroutable address.
	return substrate.ActorInfo{PodIP: "127.0.0.1"}, nil
}

func (c *cloningControl) TagSnapshot(_ context.Context, snapshot, tag substrate.SnapshotID) error {
	c.calls = append(c.calls, "tag:"+snapshot.Name+"->"+tag.Name)
	return nil
}

func (c *cloningControl) CreateActorFromSnapshot(_ context.Context, a substrate.ActorRef, _ substrate.ObjectRef, tag substrate.SnapshotID) error {
	if c.failCloneOf == a.Name {
		if c.onFailedClone != nil {
			c.onFailedClone()
		}
		return errors.New("clone refused")
	}
	c.calls = append(c.calls, "clone:"+a.Name+":from="+tag.Name)
	return nil
}

func (c *cloningControl) DeleteSnapshotTag(ctx context.Context, tag substrate.SnapshotID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.calls = append(c.calls, "untag:"+tag.Name)
	return nil
}

func (c *cloningControl) DeleteActor(ctx context.Context, a substrate.ActorRef) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.calls = append(c.calls, "delete:"+a.Name)
	return nil
}

func memorySubstratePlacer(ctl substrate.ControlClient) *placement.Placer {
	return placement.New(substrate.New(ctl, "space", substrate.ObjectRef{Name: "counter"},
		api.Descriptor{ID: "mem", Capabilities: api.Capabilities{Resumability: api.ResumabilityRequiresMemorySnapshot}}),
		echoagent.Model)
}

// Forking a REQUIRES_MEMORY_SNAPSHOT session must checkpoint the parent first and clone the child
// from that snapshot: the parent's live RAM is not in the journal, so a replay-fork would silently
// hand back a divergent child. The parent must SURVIVE (snapshot, not Stop/delete), and the ref must
// ride the chain in a SUSPEND event so it stays recoverable.
func TestForkOfMemoryHarnessClonesParentSnapshot(t *testing.T) {
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	parent, child := store.Session("parent"), store.Session("child")

	fence, err := parent.NewFence()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parent.Append(0, fence, api.Event{Kind: api.EventInput, Message: api.TextMessage("user", "hi")}); err != nil {
		t.Fatal(err)
	}
	head, _ := parent.Head()

	ctl := &cloningControl{snapshot: "snap-parent-1"}
	if err := memorySubstratePlacer(ctl).Fork(context.Background(), parent, "parent", []placement.ForkChild{{UID: "child", Log: child}}, head); err != nil {
		t.Fatalf("fork: %v", err)
	}

	want := []string{"suspend:parent", "tag:snap-parent-1->fork-child", "clone:child:from=fork-child", "resume:child:boot=false"}
	if !reflect.DeepEqual(ctl.calls, want) {
		t.Fatalf("fork calls=%v want %v", ctl.calls, want)
	}
	// Snapshot frees the parent's worker but keeps the actor; a delete here would destroy the branch
	// point the children were taken from.
	for _, c := range ctl.calls {
		if c == "delete:parent" {
			t.Fatal("forking must not delete the parent actor")
		}
	}

	recs, _ := parent.Read(1)
	last := recs[len(recs)-1].Event
	if last.Kind != api.EventLifecycle || last.Lifecycle == nil || last.Lifecycle.Kind != api.LifecycleSuspend {
		t.Fatalf("the parent checkpoint must be recorded as a SUSPEND event, got %v", last.Kind)
	}
	if last.Lifecycle.Snapshot == nil || last.Lifecycle.Snapshot.ExternalURI != "snap-parent-1" {
		t.Fatalf("the SUSPEND event must carry the cloned snapshot ref, got %+v", last.Lifecycle.Snapshot)
	}
	// The child shares the parent's prefix hashes and is capped with a FORK marker; the fork-time
	// checkpoint (appended to the parent AFTER the fork point) must not leak into it.
	pRecs, _ := parent.Read(1)
	cRecs, _ := child.Read(1)
	if len(cRecs) != int(head)+1 {
		t.Fatalf("child len=%d want %d prefix records + a FORK marker", len(cRecs), head)
	}
	for i := 0; i < int(head); i++ {
		if cRecs[i].Hash != pRecs[i].Hash {
			t.Fatalf("fork prefix hash diverged at seq %d", i+1)
		}
	}
	if tail := cRecs[len(cRecs)-1].Event; tail.Kind != api.EventLifecycle || tail.Lifecycle == nil || tail.Lifecycle.Kind != api.LifecycleFork {
		t.Fatalf("child must end with a FORK marker, got %v", tail.Kind)
	}
	if err := child.Verify(); err != nil {
		t.Fatalf("verify child: %v", err)
	}
	if err := parent.Verify(); err != nil {
		t.Fatalf("verify parent: %v", err)
	}
}

// A memory snapshot captures the parent's RAM as it is NOW, so it cannot realize a fork taken at a
// historical seq. Pairing an old log prefix with present-day RAM is exactly the silent divergence the
// capability gate exists to prevent, so the Placer refuses before touching the control plane.
func TestForkOfMemoryHarnessAtHistoricalSeqIsRefused(t *testing.T) {
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	parent, child := store.Session("parent"), store.Session("child")

	fence, err := parent.NewFence()
	if err != nil {
		t.Fatal(err)
	}
	last := int64(0)
	for _, text := range []string{"one", "two"} {
		r, err := parent.Append(last, fence, api.Event{Kind: api.EventInput, Message: api.TextMessage("user", text)})
		if err != nil {
			t.Fatal(err)
		}
		last = r.Seq
	}

	ctl := &cloningControl{snapshot: "snap-parent-1"}
	before, err := parent.NewFence()
	if err != nil {
		t.Fatal(err)
	}
	err = memorySubstratePlacer(ctl).Fork(context.Background(), parent, "parent", []placement.ForkChild{{UID: "child", Log: child}}, last-1)
	if !errors.Is(err, placement.ErrUnplaceable) {
		t.Fatalf("err=%v want ErrUnplaceable", err)
	}
	if len(ctl.calls) != 0 {
		t.Fatalf("a refused fork must not touch the control plane, got %v", ctl.calls)
	}
	if h, _ := child.Head(); h != 0 {
		t.Fatalf("a refused fork must not write the child log, head=%d", h)
	}
	// A fence bump is an autocommitted supersede: it would kill any turn in flight on the parent.
	// A request that changes nothing must not do that, so the only mint between these two calls is
	// this test's own.
	after, err := parent.NewFence()
	if err != nil {
		t.Fatal(err)
	}
	if after != before+1 {
		t.Fatalf("a refused fork minted a fence (%d -> %d), superseding the parent's in-flight writer", before, after)
	}
}

// A STATELESS_REPLAY session forks without checkpointing its parent: the journal already holds
// everything the child needs, so the parent keeps its worker and nothing is appended to its log.
func TestForkOfStatelessHarnessLeavesParentRunning(t *testing.T) {
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	parent, child := store.Session("parent"), store.Session("child")
	p := newLocalPlacer(t, echoagent.Harness{})

	if _, err := p.Exec(context.Background(), parent, "parent", []api.Message{*api.TextMessage("user", "hi")}, 0); err != nil {
		t.Fatal(err)
	}
	head, _ := parent.Head()
	if err := p.Fork(context.Background(), parent, "parent", []placement.ForkChild{{UID: "child", Log: child}}, head); err != nil {
		t.Fatalf("fork: %v", err)
	}

	if h, _ := parent.Head(); h != head {
		t.Fatalf("a stateless fork must not checkpoint the parent, head %d -> %d", head, h)
	}
	cRecs, _ := child.Read(1)
	if len(cRecs) != int(head)+1 {
		t.Fatalf("child len=%d want %d prefix records + a FORK marker", len(cRecs), head)
	}
	if err := child.Verify(); err != nil {
		t.Fatalf("verify child: %v", err)
	}
}

// The fan-out case behind session forking: N children branched from ONE parent checkpoint. The parent
// is snapshotted exactly once no matter how many children are taken, and every child clones that same
// snapshot, so the whole fleet starts from identical state.
func TestForkFanOutTakesOneParentCheckpoint(t *testing.T) {
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	parent := store.Session("parent")

	fence, err := parent.NewFence()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parent.Append(0, fence, api.Event{Kind: api.EventInput, Message: api.TextMessage("user", "hi")}); err != nil {
		t.Fatal(err)
	}
	head, _ := parent.Head()

	children := []placement.ForkChild{
		{UID: "c1", Log: store.Session("c1")},
		{UID: "c2", Log: store.Session("c2")},
		{UID: "c3", Log: store.Session("c3")},
	}
	ctl := &cloningControl{snapshot: "snap-parent-1"}
	if err := memorySubstratePlacer(ctl).Fork(context.Background(), parent, "parent", children, head); err != nil {
		t.Fatalf("fork: %v", err)
	}

	suspends := 0
	for _, c := range ctl.calls {
		if c == "suspend:parent" {
			suspends++
		}
	}
	if suspends != 1 {
		t.Fatalf("fan-out took %d parent checkpoints, want exactly 1", suspends)
	}
	for _, child := range children {
		want := "clone:" + child.UID + ":from=fork-" + child.UID
		if !slices.Contains(ctl.calls, want) {
			t.Fatalf("missing %q in %v", want, ctl.calls)
		}
		if err := child.Log.Verify(); err != nil {
			t.Fatalf("verify %s: %v", child.UID, err)
		}
	}

	// Every child branches from the SAME parent state: identical prefix hashes.
	pRecs, _ := parent.Read(1)
	for _, child := range children {
		cRecs, _ := child.Log.Read(1)
		for i := 0; i < int(head); i++ {
			if cRecs[i].Hash != pRecs[i].Hash {
				t.Fatalf("child %s diverged from the parent prefix at seq %d", child.UID, i+1)
			}
		}
	}
}

// A child inherits its parent's log prefix VERBATIM, so a parent's own SUSPEND record — snapshot ref
// and all — can sit inside the child's history. Resume keys off SnapshotRef.Local (the actor handle),
// so honoring an inherited ref would restore the PARENT's incarnation under the child's session: two
// sessions driving one actor. The child must skip refs it did not write and fall back to its own
// handle.
func TestResumeIgnoresInheritedParentSnapshotRef(t *testing.T) {
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	parent, child := store.Session("parent"), store.Session("child")

	fence, err := parent.NewFence()
	if err != nil {
		t.Fatal(err)
	}
	// A realistic parent: a turn, a suspend/resume cycle of its OWN, then another turn. The fork point
	// is after all of it, so the parent's SUSPEND record lands inside the copied prefix.
	rec, err := parent.Append(0, fence, api.Event{Kind: api.EventInput, Message: api.TextMessage("user", "hi")})
	if err != nil {
		t.Fatal(err)
	}
	rec, err = parent.Append(rec.Seq, fence, api.Event{Kind: api.EventLifecycle, Lifecycle: &api.Lifecycle{
		Kind:     api.LifecycleSuspend,
		Snapshot: &api.SnapshotRef{Local: "parent", ExternalURI: "snap-parent-earlier", Memory: true},
	}})
	if err != nil {
		t.Fatal(err)
	}
	parentSuspendSeq := rec.Seq
	rec, err = parent.Append(rec.Seq, fence, api.Event{Kind: api.EventInput, Message: api.TextMessage("user", "again")})
	if err != nil {
		t.Fatal(err)
	}
	head := rec.Seq

	ctl := &cloningControl{snapshot: "snap-parent-now"}
	p := memorySubstratePlacer(ctl)
	if err := p.Fork(context.Background(), parent, "parent", []placement.ForkChild{{UID: "child", Log: child}}, head); err != nil {
		t.Fatal(err)
	}

	// Guard the premise: the parent's own SUSPEND really is in the child's copied prefix, otherwise
	// this test would pass without exercising the filter at all.
	recs, err := child.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	inherited := false
	for _, r := range recs {
		if r.Seq == parentSuspendSeq && r.Event.Lifecycle != nil &&
			r.Event.Lifecycle.Snapshot != nil && r.Event.Lifecycle.Snapshot.Local == "parent" {
			inherited = true
		}
	}
	if !inherited {
		t.Fatal("premise broken: the child did not inherit the parent's SUSPEND record")
	}

	// No harness is listening, so the drive fails after Restore. Restore is what is under test and it
	// has already run by then; the calls it recorded are the assertion.
	ctl.calls = nil
	_ = p.Resume(context.Background(), child, "child")
	if slices.Contains(ctl.calls, "resume:parent:boot=false") {
		t.Fatalf("resuming the child restored the PARENT's actor via an inherited ref: %v", ctl.calls)
	}
	if !slices.Contains(ctl.calls, "resume:child:boot=false") {
		t.Fatalf("the child must restore its OWN actor, got %v", ctl.calls)
	}
}

// The fork checkpoint must be committed under a compare-and-swap on the seq the caller validated.
// The only window is the snapshot call itself, so a writer that commits there (under a newer fence,
// which is what lets it past the fork's own fence) must abort the fork: its children would otherwise
// pair a prefix ending at atSeq with RAM that already reflects the later event.
func TestForkAbortsWhenParentAdvancesUnderIt(t *testing.T) {
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	parent, child := store.Session("parent"), store.Session("child")

	fence, err := parent.NewFence()
	if err != nil {
		t.Fatal(err)
	}
	rec, err := parent.Append(0, fence, api.Event{Kind: api.EventInput, Message: api.TextMessage("user", "one")})
	if err != nil {
		t.Fatal(err)
	}
	head := rec.Seq

	ctl := &cloningControl{snapshot: "snap-parent-1"}
	ctl.beforeSuspend = func() {
		racer, err := parent.NewFence()
		if err != nil {
			t.Error(err)
			return
		}
		if _, err := parent.Append(head, racer, api.Event{Kind: api.EventInput, Message: api.TextMessage("user", "two")}); err != nil {
			t.Error(err)
		}
	}

	// Fork at what WAS the head; the racer moves it while the snapshot is being taken.
	err = memorySubstratePlacer(ctl).Fork(context.Background(), parent, "parent", []placement.ForkChild{{UID: "child", Log: child}}, head)
	if !errors.Is(err, eventlog.ErrConflict) && !errors.Is(err, eventlog.ErrFenced) {
		t.Fatalf("err=%v want the fork to abort because its fork point was overtaken", err)
	}
	if h, _ := child.Head(); h != 0 {
		t.Fatalf("an aborted fork must not write the child log, head=%d", h)
	}
	if slices.Contains(ctl.calls, "clone:child:from=fork-child") {
		t.Fatalf("an aborted fork must not provision children, calls=%v", ctl.calls)
	}
}

// A fan-out that fails partway must not strand the children it already provisioned: their UIDs are
// never returned to the caller, so an un-rolled-back actor is unreachable and unstoppable.
func TestForkFanOutRollsBackProvisionedChildren(t *testing.T) {
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	parent := store.Session("parent")

	fence, err := parent.NewFence()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parent.Append(0, fence, api.Event{Kind: api.EventInput, Message: api.TextMessage("user", "hi")}); err != nil {
		t.Fatal(err)
	}
	head, _ := parent.Head()

	children := []placement.ForkChild{
		{UID: "c1", Log: store.Session("c1")},
		{UID: "c2", Log: store.Session("c2")},
	}
	ctl := &cloningControl{snapshot: "snap-parent-1", failCloneOf: "c2"}
	if err := memorySubstratePlacer(ctl).Fork(context.Background(), parent, "parent", children, head); err == nil {
		t.Fatal("expected the fan-out to fail")
	}
	// c1 was fully provisioned before c2 failed, so it must have been torn down — including the
	// retention pin its clone placed on the parent snapshot, which nothing can name once the child
	// UID is discarded.
	if !slices.Contains(ctl.calls, "suspend:c1") || !slices.Contains(ctl.calls, "delete:c1") {
		t.Fatalf("the already-provisioned child must be rolled back, calls=%v", ctl.calls)
	}
	if !slices.Contains(ctl.calls, "untag:fork-c1") {
		t.Fatalf("rolling back a child must release its fork tag, calls=%v", ctl.calls)
	}
	// c2's own retention pin must not survive its failed clone either.
	if !slices.Contains(ctl.calls, "untag:fork-c2") {
		t.Fatalf("a failed clone must release the tag it created, calls=%v", ctl.calls)
	}
}

// A wide fan-out provisions children one at a time, each a full snapshot restore, so running out of
// the caller's deadline partway is the expected failure — not an exotic one. Rollback bound to that
// same context would be a no-op in precisely that case and strand every child it exists to reclaim.
func TestForkRollbackSurvivesACancelledCaller(t *testing.T) {
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	parent := store.Session("parent")

	fence, err := parent.NewFence()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parent.Append(0, fence, api.Event{Kind: api.EventInput, Message: api.TextMessage("user", "hi")}); err != nil {
		t.Fatal(err)
	}
	head, _ := parent.Head()

	ctx, cancel := context.WithCancel(context.Background())
	ctl := &cloningControl{snapshot: "snap-parent-1", failCloneOf: "c2"}
	// The caller goes away at the moment the fan-out starts unwinding.
	ctl.onFailedClone = cancel
	defer cancel()

	children := []placement.ForkChild{
		{UID: "c1", Log: store.Session("c1")},
		{UID: "c2", Log: store.Session("c2")},
	}
	if err := memorySubstratePlacer(ctl).Fork(ctx, parent, "parent", children, head); err == nil {
		t.Fatal("expected the fan-out to fail")
	}
	// Every control call refuses a cancelled context, so recording these proves the rollback ran on a
	// context detached from the caller's.
	for _, want := range []string{"suspend:c1", "delete:c1", "untag:fork-c1"} {
		if !slices.Contains(ctl.calls, want) {
			t.Fatalf("rollback must survive the caller's cancellation, missing %q in %v", want, ctl.calls)
		}
	}
}

// Teardown must budget per child, not once for the whole fan-out. Each Stop streams a full RAM image
// to durable storage, so at the fan-out width the API allows a single shared deadline would expire
// partway through the unwind and strand the tail — the same leak the detached context prevents, just
// moved further down the list.
func TestForkRollbackBudgetsEachChildSeparately(t *testing.T) {
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	parent := store.Session("parent")

	fence, err := parent.NewFence()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parent.Append(0, fence, api.Event{Kind: api.EventInput, Message: api.TextMessage("user", "hi")}); err != nil {
		t.Fatal(err)
	}
	head, _ := parent.Head()

	const cost = 100 * time.Millisecond
	children := []placement.ForkChild{
		{UID: "c1", Log: store.Session("c1")},
		{UID: "c2", Log: store.Session("c2")},
		{UID: "c3", Log: store.Session("c3")},
		{UID: "boom", Log: store.Session("boom")},
	}
	ctl := &cloningControl{snapshot: "snap-parent-1", failCloneOf: "boom", suspendCost: cost}
	if err := memorySubstratePlacer(ctl).Fork(context.Background(), parent, "parent", children, head); err == nil {
		t.Fatal("expected the fan-out to fail")
	}
	if len(ctl.budgets) != 3 {
		t.Fatalf("want a budgeted teardown per provisioned child, got %d: %v", len(ctl.budgets), ctl.budgets)
	}
	// A shared budget shrinks by the accumulated cost of every earlier teardown; a per-child budget
	// starts over each time.
	if drop := ctl.budgets[0] - ctl.budgets[len(ctl.budgets)-1]; drop > cost {
		t.Fatalf("teardown budget shrank by %v across the fan-out (%v); it must be per child", drop, ctl.budgets)
	}
	for _, uid := range []string{"c1", "c2", "c3"} {
		if !slices.Contains(ctl.calls, "delete:"+uid) {
			t.Fatalf("every provisioned child must be torn down, missing %q in %v", uid, ctl.calls)
		}
	}
}

// failOnceHarness is a STATELESS_REPLAY echo whose first Run fails, leaving an interrupted turn
// (no END) on the log for Resume to re-drive.
type failOnceHarness struct {
	echoagent.Harness
	failed *atomic.Bool
}

func (h failOnceHarness) Run(ctx context.Context, s *api.Start, sink api.EventSink) error {
	if h.failed.CompareAndSwap(false, true) {
		return errors.New("harness went away mid-turn")
	}
	return h.Harness.Run(ctx, s, sink)
}

// describeOverride serves the real local backend but lets the test change what Describe reports,
// standing in for a different harness answering at the same address (runtime/remote).
type describeOverride struct {
	*local.Backend
	desc     *api.Descriptor
	restores atomic.Int32
}

func (b *describeOverride) Describe(ctx context.Context) (api.Descriptor, error) {
	if b.desc != nil {
		return *b.desc, nil
	}
	return b.Backend.Describe(ctx)
}

func (b *describeOverride) Restore(ctx context.Context, ref api.SnapshotRef) (api.Incarnation, error) {
	b.restores.Add(1)
	return b.Backend.Restore(ctx, ref)
}

// Resume re-drives an interrupted turn, so it must pass the same CanPlace gate as Exec. If the
// harness now declares REQUIRES_MEMORY_SNAPSHOT on a backend that cannot snapshot memory, Resume is
// refused before Restore and nothing is journaled.
func TestResumeRefusesAnUnplaceableHarness(t *testing.T) {
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	log := store.Session("s")

	inner := local.New(failOnceHarness{failed: &atomic.Bool{}})
	t.Cleanup(func() { _ = inner.Close() })
	backend := &describeOverride{Backend: inner}
	p := placement.New(backend, echoagent.Model)

	if _, err := p.Exec(context.Background(), log, "s", []api.Message{*api.TextMessage("user", "hi")}, 0); err == nil {
		t.Fatal("the first turn should have been interrupted")
	}
	head, err := log.Head()
	if err != nil {
		t.Fatal(err)
	}

	backend.desc = &api.Descriptor{ID: "mem", Capabilities: api.Capabilities{Resumability: api.ResumabilityRequiresMemorySnapshot}}
	if err := p.Resume(context.Background(), log, "s"); !errors.Is(err, placement.ErrUnplaceable) {
		t.Fatalf("resume into a REQUIRES_MEMORY_SNAPSHOT harness on a filesystem-only backend: got %v, want ErrUnplaceable", err)
	}
	if got, _ := log.Head(); got != head {
		t.Fatalf("a refused resume wrote to the log: head %d -> %d", head, got)
	}
	if n := backend.restores.Load(); n != 0 {
		t.Fatalf("a refused resume restored compute %d time(s)", n)
	}

	// The same session resumes once a placeable harness answers again, so the refusal above was the
	// gate and not a session that could never resume.
	backend.desc = nil
	if err := p.Resume(context.Background(), log, "s"); err != nil {
		t.Fatalf("resume with a placeable harness: %v", err)
	}
	if got, _ := log.Head(); got <= head {
		t.Fatal("resume did not re-drive the interrupted turn")
	}
	if err := log.Verify(); err != nil {
		t.Fatalf("verify: %v", err)
	}
}

// describeFails serves the real local backend but fails Describe the way a gRPC client does, so a
// test can tell an outage at the harness from the caller's own deadline running out.
type describeFails struct {
	*local.Backend
	err func(ctx context.Context) error
}

func (b *describeFails) Describe(ctx context.Context) (api.Descriptor, error) {
	return api.Descriptor{}, b.err(ctx)
}

// Admission maps a harness that cannot be described to ErrHarnessUnavailable, a retryable outage,
// but only while the caller's context is still live. gRPC reports a caller's expired deadline or
// cancellation as DeadlineExceeded or Canceled too, and resending that call cannot succeed, so it
// must keep the context's error instead.
func TestAdmissionKeepsTheCallersDeadline(t *testing.T) {
	// What a gRPC client returns: the context's own status once it is done, otherwise peerErr.
	grpcLike := func(peerErr error) func(ctx context.Context) error {
		return func(ctx context.Context) error {
			if ctx.Err() != nil {
				return status.FromContextError(ctx.Err()).Err()
			}
			return peerErr
		}
	}
	// A harness that accepts the call and never answers: Describe returns only when its context ends.
	neverAnswers := func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}
	// A backend whose own internal timeout fired, independent of the caller's context.
	backendTimedOut := func(context.Context) error {
		return fmt.Errorf("backend gave up: %w", context.DeadlineExceeded)
	}
	placement.SetDescribeTimeout(t, 200*time.Millisecond)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	tests := []struct {
		name    string
		ctx     context.Context
		opts    []placement.ExecOption
		peerErr error
		// describe overrides grpcLike(peerErr) when set.
		describe func(ctx context.Context) error
		want     error
		notWant  error
		// interrupted: the error must be ErrAdmissionInterrupted, the only form the service maps
		// to DEADLINE_EXCEEDED or CANCELLED.
		interrupted bool
	}{
		{
			name:        "caller deadline already expired",
			ctx:         context.Background(),
			opts:        []placement.ExecOption{placement.WithDeadline(time.Now().Add(-time.Minute))},
			want:        context.DeadlineExceeded,
			notWant:     placement.ErrHarnessUnavailable,
			interrupted: true,
		},
		{
			name:        "caller cancelled",
			ctx:         cancelled,
			want:        context.Canceled,
			notWant:     placement.ErrHarnessUnavailable,
			interrupted: true,
		},
		{
			name:    "peer deadline while the caller is live",
			ctx:     context.Background(),
			peerErr: status.Error(codes.DeadlineExceeded, "peer deadline"),
			want:    placement.ErrHarnessUnavailable,
		},
		{
			name:    "connection refused",
			ctx:     context.Background(),
			peerErr: status.Error(codes.Unavailable, "connection refused"),
			want:    placement.ErrHarnessUnavailable,
		},
		{
			name:     "harness never answers and the caller set no deadline",
			ctx:      context.Background(),
			describe: neverAnswers,
			want:     placement.ErrHarnessUnavailable,
			notWant:  placement.ErrAdmissionInterrupted,
		},
		{
			name:        "harness never answers and the caller's deadline is earlier than the bound",
			ctx:         context.Background(),
			opts:        []placement.ExecOption{placement.WithDeadline(time.Now().Add(50 * time.Millisecond))},
			describe:    neverAnswers,
			want:        context.DeadlineExceeded,
			notWant:     placement.ErrHarnessUnavailable,
			interrupted: true,
		},
		{
			name:        "caller cancelled while the backend reports its own timeout",
			ctx:         cancelled,
			describe:    backendTimedOut,
			want:        context.Canceled,
			notWant:     context.DeadlineExceeded,
			interrupted: true,
		},
		{
			name:    "harness answered with an error",
			ctx:     context.Background(),
			peerErr: status.Error(codes.Unimplemented, "no Describe"),
			notWant: placement.ErrHarnessUnavailable,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, err := sqlitelog.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			log := store.Session("s")
			inner := local.New(echoagent.Harness{})
			t.Cleanup(func() { _ = inner.Close() })
			describe := grpcLike(tt.peerErr)
			if tt.describe != nil {
				describe = tt.describe
			}
			p := placement.New(&describeFails{Backend: inner, err: describe}, echoagent.Model)

			_, err = p.Exec(tt.ctx, log, "s", []api.Message{*api.TextMessage("user", "hi")}, 0, tt.opts...)
			if err == nil {
				t.Fatal("exec succeeded with a harness that could not be described")
			}
			if tt.want != nil && !errors.Is(err, tt.want) {
				t.Fatalf("exec: got %v, want %v", err, tt.want)
			}
			if tt.notWant != nil && errors.Is(err, tt.notWant) {
				t.Fatalf("exec: got %v, must not be %v", err, tt.notWant)
			}
			if got := errors.Is(err, placement.ErrAdmissionInterrupted); got != tt.interrupted {
				t.Fatalf("exec: got %v, ErrAdmissionInterrupted = %v, want %v", err, got, tt.interrupted)
			}
			if head, _ := log.Head(); head != 0 {
				t.Fatalf("a refused admission wrote to the log: head %d", head)
			}
		})
	}
}

// liveLocal is the local backend presented as one whose Describe asks a live harness, so the Placer
// re-checks the harness it dials for the turn (placement.LiveDescriber), as it does for
// runtime/remote.
type liveLocal struct{ *local.Backend }

func (liveLocal) DescribesLiveHarness() bool { return true }

// For a live-describing backend, the harness the turn actually runs on is gated too, not only the
// one the backend described. Here the backend's Describe reaches a STATELESS_REPLAY harness while the
// turn's connection reaches a REQUIRES_MEMORY_SNAPSHOT one, as with two replicas behind one address:
// Exec and Resume are refused before anything is written to the log.
func TestLiveBackendGatesTheHarnessTheTurnRunsOn(t *testing.T) {
	store, err := sqlitelog.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	log := store.Session("s")

	stateless := failOnceHarness{failed: &atomic.Bool{}}
	inner := local.New(stateless)
	t.Cleanup(func() { _ = inner.Close() })
	var dials atomic.Int32
	// The first turn runs on the stateless harness and is interrupted; every later connection reaches
	// the memory harness.
	dial := placement.WithDialer(func(string) (api.Harness, func() error, error) {
		if dials.Add(1) == 1 {
			return stateless, func() error { return nil }, nil
		}
		return &counteragent.Harness{}, func() error { return nil }, nil
	})
	p := placement.New(liveLocal{inner}, echoagent.Model, dial)

	if _, err := p.Exec(context.Background(), log, "s", []api.Message{*api.TextMessage("user", "hi")}, 0); err == nil || errors.Is(err, placement.ErrUnplaceable) {
		t.Fatalf("the first turn should have run and been interrupted, got %v", err)
	}
	head, err := log.Head()
	if err != nil {
		t.Fatal(err)
	}
	if head == 0 {
		t.Fatal("the first turn wrote nothing")
	}

	if err := p.Resume(context.Background(), log, "s"); !errors.Is(err, placement.ErrUnplaceable) {
		t.Fatalf("resume onto a REQUIRES_MEMORY_SNAPSHOT harness behind a stateless descriptor: got %v, want ErrUnplaceable", err)
	}
	if got, _ := log.Head(); got != head {
		t.Fatalf("a refused resume wrote to the log: head %d -> %d", head, got)
	}
	if _, err := p.Exec(context.Background(), log, "s", []api.Message{*api.TextMessage("user", "again")}, head); !errors.Is(err, placement.ErrUnplaceable) {
		t.Fatalf("exec onto a REQUIRES_MEMORY_SNAPSHOT harness behind a stateless descriptor: got %v, want ErrUnplaceable", err)
	}
	if got, _ := log.Head(); got != head {
		t.Fatalf("a refused exec wrote to the log: head %d -> %d", head, got)
	}
}

// stallingHarness answers its first `answered` Describe calls and then never again, like a harness
// that accepts the call and wedges. Its Describe returns only when the caller gives up.
type stallingHarness struct {
	echoagent.Harness
	answered int32
	calls    *atomic.Int32
}

func (h stallingHarness) Describe(ctx context.Context) (api.Descriptor, error) {
	if h.calls.Add(1) > h.answered {
		<-ctx.Done()
		return api.Descriptor{}, ctx.Err()
	}
	return h.Harness.Describe(ctx)
}

// A harness that completes the gRPC connection but never answers Describe must not hold a call that
// has no deadline. Admission bounds Describe on its own and reports the harness as unavailable,
// whether it stalls on the backend's connection (admission) or on the turn's connection (the
// re-check), and nothing is journaled.
func TestAdmissionBoundsAHarnessThatNeverAnswers(t *testing.T) {
	placement.SetDescribeTimeout(t, 300*time.Millisecond)
	for _, tt := range []struct {
		name     string
		answered int32
	}{
		{name: "stalls on admission", answered: 0},
		{name: "stalls on the turn's connection", answered: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			lis, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			srv := grpc.NewServer()
			v1.RegisterHarnessServer(srv, harnesswire.NewServer(stallingHarness{answered: tt.answered, calls: &atomic.Int32{}}))
			go func() { _ = srv.Serve(lis) }()
			t.Cleanup(srv.Stop)
			backend := remote.New(lis.Addr().String())
			t.Cleanup(func() { _ = backend.Close() })
			store, err := sqlitelog.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			log := store.Session("s")

			start := time.Now()
			_, err = placement.New(backend, echoagent.Model).Exec(context.Background(), log, "s", []api.Message{*api.TextMessage("user", "hi")}, 0)
			if !errors.Is(err, placement.ErrHarnessUnavailable) || errors.Is(err, placement.ErrAdmissionInterrupted) {
				t.Fatalf("exec against a harness that never answers Describe: got %v, want ErrHarnessUnavailable", err)
			}
			if elapsed := time.Since(start); elapsed > 5*time.Second {
				t.Fatalf("exec took %v; the describe bound should have ended it", elapsed)
			}
			if head, _ := log.Head(); head != 0 {
				t.Fatalf("a refused admission wrote to the log: head %d", head)
			}
		})
	}
}
