package conformance_test

import (
	"context"
	"encoding/base64"
	"errors"
	"reflect"
	"strconv"
	"testing"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/sqlitelog"
)

// startSensitiveHarness runs remotely, making byte-exact config/cursor and the full history
// boundary part of a model request checked by I0. Channels synchronize captured remote Starts.
type startSensitiveHarness struct {
	starts    chan<- api.Start
	interrupt bool
}

func (startSensitiveHarness) Describe(context.Context) (api.Descriptor, error) {
	return api.Descriptor{ID: "start-sensitive", Capabilities: api.Capabilities{
		Resumability: api.ResumabilityStatelessReplay, ForkSafe: true,
	}}, nil
}

func (h startSensitiveHarness) Run(ctx context.Context, start *api.Start, sink api.EventSink) error {
	captured := *start
	captured.Config = append([]byte(nil), start.Config...)
	captured.History = append([]api.Event{}, start.History...)
	captured.Inputs = append([]api.Message{}, start.Inputs...)
	h.starts <- captured
	_, err := sink.Model(ctx, api.ModelRequest{
		Model: "start-sensitive", Messages: start.Inputs,
		Params: map[string]string{
			"config":        base64.StdEncoding.EncodeToString(start.Config),
			"cursor":        strconv.FormatInt(start.ResumeFromSeq, 10),
			"history_count": strconv.Itoa(len(start.History)),
		},
	})
	if err != nil {
		return err
	}
	if h.interrupt {
		return errors.New("interrupted after model completion")
	}
	return sink.Output(ctx, "continued")
}

func TestWireExecutionStartReopenResumeReplayAndFork(t *testing.T) {
	store, path := openFile(t)
	starts := make(chan api.Start, 8)
	har := wireHarnessFrom(t, startSensitiveHarness{starts: starts})
	interrupted := wireHarnessFrom(t, startSensitiveHarness{starts: starts, interrupt: true})
	model := &countModel{}
	log := store.Session("parent")
	configs := [][]byte{{0, 255, ' ', '\n', '\t'}, []byte("  {\"config\":2} \r\n")}
	var liveStarts []api.Start
	var firstEnd int64
	for i, config := range configs {
		live, err := controller.New(log, model.call, controller.WithStart(config, int64(41+i)))
		if err != nil {
			t.Fatal(err)
		}
		head, err := log.Head()
		if err != nil {
			t.Fatal(err)
		}
		runner := har
		if i == 1 {
			runner = interrupted
		}
		err = live.Exec(t.Context(), runner, []api.Message{*api.TextMessage("user", "same")}, head)
		if (err != nil) != (i == 1) {
			t.Fatalf("turn %d error = %v", i, err)
		}
		liveStarts = append(liveStarts, <-starts)
		if i == 0 {
			firstEnd, err = log.Head()
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := sqlitelog.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	log = store.Session("parent")
	recovering, err := controller.New(log, model.call, controller.WithStart([]byte("wrong"), 999))
	if err != nil {
		t.Fatal(err)
	}
	if resumed, err := recovering.Resume(t.Context(), har); err != nil || !resumed {
		t.Fatalf("wire Resume = %v, %v", resumed, err)
	}
	if resumedStart := <-starts; !reflect.DeepEqual(resumedStart, liveStarts[1]) {
		t.Fatalf("wire Resume changed Start\nwant: %#v\ngot: %#v", liveStarts[1], resumedStart)
	}
	if model.n != 2 || recovering.ModelInvocations() != 0 {
		t.Fatalf("wire recovery repeated model: total=%d recovered=%d", model.n, recovering.ModelInvocations())
	}
	before, err := log.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	if outputs, err := recovering.Replay(t.Context(), har); err != nil || !reflect.DeepEqual(outputs, []string{"resp#1:same", "continued", "resp#2:same", "continued"}) {
		t.Fatalf("wire replay outputs = %v, %v", outputs, err)
	}
	for i := range liveStarts {
		if got := <-starts; !reflect.DeepEqual(got, liveStarts[i]) {
			t.Fatalf("wire Replay changed Start %d", i)
		}
	}
	after, err := log.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) || model.n != 2 {
		t.Fatal("wire Replay wrote records or invoked model")
	}
	head, err := log.Head()
	if err != nil {
		t.Fatal(err)
	}
	for _, fork := range []struct {
		name  string
		seq   int64
		count int
	}{
		{"historical", firstEnd, 1}, {"head", head, 2},
	} {
		child := store.Session(fork.name)
		if err := controller.Fork(log, child, fork.seq); err != nil {
			t.Fatal(err)
		}
		replay, err := controller.New(child, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := replay.Replay(t.Context(), har); err != nil {
			t.Fatalf("wire fork %s Replay: %v", fork.name, err)
		}
		for i := 0; i < fork.count; i++ {
			if got := <-starts; !reflect.DeepEqual(got, liveStarts[i]) {
				t.Fatalf("wire fork %s changed inherited Start %d", fork.name, i)
			}
		}
		if err := child.Verify(); err != nil {
			t.Fatal(err)
		}
	}
	if err := log.Verify(); err != nil {
		t.Fatal(err)
	}
}
