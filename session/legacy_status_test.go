package session_test

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/wire"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestExecLegacyBoundaryRefusalIsFailedPrecondition(t *testing.T) {
	for _, tc := range []struct {
		name   string
		events []api.Event
		detail string
	}{
		{"unknown legacy kind", []api.Event{{Kind: api.EventKind("INVALID")}}, "not a legacy event"},
		{"ambiguous markerless boundary", []api.Event{
			{Kind: api.EventInput, Message: api.TextMessage("user", "old")},
			{Kind: api.EventInput, ExecutionID: "modern", Message: api.TextMessage("user", "new")},
		}, "unfinished legacy prefix"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := openStore(t, ":memory:")
			client := serve(t, store)
			uid := mustCreate(t, client)
			log := store.Session(uid)
			fence, err := log.NewFence()
			if err != nil {
				t.Fatal(err)
			}
			for i, event := range tc.events {
				if _, err := log.Append(int64(i), fence, event); err != nil {
					t.Fatal(err)
				}
			}
			before, err := log.Read(1)
			if err != nil {
				t.Fatal(err)
			}
			stream, err := client.Exec(t.Context(), &v1.ExecRequest{Session: uid, Inputs: []*v1.Message{wire.MessageToProto(api.TextMessage("user", "next"))}})
			if err == nil {
				err = drainExec(stream)
			}
			if status.Code(err) != codes.FailedPrecondition || !strings.Contains(status.Convert(err).Message(), tc.detail) {
				t.Fatalf("Exec refusal = %v; want FailedPrecondition with %q", err, tc.detail)
			}
			if after, err := log.Read(1); err != nil || !reflect.DeepEqual(after, before) {
				t.Fatalf("refused Exec changed journal: %v", err)
			}
		})
	}
}

func TestExecV012FailedTurnThroughSessions(t *testing.T) {
	store := openStore(t, ":memory:")
	client := serve(t, store)
	uid := mustCreate(t, client)
	log := store.Session(uid)
	data, err := os.ReadFile(filepath.Join("..", "controller", "testdata", "v0.1.2", "model-error.json"))
	if err != nil {
		t.Fatal(err)
	}
	var original []eventlog.Record
	if err := json.Unmarshal(data, &original); err != nil {
		t.Fatal(err)
	}
	fence, err := log.NewFence()
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range original {
		got, err := log.Append(record.Seq-1, fence, record.Event)
		if err != nil || !reflect.DeepEqual(record, got) {
			t.Fatalf("copy actual v0.1.2 ERROR record: %#v, %v", got, err)
		}
	}
	if _, err := client.Resume(t.Context(), &v1.ResumeRequest{Session: uid}); err != nil {
		t.Fatalf("ERROR-finished Sessions Resume: %v", err)
	}
	resumed, err := log.Read(1)
	if err != nil || len(resumed) != len(original)+1 || !reflect.DeepEqual(resumed[:len(original)], original) ||
		resumed[len(original)].Event.Kind != api.EventLifecycle || resumed[len(original)].Event.Lifecycle.Kind != api.LifecycleResume {
		t.Fatalf("finished ERROR must append only the placement RESUME marker: %#v, %v", resumed, err)
	}
	if out := execOutputs(t, client, uid, "new", int64(len(resumed))); !reflect.DeepEqual(out, []string{"echo:new"}) {
		t.Fatalf("Sessions Exec after ERROR = %v", out)
	}
	stream, err := client.Replay(t.Context(), &v1.ReplayRequest{Session: uid})
	if err != nil {
		t.Fatal(err)
	}
	var output []string
	for {
		record, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Sessions Replay after ERROR: %v", err)
		}
		if record.GetEvent().GetKind() == v1.EventKind_EVENT_OUTPUT {
			output = append(output, wire.EventFromProto(record.GetEvent()).Message.Text())
		}
	}
	if !reflect.DeepEqual(output, []string{"echo:new"}) {
		t.Fatalf("Sessions Replay outputs = %v", output)
	}
}
