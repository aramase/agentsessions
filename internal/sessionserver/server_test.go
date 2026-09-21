package sessionserver_test

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/client"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/internal/sessionserver"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/runtime/local"
)

func TestServePersistsSessionsAcrossRestart(t *testing.T) {
	journal := filepath.Join(t.TempDir(), "journal.db")
	uid := serveOnce(t, journal, "")
	serveOnce(t, journal, uid)
}

func serveOnce(t *testing.T, journal, uid string) string {
	t.Helper()
	backend := local.New(echoagent.Harness{})
	defer func() { _ = backend.Close() }()
	registry, err := placement.NewRegistry("echo", map[string]*placement.Placer{
		"echo": placement.New(backend, echoagent.Model),
	})
	if err != nil {
		t.Fatal(err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- sessionserver.Serve(ctx, lis, sessionserver.Config{
			Journal:          journal,
			Project:          "test",
			ModelDescription: "echo",
		}, registry, nil)
	}()

	c, err := client.Dial(lis.Addr().String(), client.WithProject("test"))
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	callCtx, callCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer callCancel()
	if uid == "" {
		turn, err := c.Exec(callCtx, client.ExecOptions{
			Inputs: []string{"hello"},
		})
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		uid = turn.Session.GetMetadata().GetUid()
	} else {
		session, err := c.GetSession(callCtx, uid)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		if session.GetLastSeq() == 0 {
			cancel()
			t.Fatal("persisted session has no committed records")
		}
		records, err := c.Replay(callCtx, uid, 1, 0)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		if len(records) == 0 || records[0].GetEvent().GetKind() != v1.EventKind_EVENT_INPUT {
			cancel()
			t.Fatalf("persisted records = %v", records)
		}
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not stop after cancellation")
	}
	return uid
}
