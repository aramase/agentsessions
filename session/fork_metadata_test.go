package session_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"maps"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/runtime/local"
	"github.com/aramase/agentsessions/session"
)

// Track placement's descriptor lookup while keeping the real harness and runtime behavior.
type describeTrackingHarness struct {
	echoagent.Harness
	describes atomic.Int32
}

func (h *describeTrackingHarness) Describe(ctx context.Context) (api.Descriptor, error) {
	h.describes.Add(1)
	return h.Harness.Describe(ctx)
}

func assertSessionMaps(t *testing.T, sess *v1.Session, labels, annotations map[string]string) {
	t.Helper()
	if !maps.Equal(sess.GetLabels(), labels) {
		t.Errorf("session %s labels = %v, want %v", sess.GetMetadata().GetUid(), sess.GetLabels(), labels)
	}
	if !maps.Equal(sess.GetAnnotations(), annotations) {
		t.Errorf("session %s annotations = %v, want %v", sess.GetMetadata().GetUid(), sess.GetAnnotations(), annotations)
	}
}

func TestForkMetadataMapsRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name       string
		labels     map[string]string
		wantLabels map[string]string
	}{
		{
			name:       "request_labels_replace_parent_labels",
			labels:     map[string]string{"team": "child-team", "branch": "experiment", "empty": ""},
			wantLabels: map[string]string{"team": "child-team", "branch": "experiment", "empty": ""},
		},
		{name: "omitted_labels_are_empty"},
		{name: "empty_labels_are_empty", labels: map[string]string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "journal.db")
			store := openStore(t, path)
			client := serve(t, store)
			ctx := context.Background()
			parent, err := client.CreateSession(ctx, &v1.CreateSessionRequest{Session: &v1.Session{
				Metadata:    &v1.ResourceMetadata{Project: "acme"},
				Labels:      map[string]string{"team": "parent-team", "parent-only": "not-inherited"},
				Annotations: map[string]string{"external/context": "triage \"nightly\"\nline two", "owner": "Māori"},
				Identity:    &v1.IdentityRef{Principal: "agent://parent"},
			}})
			if err != nil {
				t.Fatal(err)
			}
			parentUID := parent.GetMetadata().GetUid()
			forked, err := client.Fork(ctx, &v1.ForkRequest{Session: parentUID, Count: 2, Labels: tc.labels})
			if err != nil {
				t.Fatalf("fork without identity: %v", err)
			}
			if len(forked.GetChildren()) != 2 {
				t.Fatalf("children = %d, want 2", len(forked.GetChildren()))
			}
			wantAnnotations := map[string]string{"external/context": "triage \"nightly\"\nline two", "owner": "Māori"}
			children := map[string]bool{}
			for _, child := range forked.GetChildren() {
				uid := child.GetMetadata().GetUid()
				if uid == "" || uid == parentUID || children[uid] {
					t.Fatalf("invalid or duplicate child uid %q", uid)
				}
				children[uid] = true
				assertSessionMaps(t, child, tc.wantLabels, wantAnnotations)
				if child.GetIdentity() != nil {
					t.Errorf("child identity = %v, want no inherited parent identity", child.GetIdentity())
				}
			}

			checkStored := func(t *testing.T, client v1.SessionsClient) {
				t.Helper()
				for uid := range children {
					stored, err := client.GetSession(ctx, &v1.GetSessionRequest{Uid: uid})
					if err != nil {
						t.Fatal(err)
					}
					assertSessionMaps(t, stored, tc.wantLabels, wantAnnotations)
				}
				listed, err := client.ListSessions(ctx, &v1.ListSessionsRequest{Project: "acme"})
				if err != nil {
					t.Fatal(err)
				}
				if len(listed.GetSessions()) != 3 {
					t.Fatalf("list = %v, want parent plus two children", uids(listed.GetSessions()))
				}
				seen := map[string]bool{}
				for _, sess := range listed.GetSessions() {
					uid := sess.GetMetadata().GetUid()
					if seen[uid] || (uid != parentUID && !children[uid]) {
						t.Fatalf("unexpected or duplicate listed uid %q", uid)
					}
					seen[uid] = true
					if uid == parentUID {
						assertSessionMaps(t, sess, map[string]string{"team": "parent-team", "parent-only": "not-inherited"}, wantAnnotations)
					} else {
						assertSessionMaps(t, sess, tc.wantLabels, wantAnnotations)
					}
				}
			}
			t.Run("get_and_list", func(t *testing.T) { checkStored(t, client) })
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			reopened := serve(t, openStore(t, path))
			t.Run("get_and_list_after_restart", func(t *testing.T) { checkStored(t, reopened) })
		})
	}
}

func TestForkRejectsSuppliedIdentityBeforeSideEffects(t *testing.T) {
	for _, identity := range []struct {
		name  string
		value *v1.IdentityRef
	}{
		{"principal", &v1.IdentityRef{Principal: "agent://do-not-leak-child-principal", Issuer: "issuer", Subject: "child"}},
		{"empty_message", &v1.IdentityRef{}},
	} {
		for _, target := range []string{"parent", "unknown_session", "empty_session"} {
			t.Run(identity.name+"/"+target, func(t *testing.T) {
				store := openStore(t, ":memory:")
				var output bytes.Buffer
				logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}))
				harness := &describeTrackingHarness{}
				client := serveWithPlacement(t, store, local.New(harness, local.WithLogger(logger)),
					[]placement.Option{placement.WithLogger(logger)}, session.WithLogger(logger))
				ctx := context.Background()
				uid := mustCreate(t, client)
				execOutputs(t, client, uid, "parent input", 0)
				before, err := client.GetSession(ctx, &v1.GetSessionRequest{Uid: uid})
				if err != nil {
					t.Fatal(err)
				}
				recordsBefore, err := store.Session(uid).Read(1)
				if err != nil {
					t.Fatal(err)
				}
				describesBefore := harness.describes.Load()
				output.Reset()

				forkUID := uid
				switch target {
				case "unknown_session":
					forkUID = "sess-does-not-exist"
				case "empty_session":
					forkUID = ""
				}
				response, err := client.Fork(ctx, &v1.ForkRequest{Session: forkUID, Count: 2, Identity: identity.value})
				if status.Code(err) != codes.InvalidArgument {
					t.Errorf("fork with supplied identity: want InvalidArgument, got %v", err)
				}
				if got := status.Convert(err).Message(); got != "ForkRequest.identity is unsupported: per-child principals are not enforced" {
					t.Errorf("fork identity error = %q, want fixed unsupported-identity message", got)
				}
				if len(response.GetChildren()) != 0 {
					t.Errorf("rejected fork returned %d children", len(response.GetChildren()))
				}
				if strings.Contains(status.Convert(err).Message(), "do-not-leak-child-principal") {
					t.Error("rejected fork error leaked the supplied principal")
				}
				if got := harness.describes.Load(); got != describesBefore {
					t.Errorf("rejected fork consulted placement: descriptor lookups = %d, want %d", got, describesBefore)
				}
				starts, finishes := 0, 0
				scanner := bufio.NewScanner(bytes.NewReader(output.Bytes()))
				for scanner.Scan() {
					var record map[string]any
					if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
						t.Fatal(err)
					}
					for field, value := range record {
						if text, ok := value.(string); ok && strings.Contains(text, "do-not-leak-child-principal") {
							t.Errorf("rejected fork log field %s leaked the supplied principal", field)
						}
					}
					if record["component"] != "session" || record["operation"] != "fork" {
						t.Errorf("rejected fork emitted an unexpected operation: %v", record)
						continue
					}
					if requestID, _ := record["request_id"].(string); requestID == "" {
						t.Error("rejected fork log is missing request_id")
					}
					switch record["phase"] {
					case "start":
						starts++
					case "finish":
						finishes++
						if record["error_kind"] != "invalid_argument" || record["children_created"] != float64(0) || record["outcome"] != "error" {
							t.Errorf("rejected fork finish has incorrect outcome fields: %v", record)
						}
					default:
						t.Errorf("rejected fork log has an unexpected phase: %v", record)
					}
				}
				if err := scanner.Err(); err != nil {
					t.Fatal(err)
				}
				if starts != 1 || finishes != 1 {
					t.Errorf("rejected fork emitted %d starts and %d finishes, want one session/fork pair", starts, finishes)
				}

				listed, err := client.ListSessions(ctx, &v1.ListSessionsRequest{})
				if err != nil {
					t.Fatal(err)
				}
				if got := uids(listed.GetSessions()); len(got) != 1 || got[0] != uid {
					t.Errorf("sessions after rejected fork = %v, want only parent %s", got, uid)
				}
				after, err := client.GetSession(ctx, &v1.GetSessionRequest{Uid: uid})
				if err != nil {
					t.Fatal(err)
				}
				if !proto.Equal(before, after) {
					t.Errorf("rejected fork changed parent metadata: before=%v after=%v", before, after)
				}
				recordsAfter, err := store.Session(uid).Read(1)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(recordsBefore, recordsAfter) {
					t.Error("rejected fork changed the parent's log")
				}
			})
		}
	}
}
