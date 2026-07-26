// Package session implements the client-facing Sessions gRPC service, backed by the durable
// sqlitelog journal and the event-sourced controller. It is the top seam a producer (GitHub,
// Foundry, a custom app) or the agentctl CLI drives. For M0 it hosts a single harness (echo); the
// model and harness are injected. A session UID is a key in the sqlitelog store; a session "exists"
// once it has events, so CreateSession simply mints a UID.
package session

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/sqlitelog"
	"github.com/aramase/agentsessions/wire"
)

// Service implements v1.SessionsServer over a sqlitelog store and the controller.
type Service struct {
	v1.UnimplementedSessionsServer
	store  *sqlitelog.Store
	placer *placement.Placer
}

// NewService builds the Sessions service over store, driving executions through placer (which owns
// the Runtime backend and the M0 harness). The bare harness is no longer held here — it comes from
// the backend via the Placer.
func NewService(store *sqlitelog.Store, placer *placement.Placer) *Service {
	return &Service{store: store, placer: placer}
}

func newUID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return "sess-" + hex.EncodeToString(b[:])
}

func (s *Service) session(uid string, lastSeq int64, parent string, forkSeq int64) *v1.Session {
	return &v1.Session{
		Metadata:  &v1.ResourceMetadata{Uid: uid},
		Harness:   "echo",
		LastSeq:   lastSeq,
		ParentUid: parent,
		ForkSeq:   forkSeq,
	}
}

// CreateSession mints a fresh session UID. The session materializes lazily on first Exec.
func (s *Service) CreateSession(ctx context.Context, req *v1.CreateSessionRequest) (*v1.Session, error) {
	return s.session(newUID(), 0, "", 0), nil
}

// GetSession returns the session's current log cursor.
func (s *Service) GetSession(ctx context.Context, req *v1.GetSessionRequest) (*v1.Session, error) {
	head, err := s.store.Session(req.GetUid()).Head()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "head: %v", err)
	}
	return s.session(req.GetUid(), head, "", 0), nil
}

// Exec runs one turn against the session's durable log (CAS-guarded by expected_last_seq) and
// streams the committed LogRecords produced by the turn.
func (s *Service) Exec(req *v1.ExecRequest, stream v1.Sessions_ExecServer) error {
	if req.GetSession() == "" {
		return status.Error(codes.InvalidArgument, "session is required")
	}
	log := s.store.Session(req.GetSession())
	headBefore, err := log.Head()
	if err != nil {
		return status.Errorf(codes.Internal, "head: %v", err)
	}
	inputs := make([]api.Message, 0, len(req.GetInputs()))
	for _, m := range req.GetInputs() {
		if dm := wire.MessageFromProto(m); dm != nil {
			inputs = append(inputs, *dm)
		}
	}
	// Route the turn through the placement seam: Create the incarnation, mint+bind the fence, and
	// drive the placed harness through the Runtime SPI instead of a co-located controller.
	if _, err := s.placer.Exec(stream.Context(), log, req.GetSession(), inputs, req.GetExpectedLastSeq()); err != nil {
		return execError(err)
	}
	recs, err := log.Read(headBefore + 1)
	if err != nil {
		return status.Errorf(codes.Internal, "read: %v", err)
	}
	for _, r := range recs {
		if err := stream.Send(&v1.ExecUpdate{Update: &v1.ExecUpdate_Record{Record: eventlog.RecordToProto(r)}}); err != nil {
			return err
		}
	}
	return nil
}

// Replay re-delivers committed records from from_seq (read-only; audit / provenance).
func (s *Service) Replay(req *v1.ReplayRequest, stream v1.Sessions_ReplayServer) error {
	from := req.GetFromSeq()
	if from <= 0 {
		from = 1
	}
	recs, err := s.store.Session(req.GetSession()).Read(from)
	if err != nil {
		return status.Errorf(codes.Internal, "read: %v", err)
	}
	for _, r := range recs {
		if to := req.GetToSeq(); to != 0 && r.Seq > to {
			break
		}
		if err := stream.Send(eventlog.RecordToProto(r)); err != nil {
			return err
		}
	}
	return nil
}

// Fork branches the session at at_seq into count children (each a new session sharing the parent
// prefix chain), the differentiator ax lacks.
func (s *Service) Fork(ctx context.Context, req *v1.ForkRequest) (*v1.ForkResponse, error) {
	parent := s.store.Session(req.GetSession())
	atSeq := req.GetAtSeq()
	if atSeq <= 0 {
		h, err := parent.Head()
		if err != nil {
			return nil, status.Errorf(codes.Internal, "head: %v", err)
		}
		atSeq = h
	}
	count := int(req.GetCount())
	if count <= 0 {
		count = 1
	}
	var children []*v1.Session
	for i := 0; i < count; i++ {
		uid := newUID()
		child := s.store.Session(uid)
		if err := controller.Fork(parent, child, atSeq); err != nil {
			return nil, status.Errorf(codes.Internal, "fork: %v", err)
		}
		h, _ := child.Head()
		children = append(children, s.session(uid, h, req.GetSession(), atSeq))
	}
	return &v1.ForkResponse{Children: children}, nil
}

// Suspend marks the session cold. In the co-located model the journal is already durable, so this
// records a SUSPEND lifecycle marker; the worker is freed by the process exiting.
func (s *Service) Suspend(ctx context.Context, req *v1.SuspendRequest) (*v1.Session, error) {
	log := s.store.Session(req.GetSession())
	if err := appendLifecycle(log, api.LifecycleSuspend); err != nil {
		return nil, status.Errorf(codes.Internal, "suspend: %v", err)
	}
	h, _ := log.Head()
	sp := s.session(req.GetSession(), h, "", 0)
	sp.ComputeState = v1.ComputeState_COMPUTE_COLD
	return sp, nil
}

// Resume re-drives any interrupted turn (crash-recovery via replay) and records a RESUME marker.
func (s *Service) Resume(ctx context.Context, req *v1.ResumeRequest) (*v1.Session, error) {
	log := s.store.Session(req.GetSession())
	c, err := controller.New(log, s.placer.Model())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "controller: %v", err)
	}
	if _, err := c.Resume(ctx, s.placer.Harness()); err != nil {
		return nil, status.Errorf(codes.Internal, "resume: %v", err)
	}
	if err := appendLifecycle(log, api.LifecycleResume); err != nil {
		return nil, status.Errorf(codes.Internal, "resume marker: %v", err)
	}
	h, _ := log.Head()
	sp := s.session(req.GetSession(), h, "", 0)
	sp.ComputeState = v1.ComputeState_COMPUTE_LIVE
	return sp, nil
}

func appendLifecycle(log eventlog.Store, kind api.LifecycleKind) error {
	fence, err := log.NewFence()
	if err != nil {
		return err
	}
	head, err := log.Head()
	if err != nil {
		return err
	}
	_, err = log.Append(head, fence, api.Event{Kind: api.EventLifecycle, Lifecycle: &api.Lifecycle{Kind: kind}})
	return err
}

func execError(err error) error {
	switch {
	case errors.Is(err, placement.ErrUnplaceable):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, eventlog.ErrConflict), errors.Is(err, eventlog.ErrFenced):
		return status.Error(codes.Aborted, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
