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
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/aramase/agentsessions/api"
	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/observability"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/sqlitelog"
	"github.com/aramase/agentsessions/wire"
)

// Service implements v1.SessionsServer over a sqlitelog store and the controller.
type Service struct {
	v1.UnimplementedSessionsServer
	store  *sqlitelog.Store
	placer *placement.Placer
	logger *slog.Logger
}

// Option configures a Service.
type Option func(*Service)

// WithLogger enables structured operational logs.
func WithLogger(logger *slog.Logger) Option { return func(s *Service) { s.logger = logger } }

// NewService builds the Sessions service over store, driving executions through placer (which owns
// the Runtime backend and the M0 harness). The bare harness is no longer held here — it comes from
// the backend via the Placer.
func NewService(store *sqlitelog.Store, placer *placement.Placer, opts ...Option) *Service {
	s := &Service{store: store, placer: placer, logger: slog.New(slog.DiscardHandler)}
	for _, opt := range opts {
		opt(s)
	}
	if s.logger == nil {
		s.logger = slog.New(slog.DiscardHandler)
	}
	return s
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
	ctx = observability.EnsureRequestID(ctx)
	finish := observability.Start(ctx, s.logger, "session", "create")
	session := s.session(newUID(), 0, "", 0)
	finish(nil, "session_uid", session.GetMetadata().GetUid())
	return session, nil
}

// GetSession returns the session's current log cursor.
func (s *Service) GetSession(ctx context.Context, req *v1.GetSessionRequest) (session *v1.Session, err error) {
	ctx = observability.EnsureRequestID(ctx)
	finish := observability.Start(ctx, s.logger, "session", "get", "session_uid", req.GetUid())
	defer func() {
		finish(err, "error_kind", serviceErrorKind(err))
	}()

	head, err := s.store.Session(req.GetUid()).Head()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "head: %v", err)
	}
	return s.session(req.GetUid(), head, "", 0), nil
}

// Exec runs one turn against the session's durable log (CAS-guarded by expected_last_seq) and
// streams the committed LogRecords produced by the turn.
func (s *Service) Exec(req *v1.ExecRequest, stream v1.Sessions_ExecServer) (err error) {
	ctx := observability.EnsureRequestID(stream.Context())
	var recordsSent int
	finish := observability.Start(ctx, s.logger, "session", "exec",
		"session_uid", req.GetSession(),
		"expected_last_seq", req.GetExpectedLastSeq(),
		"input_count", len(req.GetInputs()),
	)
	defer func() {
		finish(err, "error_kind", serviceErrorKind(err), "records_sent", recordsSent)
	}()

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
	if _, err := s.placer.Exec(ctx, log, req.GetSession(), inputs, req.GetExpectedLastSeq()); err != nil {
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
		recordsSent++
	}
	return nil
}

// Replay re-delivers committed records from from_seq (read-only; audit / provenance).
func (s *Service) Replay(req *v1.ReplayRequest, stream v1.Sessions_ReplayServer) (err error) {
	ctx := observability.EnsureRequestID(stream.Context())
	var recordsSent int
	finish := observability.Start(ctx, s.logger, "session", "replay",
		"session_uid", req.GetSession(),
		"from_seq", req.GetFromSeq(),
		"to_seq", req.GetToSeq(),
	)
	defer func() {
		finish(err, "error_kind", serviceErrorKind(err), "records_sent", recordsSent)
	}()

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
		recordsSent++
	}
	return nil
}

// Fork branches the session at at_seq into count children (each a new session sharing the parent
// prefix chain), the differentiator ax lacks.
func (s *Service) Fork(ctx context.Context, req *v1.ForkRequest) (response *v1.ForkResponse, err error) {
	ctx = observability.EnsureRequestID(ctx)
	var childrenCreated int
	finish := observability.Start(ctx, s.logger, "session", "fork",
		"parent_session_uid", req.GetSession(),
		"requested_at_seq", req.GetAtSeq(),
		"requested_child_count", req.GetCount(),
	)
	defer func() {
		finish(err, "error_kind", serviceErrorKind(err), "children_created", childrenCreated)
	}()

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
		if err := s.placer.Fork(ctx, parent, child, req.GetSession(), uid, atSeq); err != nil {
			return nil, status.Errorf(codes.Internal, "fork: %v", err)
		}
		h, _ := child.Head()
		children = append(children, s.session(uid, h, req.GetSession(), atSeq))
		childrenCreated++
	}
	return &v1.ForkResponse{Children: children}, nil
}

// Suspend snapshots the incarnation and marks the session cold. The Placer records the SnapshotRef
// in a SUSPEND lifecycle event (§5.1) and frees the worker via the Runtime SPI.
func (s *Service) Suspend(ctx context.Context, req *v1.SuspendRequest) (session *v1.Session, err error) {
	ctx = observability.EnsureRequestID(ctx)
	finish := observability.Start(ctx, s.logger, "session", "suspend", "session_uid", req.GetSession())
	defer func() { finish(err, "error_kind", serviceErrorKind(err)) }()

	log := s.store.Session(req.GetSession())
	if _, err := s.placer.Suspend(ctx, log, req.GetSession()); err != nil {
		return nil, status.Errorf(codes.Internal, "suspend: %v", err)
	}
	h, _ := log.Head()
	sp := s.session(req.GetSession(), h, "", 0)
	sp.ComputeState = v1.ComputeState_COMPUTE_COLD
	return sp, nil
}

// Resume restores the incarnation via the Runtime SPI, re-drives any interrupted turn, and records a
// RESUME marker.
func (s *Service) Resume(ctx context.Context, req *v1.ResumeRequest) (session *v1.Session, err error) {
	ctx = observability.EnsureRequestID(ctx)
	finish := observability.Start(ctx, s.logger, "session", "resume", "session_uid", req.GetSession())
	defer func() { finish(err, "error_kind", serviceErrorKind(err)) }()

	log := s.store.Session(req.GetSession())
	if err := s.placer.Resume(ctx, log, req.GetSession()); err != nil {
		return nil, status.Errorf(codes.Internal, "resume: %v", err)
	}
	h, _ := log.Head()
	sp := s.session(req.GetSession(), h, "", 0)
	sp.ComputeState = v1.ComputeState_COMPUTE_LIVE
	return sp, nil
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

func serviceErrorKind(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline_exceeded"
	}
	switch status.Code(err) {
	case codes.Aborted:
		return "conflict"
	case codes.FailedPrecondition:
		return "failed_precondition"
	case codes.InvalidArgument:
		return "invalid_argument"
	case codes.Canceled:
		return "canceled"
	case codes.DeadlineExceeded:
		return "deadline_exceeded"
	case codes.Unavailable:
		return "unavailable"
	case codes.NotFound:
		return "not_found"
	case codes.AlreadyExists:
		return "already_exists"
	default:
		return "internal"
	}
}
