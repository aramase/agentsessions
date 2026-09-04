// Package session implements the client-facing Sessions gRPC service, backed by the durable
// sqlitelog journal and the event-sourced controller. It is the top seam a producer (GitHub,
// Foundry, a custom app) or the agentctl CLI drives. For M0 it hosts a single harness (echo); the
// model and harness are injected.
//
// A session UID is a key in the sqlitelog store. CreateSession persists a metadata row up front,
// so a session exists — and is listable — from the moment it is created, before it has any events
// and before any compute is provisioned. The log remains the authority for everything the log
// knows (cursor, lifecycle); the metadata row carries only what the log cannot express, such as
// the project, display name, and configured harness/model.
package session

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

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
	store          *sqlitelog.Store
	registry       *placement.Registry
	logger         *slog.Logger
	defaultProject string
}

// Option configures a Service.
type Option func(*Service)

// WithLogger enables structured operational logs.
func WithLogger(logger *slog.Logger) Option { return func(s *Service) { s.logger = logger } }

// WithDefaultProject sets the project assigned to sessions created without one. It must match the
// value given to sqlitelog.Open, so that sessions created through this service and sessions
// backfilled by the store migration land in the same project and a project-filtered list sees
// both. An empty project is never persisted: ListSessions filters on exact match, so a session
// stored under "" would be invisible to every real caller.
func WithDefaultProject(project string) Option {
	return func(s *Service) {
		if project != "" {
			s.defaultProject = project
		}
	}
}

// NewService builds the Sessions service over store, routing each session to a harness through
// registry. There is no default-harness option: the registry already names its default, and a
// service-level override could name a harness the host does not serve, which would record a
// harness on every new session that no execution could ever resolve.
func NewService(store *sqlitelog.Store, registry *placement.Registry, opts ...Option) *Service {
	s := &Service{
		store:          store,
		registry:       registry,
		logger:         slog.New(slog.DiscardHandler),
		defaultProject: sqlitelog.DefaultProject,
	}
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

// sessionProto renders stored metadata as the wire type. compute_state comes from the projection
// the store maintains inside the append transaction, so it reflects the last recorded lifecycle
// transition rather than a live probe of the backend — see sqlitelog for what that does and does
// not guarantee.
func sessionProto(info sqlitelog.SessionInfo) *v1.Session {
	md := &v1.ResourceMetadata{
		Project: info.Project,
		Name:    info.Name,
		Uid:     info.UID,
	}
	if !info.CreatedAt.IsZero() {
		md.CreateTime = timestamppb.New(info.CreatedAt)
	}
	if !info.UpdatedAt.IsZero() {
		md.UpdateTime = timestamppb.New(info.UpdatedAt)
	}
	return &v1.Session{
		Metadata:     md,
		Harness:      info.Harness,
		Model:        info.Model,
		ComputeState: wire.ComputeStateToProto(info.ComputeState),
		LastSeq:      info.LastSeq,
		ParentUid:    info.ParentUID,
		ForkSeq:      info.ForkSeq,
	}
}

// CreateSession persists the session's metadata and returns it. The metadata row is written before
// any event exists, which is what makes a freshly created session appear in ListSessions and
// survive a restart; compute is still provisioned lazily on first Exec.
func (s *Service) CreateSession(ctx context.Context, req *v1.CreateSessionRequest) (session *v1.Session, err error) {
	ctx = observability.EnsureRequestID(ctx)
	finish := observability.Start(ctx, s.logger, "session", "create")
	uid := newUID()
	defer func() { finish(err, "error_kind", serviceErrorKind(err), "session_uid", uid) }()

	spec := req.GetSession()
	meta := sqlitelog.SessionMeta{
		UID:     uid,
		Project: spec.GetMetadata().GetProject(),
		Name:    spec.GetMetadata().GetName(),
		Harness: spec.GetHarness(),
		Model:   spec.GetModel(),
	}
	if meta.Project == "" {
		meta.Project = s.defaultProject
	}
	if meta.Harness == "" {
		meta.Harness = s.registry.Default()
	} else if _, err := s.registry.For(meta.Harness); err != nil {
		// Reject at create rather than at first Exec. Storing an unservable harness would make
		// the session listable but permanently unrunnable, and the failure would surface later
		// somewhere that looks unrelated.
		return nil, harnessError(err)
	}
	if err := s.store.PutSession(meta); err != nil {
		return nil, status.Errorf(codes.Internal, "create session: %v", err)
	}
	info, err := s.store.SessionInfo(uid)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create session: %v", err)
	}
	return sessionProto(info), nil
}

// GetSession returns the session's stored metadata and current log cursor.
func (s *Service) GetSession(ctx context.Context, req *v1.GetSessionRequest) (session *v1.Session, err error) {
	ctx = observability.EnsureRequestID(ctx)
	finish := observability.Start(ctx, s.logger, "session", "get", "session_uid", req.GetUid())
	defer func() {
		finish(err, "error_kind", serviceErrorKind(err))
	}()

	if req.GetUid() == "" {
		return nil, status.Error(codes.InvalidArgument, "uid is required")
	}
	info, err := s.store.SessionInfo(req.GetUid())
	if err != nil {
		return nil, sessionStoreError(err, req.GetUid())
	}
	return sessionProto(info), nil
}

// ListSessions enumerates sessions in a project, newest first, from the store rather than from any
// in-memory view — so the listing is complete after a restart and includes sessions that have no
// events and no compute yet.
//
// Paging is keyset, not offset: the token carries the last (create time, uid) seen, so a session
// created while a caller is walking pages neither skips nor duplicates a row. An unparseable token
// is rejected rather than silently treated as "start over", which would otherwise show up as a
// caller quietly re-reading page one forever.
func (s *Service) ListSessions(ctx context.Context, req *v1.ListSessionsRequest) (resp *v1.ListSessionsResponse, err error) {
	ctx = observability.EnsureRequestID(ctx)
	var returned int
	finish := observability.Start(ctx, s.logger, "session", "list",
		"project", req.GetProject(),
		"page_size", req.GetPageSize(),
	)
	defer func() {
		finish(err, "error_kind", serviceErrorKind(err), "sessions_returned", returned)
	}()

	project := req.GetProject()
	if project == "" {
		project = s.defaultProject
	}
	page, err := s.store.ListSessions(sqlitelog.ListOptions{
		Project:   project,
		PageSize:  int(req.GetPageSize()),
		PageToken: req.GetPageToken(),
	})
	if err != nil {
		if errors.Is(err, sqlitelog.ErrInvalidPageToken) {
			return nil, status.Errorf(codes.InvalidArgument, "page_token: %v", err)
		}
		return nil, status.Errorf(codes.Internal, "list sessions: %v", err)
	}
	out := make([]*v1.Session, 0, len(page.Sessions))
	for _, info := range page.Sessions {
		out = append(out, sessionProto(info))
	}
	returned = len(out)
	return &v1.ListSessionsResponse{Sessions: out, NextPageToken: page.NextPageToken}, nil
}

// placerFor routes a session to the harness that runs it. override comes from
// ExecRequest.harness, which the contract defines as "empty = session default"; anything else
// falls back to the harness recorded on the session at create time.
//
// The lookup is by stored harness rather than by a single configured one, which is what makes
// Session.harness mean something. A session that names a harness this host does not serve fails
// here instead of silently running on whatever the host happens to have wired.
func (s *Service) placerFor(uid, override string) (*placement.Placer, error) {
	if uid == "" {
		return nil, status.Error(codes.InvalidArgument, "session is required")
	}
	harness := override
	if harness == "" {
		info, err := s.store.SessionInfo(uid)
		if err != nil {
			return nil, sessionStoreError(err, uid)
		}
		harness = info.Harness
	}
	p, err := s.registry.For(harness)
	if err != nil {
		return nil, harnessError(err)
	}
	return p, nil
}

// harnessError reports an unservable harness as InvalidArgument. Naming a harness the host does
// not run is a caller mistake, and the message lists what is registered so the caller can correct
// it without reading the host's configuration.
func harnessError(err error) error {
	if errors.Is(err, placement.ErrUnknownHarness) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	return status.Errorf(codes.Internal, "resolve harness: %v", err)
}

// sessionStoreError maps a store lookup failure to a gRPC code. An unknown UID is NotFound rather
// than Internal so a caller can tell a bad reference from a broken host.
func sessionStoreError(err error, uid string) error {
	if errors.Is(err, sqlitelog.ErrSessionNotFound) {
		return status.Errorf(codes.NotFound, "session %q not found", uid)
	}
	return status.Errorf(codes.Internal, "session %q: %v", uid, err)
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
	placer, err := s.placerFor(req.GetSession(), req.GetHarness())
	if err != nil {
		return err
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
	if _, err := placer.Exec(ctx, log, req.GetSession(), inputs, req.GetExpectedLastSeq()); err != nil {
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

// MaxForkChildren bounds one fan-out. count arrives off the wire and sizes both an allocation and a
// provisioning loop, and each child costs a full snapshot restore on a memory-capable backend, so an
// unbounded value is a resource-exhaustion vector rather than a useful request. It is a guardrail, not
// a statement about how wide forking can scale.
const MaxForkChildren = 128

// Fork branches the session at at_seq into count children (each a new session sharing the parent
// prefix chain), the differentiator ax lacks. The whole fan-out branches from one parent checkpoint,
// so every child starts from identical state.
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

	placer, err := s.placerFor(req.GetSession(), "")
	if err != nil {
		return nil, err
	}
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
	if count > MaxForkChildren {
		return nil, status.Errorf(codes.InvalidArgument, "count %d exceeds the maximum of %d children per fork", count, MaxForkChildren)
	}
	// Validated before anything is provisioned: a name/count mismatch is a caller mistake, and
	// failing after the fan-out would leave children the caller cannot name.
	childNames := req.GetChildNames()
	if len(childNames) != 0 && len(childNames) != count {
		return nil, status.Errorf(codes.InvalidArgument, "child_names has %d entries, want 0 or exactly count (%d)", len(childNames), count)
	}
	children := make([]placement.ForkChild, 0, count)
	for i := 0; i < count; i++ {
		uid := newUID()
		children = append(children, placement.ForkChild{UID: uid, Log: s.store.Session(uid)})
	}
	// The fan-out is all-or-nothing: Placer.Fork rolls back everything it provisioned on failure,
	// so children_created stays 0 rather than reporting compute that no longer exists.
	if err := placer.Fork(ctx, parent, req.GetSession(), children, atSeq); err != nil {
		return nil, forkError(err)
	}
	childrenCreated = len(children)
	// Child metadata is written only after the fan-out commits. Placer.forkSource refuses an
	// unplaceable fork with no side effects at all, and registering children first would turn that
	// into a listing full of phantom sessions the caller was never given UIDs for and cannot
	// delete. Children inherit the parent's project, harness, and model: a fork is the same
	// workload branched, so it must stay in the tenant that owns the parent rather than landing in
	// the service default that copying the log would otherwise give it. The name is deliberately
	// not inherited; see child_names in the proto.
	parentInfo, err := s.store.SessionInfo(req.GetSession())
	if err != nil {
		return nil, sessionStoreError(err, req.GetSession())
	}
	out := make([]*v1.Session, 0, len(children))
	for i, child := range children {
		var name string
		if len(childNames) != 0 {
			name = childNames[i]
		}
		if err := s.store.PutSession(sqlitelog.SessionMeta{
			UID:       child.UID,
			Project:   parentInfo.Project,
			Name:      name,
			Harness:   parentInfo.Harness,
			Model:     parentInfo.Model,
			ParentUID: req.GetSession(),
			ForkSeq:   atSeq,
		}); err != nil {
			return nil, status.Errorf(codes.Internal, "fork: record child %q: %v", child.UID, err)
		}
		info, err := s.store.SessionInfo(child.UID)
		if err != nil {
			return nil, sessionStoreError(err, child.UID)
		}
		out = append(out, sessionProto(info))
	}
	return &v1.ForkResponse{Children: out}, nil
}

// forkError surfaces a refused fork as FailedPrecondition rather than Internal. A fork is refused
// when the harness's capabilities cannot realize it — e.g. a REQUIRES_MEMORY_SNAPSHOT session forked
// at a historical seq, which no memory snapshot can reproduce — which is a caller-visible
// precondition, not a host fault. A concurrent writer that invalidated the fork point is Aborted, so
// the caller knows a retry is meaningful.
func forkError(err error) error {
	switch {
	case errors.Is(err, placement.ErrUnplaceable):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, eventlog.ErrConflict), errors.Is(err, eventlog.ErrFenced):
		return status.Error(codes.Aborted, err.Error())
	default:
		return status.Errorf(codes.Internal, "fork: %v", err)
	}
}

// Suspend snapshots the incarnation and marks the session cold. The Placer records the SnapshotRef
// in a SUSPEND lifecycle event (§5.1) and frees the worker via the Runtime SPI.
func (s *Service) Suspend(ctx context.Context, req *v1.SuspendRequest) (session *v1.Session, err error) {
	ctx = observability.EnsureRequestID(ctx)
	finish := observability.Start(ctx, s.logger, "session", "suspend", "session_uid", req.GetSession())
	defer func() { finish(err, "error_kind", serviceErrorKind(err)) }()

	placer, err := s.placerFor(req.GetSession(), "")
	if err != nil {
		return nil, err
	}
	log := s.store.Session(req.GetSession())
	if _, err := placer.Suspend(ctx, log, req.GetSession()); err != nil {
		return nil, status.Errorf(codes.Internal, "suspend: %v", err)
	}
	// The SUSPEND append already moved the stored compute_state projection, so re-reading is
	// what keeps the response and a subsequent ListSessions from disagreeing.
	info, err := s.store.SessionInfo(req.GetSession())
	if err != nil {
		return nil, sessionStoreError(err, req.GetSession())
	}
	return sessionProto(info), nil
}

// Resume restores the incarnation via the Runtime SPI, re-drives any interrupted turn, and records a
// RESUME marker.
func (s *Service) Resume(ctx context.Context, req *v1.ResumeRequest) (session *v1.Session, err error) {
	ctx = observability.EnsureRequestID(ctx)
	finish := observability.Start(ctx, s.logger, "session", "resume", "session_uid", req.GetSession())
	defer func() { finish(err, "error_kind", serviceErrorKind(err)) }()

	placer, err := s.placerFor(req.GetSession(), "")
	if err != nil {
		return nil, err
	}
	log := s.store.Session(req.GetSession())
	if err := placer.Resume(ctx, log, req.GetSession()); err != nil {
		return nil, status.Errorf(codes.Internal, "resume: %v", err)
	}
	info, err := s.store.SessionInfo(req.GetSession())
	if err != nil {
		return nil, sessionStoreError(err, req.GetSession())
	}
	return sessionProto(info), nil
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
