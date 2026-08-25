package sqlitelog

import (
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aramase/agentsessions/api"
)

// ErrSessionNotFound is returned for a session UID with no metadata row. It is distinct from an
// empty log: a session exists from the moment it is created, before it has any events.
var ErrSessionNotFound = errors.New("sqlitelog: session not found")

// ErrInvalidPageToken is returned for a page token the store cannot parse. Resuming from page one
// instead would silently re-deliver rows the caller already saw, so a bad cursor is an error.
var ErrInvalidPageToken = errors.New("sqlitelog: invalid page token")

// Page sizing for ListSessions. A caller asking for nothing gets DefaultPageSize; a caller asking
// for more than MaxPageSize is clamped rather than refused, since an oversized ask is not an error,
// it just cannot be honored.
const (
	DefaultPageSize = 50
	MaxPageSize     = 500
)

// SessionMeta is the part of a session that the event log cannot reproduce: who it belongs to, what
// it runs, and where it was forked from. Everything here is supplied at creation and is durable
// from that moment, which is what makes a session with no events listable.
type SessionMeta struct {
	UID       string
	Project   string
	Name      string
	Harness   string
	Model     string
	ParentUID string // set iff this session was forked
	ForkSeq   int64  // parent event seq forked at
	CreatedAt time.Time
	UpdatedAt time.Time
}

// SessionInfo is the stored metadata plus the two fields the store derives: the log cursor, and the
// compute state projected from the session's lifecycle events.
type SessionInfo struct {
	SessionMeta
	LastSeq      int64
	ComputeState api.ComputeState
}

// ListOptions bounds one page of ListSessions.
type ListOptions struct {
	Project   string // exact match; empty means DefaultProject, never "all tenants"
	PageSize  int
	PageToken string
}

// SessionPage is one page of results plus the cursor that continues it.
type SessionPage struct {
	Sessions      []SessionInfo
	NextPageToken string // empty on the last page
}

// eventComputeState maps a committed event to the compute state it implies, reporting false for
// events that say nothing about the incarnation.
//
// A non-lifecycle event is evidence that compute was running: only a live incarnation produces a
// turn's input and output records. This is load-bearing rather than incidental. Placer.Exec appends
// no lifecycle marker of its own — RESUME is written only by Placer.Resume (placement.go:304) — so
// a session that has only ever been Exec'd, which is every session before its first suspend, would
// otherwise sit at NONE forever and never look live to a caller.
//
// FORK lands the child at COLD. controller.Fork copies the parent's prefix through child.Append, so
// without this the copied records would leave a child looking live when nothing is running for it;
// the FORK marker is always the child's last event, so it settles the projection. COLD is also the
// truthful state: a child begins from its parent's fork-point snapshot with no compute of its own,
// and is placed on its first Exec.
func eventComputeState(ev api.Event) (api.ComputeState, bool) {
	if ev.Kind != api.EventLifecycle {
		return api.ComputeLive, true
	}
	if ev.Lifecycle == nil {
		return api.ComputeNone, false
	}
	switch ev.Lifecycle.Kind {
	case api.LifecycleSuspend, api.LifecycleFork:
		return api.ComputeCold, true
	case api.LifecycleResume:
		return api.ComputeLive, true
	default:
		return api.ComputeNone, false
	}
}

// PutSession writes a session's metadata, creating the row if the session is new.
//
// It deliberately preserves two fields it does not own: `fence` belongs to the incarnation
// sequence and `compute_state` to the lifecycle projection, so re-registering an existing session
// updates its metadata without rewinding its identity. This is what lets Fork write child metadata
// around a controller.Fork that has already fenced and populated the child log.
//
// `created_at` follows the caller: an explicit CreatedAt is honored, and a zero one means "now on
// insert, leave alone on update", so a later re-registration does not restamp a session as new.
func (s *Store) PutSession(m SessionMeta) error {
	if m.UID == "" {
		return errors.New("sqlitelog: session metadata requires a uid")
	}
	if m.Project == "" {
		m.Project = s.defaultProject
	}
	if m.Project == "" {
		m.Project = DefaultProject
	}
	now := time.Now()
	created := m.CreatedAt
	explicitCreated := !created.IsZero()
	if !explicitCreated {
		created = now
	}
	_, err := s.db.Exec(
		`INSERT INTO sessions(session, fence, project, name, harness, model,
		                      parent_uid, fork_seq, compute_state, created_at, updated_at)
		 VALUES(?, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(session) DO UPDATE SET
		   project    = excluded.project,
		   name       = excluded.name,
		   harness    = excluded.harness,
		   model      = excluded.model,
		   parent_uid = excluded.parent_uid,
		   fork_seq   = excluded.fork_seq,
		   created_at = CASE WHEN ? OR sessions.created_at = 0
		                     THEN excluded.created_at ELSE sessions.created_at END,
		   updated_at = excluded.updated_at`,
		m.UID, m.Project, m.Name, m.Harness, m.Model,
		m.ParentUID, m.ForkSeq, string(api.ComputeNone), created.UnixNano(), now.UnixNano(),
		explicitCreated,
	)
	if err != nil {
		return fmt.Errorf("sqlitelog: put session %q: %w", m.UID, err)
	}
	return nil
}

// sessionSelect is shared by SessionInfo and ListSessions so a single row shape backs both. The
// LEFT JOIN keeps sessions with no events, which is exactly the population a listing built from the
// event table alone would drop.
const sessionSelect = `
SELECT s.session, s.project, s.name, s.harness, s.model, s.parent_uid, s.fork_seq,
       s.compute_state, s.created_at, s.updated_at, COALESCE(MAX(e.seq), 0)
FROM sessions s LEFT JOIN events e ON e.session = s.session`

// SessionInfo returns one session's metadata together with its derived log cursor and compute
// state.
func (s *Store) SessionInfo(uid string) (SessionInfo, error) {
	row := s.db.QueryRow(sessionSelect+`
		WHERE s.session = ? GROUP BY s.session`, uid)
	info, err := scanSession(row)
	if errors.Is(err, sql.ErrNoRows) {
		return SessionInfo{}, fmt.Errorf("%w: %s", ErrSessionNotFound, uid)
	}
	if err != nil {
		return SessionInfo{}, fmt.Errorf("sqlitelog: get session %q: %w", uid, err)
	}
	return info, nil
}

// ListSessions enumerates one project's sessions, newest first.
//
// Paging is keyset, not offset: the cursor carries the last row's (created_at, uid) and the next
// page resumes strictly after it. Sessions are created while a client pages through them, and an
// offset would then skip or repeat rows. The (created_at DESC, session) ordering is total, so the
// cursor always identifies exactly one position.
func (s *Store) ListSessions(opts ListOptions) (SessionPage, error) {
	project := opts.Project
	if project == "" {
		project = s.defaultProject
	}
	if project == "" {
		project = DefaultProject
	}
	size := opts.PageSize
	switch {
	case size <= 0:
		size = DefaultPageSize
	case size > MaxPageSize:
		size = MaxPageSize
	}

	args := []any{project}
	where := `WHERE s.project = ?`
	if opts.PageToken != "" {
		createdAt, uid, err := decodeCursor(opts.PageToken)
		if err != nil {
			return SessionPage{}, err
		}
		where += ` AND (s.created_at < ? OR (s.created_at = ? AND s.session > ?))`
		args = append(args, createdAt, createdAt, uid)
	}
	// One row beyond the page tells us whether a further page exists, without a second count query.
	args = append(args, size+1)

	rows, err := s.db.Query(sessionSelect+`
		`+where+`
		GROUP BY s.session
		ORDER BY s.created_at DESC, s.session ASC
		LIMIT ?`, args...)
	if err != nil {
		return SessionPage{}, fmt.Errorf("sqlitelog: list sessions: %w", err)
	}
	defer rows.Close()

	var page SessionPage
	for rows.Next() {
		info, err := scanSession(rows)
		if err != nil {
			return SessionPage{}, fmt.Errorf("sqlitelog: list sessions: %w", err)
		}
		if len(page.Sessions) == size {
			last := page.Sessions[size-1]
			page.NextPageToken = encodeCursor(last.CreatedAt, last.UID)
			break
		}
		page.Sessions = append(page.Sessions, info)
	}
	if err := rows.Err(); err != nil {
		return SessionPage{}, fmt.Errorf("sqlitelog: list sessions: %w", err)
	}
	return page, nil
}

// scanner is satisfied by both *sql.Row and *sql.Rows so one scan serves the single-row and
// listing paths.
type scanner interface{ Scan(dest ...any) error }

func scanSession(row scanner) (SessionInfo, error) {
	var (
		info                 SessionInfo
		state                string
		createdAt, updatedAt int64
	)
	if err := row.Scan(
		&info.UID, &info.Project, &info.Name, &info.Harness, &info.Model,
		&info.ParentUID, &info.ForkSeq, &state, &createdAt, &updatedAt, &info.LastSeq,
	); err != nil {
		return SessionInfo{}, err
	}
	info.CreatedAt = time.Unix(0, createdAt)
	info.UpdatedAt = time.Unix(0, updatedAt)
	info.ComputeState = api.ComputeState(state)
	if state == "" {
		info.ComputeState = api.ComputeNone
	}
	return info, nil
}

// The cursor is an opaque base64 of the sort key. It is not encrypted or signed: it carries only a
// timestamp and a UID the caller was just shown, so there is nothing in it to protect.
func encodeCursor(createdAt time.Time, uid string) string {
	return base64.RawURLEncoding.EncodeToString(
		[]byte(strconv.FormatInt(createdAt.UnixNano(), 10) + "\x00" + uid),
	)
}

func decodeCursor(token string) (int64, string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return 0, "", fmt.Errorf("%w: %q", ErrInvalidPageToken, token)
	}
	createdAt, uid, ok := strings.Cut(string(raw), "\x00")
	if !ok {
		return 0, "", fmt.Errorf("%w: %q", ErrInvalidPageToken, token)
	}
	nanos, err := strconv.ParseInt(createdAt, 10, 64)
	if err != nil {
		return 0, "", fmt.Errorf("%w: %q", ErrInvalidPageToken, token)
	}
	return nanos, uid, nil
}
