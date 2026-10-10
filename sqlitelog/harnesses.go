package sqlitelog

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
)

var (
	// ErrHarnessNotFound is returned for a name with no registration row.
	ErrHarnessNotFound = errors.New("sqlitelog: harness not found")
	// ErrHarnessSpecConflict is returned when a name is registered again with a different spec.
	// Registrations are immutable; a new spec needs a new name.
	ErrHarnessSpecConflict = errors.New("sqlitelog: harness name holds a different spec")
	// ErrHarnessRetired is returned when a new session selects a retired harness.
	ErrHarnessRetired = errors.New("sqlitelog: harness is retired")
	// ErrHarnessNameReserved is returned when a registration names a harness that a host sharing
	// the database reserved for a static harness.
	ErrHarnessNameReserved = errors.New("sqlitelog: harness name is reserved for a static harness")
	// ErrHarnessNameCollision is returned when a name a host serves as a static harness also has a
	// registration row.
	ErrHarnessNameCollision = errors.New("sqlitelog: registered harness collides with a static harness name")
)

// HarnessState is a registration's stored state.
type HarnessState string

const (
	HarnessActive  HarnessState = "active"
	HarnessRetired HarnessState = "retired"
)

// RegisterResult says what RegisterHarness did.
type RegisterResult int

const (
	// HarnessCreated: the name was free and now holds an active registration.
	HarnessCreated RegisterResult = iota + 1
	// HarnessUnchanged: the name already held this spec and was active.
	HarnessUnchanged
	// HarnessReactivated: the name already held this spec and was retired; it is active again.
	HarnessReactivated
)

// HarnessRecord is one stored registration. Spec is opaque to the store; SpecDigest is what makes
// a repeat registration recognizable.
type HarnessRecord struct {
	Name         string
	UID          string
	Spec         string
	SpecDigest   string
	State        HarnessState
	RetireReason string
	RetiredAt    time.Time // zero unless retired
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// HarnessListOptions bounds one page of ListHarnesses.
type HarnessListOptions struct {
	After          string // return names strictly greater than this; empty starts at the beginning
	Limit          int    // at most this many rows; must be positive
	IncludeRetired bool
}

const harnessSelect = `SELECT name, uid, spec, spec_digest, state, retire_reason, retired_at, created_at, updated_at FROM harnesses`

type rowScanner interface{ Scan(dest ...any) error }

func scanHarness(row rowScanner) (HarnessRecord, error) {
	var r HarnessRecord
	var state string
	var retired, created, updated int64
	if err := row.Scan(&r.Name, &r.UID, &r.Spec, &r.SpecDigest, &state, &r.RetireReason, &retired, &created, &updated); err != nil {
		return HarnessRecord{}, err
	}
	r.State = HarnessState(state)
	if retired != 0 {
		r.RetiredAt = time.Unix(0, retired)
	}
	r.CreatedAt, r.UpdatedAt = time.Unix(0, created), time.Unix(0, updated)
	return r, nil
}

func harnessByName(q queryRower, name string) (HarnessRecord, error) {
	r, err := scanHarness(q.QueryRow(harnessSelect+` WHERE name = ?`, name))
	if errors.Is(err, sql.ErrNoRows) {
		return HarnessRecord{}, fmt.Errorf("%w: %q", ErrHarnessNotFound, name)
	}
	if err != nil {
		return HarnessRecord{}, fmt.Errorf("sqlitelog: harness %q: %w", name, err)
	}
	return r, nil
}

// RegisterHarness stores rec as an active registration, or recognizes it. The same name and
// SpecDigest is HarnessUnchanged when active and HarnessReactivated when retired; either way the
// stored UID and creation time are kept. A different digest under the name is
// ErrHarnessSpecConflict. The read and the write share one transaction, so two racing registrations
// of one name cannot both report HarnessCreated, even from two processes sharing the database.
func (s *Store) RegisterHarness(rec HarnessRecord) (HarnessRecord, RegisterResult, error) {
	if rec.Name == "" || rec.UID == "" || rec.Spec == "" || rec.SpecDigest == "" {
		return HarnessRecord{}, 0, errors.New("sqlitelog: harness registration needs name, uid, spec and digest")
	}
	tx, err := s.db.Begin()
	if err != nil {
		return HarnessRecord{}, 0, fmt.Errorf("sqlitelog: register harness %q: %w", rec.Name, err)
	}
	defer func() { _ = tx.Rollback() }()

	if reserved, err := nameReserved(tx, rec.Name); err != nil {
		return HarnessRecord{}, 0, err
	} else if reserved {
		return HarnessRecord{}, 0, fmt.Errorf("%w: %q", ErrHarnessNameReserved, rec.Name)
	}
	now := time.Now().UnixNano()
	var result RegisterResult
	existing, err := harnessByName(tx, rec.Name)
	switch {
	case errors.Is(err, ErrHarnessNotFound):
		if _, err := tx.Exec(`INSERT INTO harnesses(name, uid, spec, spec_digest, state, created_at, updated_at)
			VALUES(?, ?, ?, ?, ?, ?, ?)`, rec.Name, rec.UID, rec.Spec, rec.SpecDigest, string(HarnessActive), now, now); err != nil {
			return HarnessRecord{}, 0, fmt.Errorf("sqlitelog: register harness %q: %w", rec.Name, err)
		}
		result = HarnessCreated
	case err != nil:
		return HarnessRecord{}, 0, err
	case existing.SpecDigest != rec.SpecDigest:
		return HarnessRecord{}, 0, fmt.Errorf("%w: %q is %s, request is %s", ErrHarnessSpecConflict, rec.Name, existing.SpecDigest, rec.SpecDigest)
	case existing.State == HarnessActive:
		result = HarnessUnchanged
	default:
		if _, err := tx.Exec(`UPDATE harnesses SET state = ?, retire_reason = '', retired_at = 0, updated_at = ? WHERE name = ?`,
			string(HarnessActive), now, rec.Name); err != nil {
			return HarnessRecord{}, 0, fmt.Errorf("sqlitelog: reactivate harness %q: %w", rec.Name, err)
		}
		result = HarnessReactivated
	}
	out, err := harnessByName(tx, rec.Name)
	if err != nil {
		return HarnessRecord{}, 0, err
	}
	if err := tx.Commit(); err != nil {
		return HarnessRecord{}, 0, fmt.Errorf("sqlitelog: register harness %q: %w", rec.Name, err)
	}
	return out, result, nil
}

func nameReserved(q queryRower, name string) (bool, error) {
	var n int
	if err := q.QueryRow(`SELECT COUNT(*) FROM reserved_harness_names WHERE name = ?`, name).Scan(&n); err != nil {
		return false, fmt.Errorf("sqlitelog: harness %q: %w", name, err)
	}
	return n > 0, nil
}

// ReserveHarnessNames records names as static harness names, so no host sharing the database can
// register them, and fails with ErrHarnessNameCollision if any of them already has a registration
// row, active or retired. The check and the reservation share one transaction, and RegisterHarness
// checks reservations in its own, so a name is either reserved or registered, never both, however
// the hosts that share the database interleave. A reservation is permanent: a host that stops
// serving a static harness does not release its name, because another host may still serve it.
func (s *Store) ReserveHarnessNames(names []string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("sqlitelog: reserve harness names: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	now := time.Now().UnixNano()
	for _, name := range names {
		if name == "" {
			return errors.New("sqlitelog: reserve harness names: empty name")
		}
		switch _, err := harnessByName(tx, name); {
		case err == nil:
			return fmt.Errorf("%w: %q", ErrHarnessNameCollision, name)
		case !errors.Is(err, ErrHarnessNotFound):
			return err
		}
		if _, err := tx.Exec(`INSERT OR IGNORE INTO reserved_harness_names(name, reserved_at) VALUES(?, ?)`, name, now); err != nil {
			return fmt.Errorf("sqlitelog: reserve harness name %q: %w", name, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlitelog: reserve harness names: %w", err)
	}
	return nil
}

// Harness returns one registration, or ErrHarnessNotFound.
func (s *Store) Harness(name string) (HarnessRecord, error) { return harnessByName(s.db, name) }

// ListHarnesses returns registrations in name order, starting after opts.After.
func (s *Store) ListHarnesses(opts HarnessListOptions) ([]HarnessRecord, error) {
	if opts.Limit <= 0 {
		return nil, errors.New("sqlitelog: list harnesses needs a positive limit")
	}
	q := harnessSelect + ` WHERE name > ?`
	args := []any{opts.After}
	if !opts.IncludeRetired {
		q += ` AND state = ?`
		args = append(args, string(HarnessActive))
	}
	q += ` ORDER BY name LIMIT ?`
	args = append(args, opts.Limit)
	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil, fmt.Errorf("sqlitelog: list harnesses: %w", err)
	}
	defer rows.Close()
	var out []HarnessRecord
	for rows.Next() {
		r, err := scanHarness(rows)
		if err != nil {
			return nil, fmt.Errorf("sqlitelog: list harnesses: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlitelog: list harnesses: %w", err)
	}
	return out, nil
}

// RetireHarness marks a registration retired and returns it. Retiring a retired harness changes
// nothing and keeps its first retire time and reason, so a retry does not rewrite history.
func (s *Store) RetireHarness(name, reason string) (HarnessRecord, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return HarnessRecord{}, fmt.Errorf("sqlitelog: retire harness %q: %w", name, err)
	}
	defer func() { _ = tx.Rollback() }()
	now := time.Now().UnixNano()
	if _, err := tx.Exec(`UPDATE harnesses SET state = ?, retire_reason = ?, retired_at = ?, updated_at = ?
		WHERE name = ? AND state = ?`, string(HarnessRetired), reason, now, now, name, string(HarnessActive)); err != nil {
		return HarnessRecord{}, fmt.Errorf("sqlitelog: retire harness %q: %w", name, err)
	}
	out, err := harnessByName(tx, name)
	if err != nil {
		return HarnessRecord{}, err
	}
	if err := tx.Commit(); err != nil {
		return HarnessRecord{}, fmt.Errorf("sqlitelog: retire harness %q: %w", name, err)
	}
	return out, nil
}

// PutSessionOnActiveHarness writes a new session's metadata only if its harness may take a new
// session. The harness check and the insert share one transaction, so a RetireHarness that has
// returned is always seen: no session created after it can land on the retired harness, even when
// another process shares the database.
//
// registered is how the caller says the name is not one of its static harnesses. For a registered
// name, a missing row is ErrHarnessNotFound and a retired one is ErrHarnessRetired. For a static
// name, any row is ErrHarnessNameCollision: another host registered the name, so it means two
// harnesses, and the session is refused rather than placed on either.
func (s *Store) PutSessionOnActiveHarness(m SessionMeta, registered bool) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("sqlitelog: put session %q: %w", m.UID, err)
	}
	defer func() { _ = tx.Rollback() }()
	rec, err := harnessByName(tx, m.Harness)
	switch {
	case errors.Is(err, ErrHarnessNotFound):
		if registered {
			return err
		}
	case err != nil:
		return err
	case !registered:
		return fmt.Errorf("%w: %q", ErrHarnessNameCollision, m.Harness)
	case rec.State != HarnessActive:
		return fmt.Errorf("%w: %q", ErrHarnessRetired, m.Harness)
	}
	if err := s.putSession(tx, m); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlitelog: put session %q: %w", m.UID, err)
	}
	return nil
}
