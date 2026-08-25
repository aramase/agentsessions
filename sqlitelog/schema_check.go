package sqlitelog

import (
	"database/sql"
	"errors"
	"fmt"
)

// ErrObsoleteSchema reports a database written before `sessions` carried metadata columns.
var ErrObsoleteSchema = errors.New("sqlitelog: obsolete database schema")

// checkSchema rejects a database whose `sessions` table predates the metadata columns.
//
// It runs before the schema is applied: the listing index names `sessions.project`, so on such a
// database CREATE INDEX fails first with a bare "no such column: project". CREATE TABLE IF NOT
// EXISTS is likewise a no-op on a table that already exists, so it cannot repair one either.
//
// There is deliberately no migration. This module has never been tagged, released, or published to
// the module proxy, and its only consumers build it through a local replace directive, so no
// database written by an older build exists outside a developer's scratch directory. Carrying an
// ALTER TABLE ladder and a backfill for that population is not worth the code or the write
// transaction it would hold at every Open. If versioned upgrades are ever needed, add PRAGMA
// user_version and a ladder then.
func checkSchema(db *sql.DB) error {
	var present string
	err := db.QueryRow(
		`SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'sessions'`,
	).Scan(&present)
	if errors.Is(err, sql.ErrNoRows) {
		return nil // A fresh database; the schema below creates the table in full.
	}
	if err != nil {
		return fmt.Errorf("sqlitelog: inspect schema: %w", err)
	}
	columns, err := tableColumns(db, "sessions")
	if err != nil {
		return err
	}
	for _, required := range []string{"project", "created_at"} {
		if !columns[required] {
			return fmt.Errorf(
				"%w: sessions is missing %q; this database was written by an older build, delete it and start again",
				ErrObsoleteSchema, required,
			)
		}
	}
	return nil
}

func tableColumns(db *sql.DB, table string) (map[string]bool, error) {
	// PRAGMA table_info does not accept a bound parameter; table is a package constant, not input.
	rows, err := db.Query(fmt.Sprintf(`PRAGMA table_info(%s)`, table))
	if err != nil {
		return nil, fmt.Errorf("sqlitelog: inspect %s: %w", table, err)
	}
	defer rows.Close()
	columns := map[string]bool{}
	for rows.Next() {
		var (
			cid, notNull, pk int
			name, typ        string
			dflt             sql.NullString
		)
		if err := rows.Scan(&cid, &name, &typ, &notNull, &dflt, &pk); err != nil {
			return nil, fmt.Errorf("sqlitelog: inspect %s: %w", table, err)
		}
		columns[name] = true
	}
	return columns, rows.Err()
}
