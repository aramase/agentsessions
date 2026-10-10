// Package approvalfixture contains only the substrate approval test harness and executor.
package approvalfixture

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	_ "modernc.org/sqlite"
)

// Effects journals the fixture's effect and dedup receipt in one SQLite statement. This is a test
// effect, not an atomicity claim about arbitrary external tools.
type Effects struct{ db *sql.DB }

func OpenEffects(path string) (*Effects, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS effects (session TEXT NOT NULL, key TEXT NOT NULL, receipt BLOB NOT NULL, PRIMARY KEY(session,key))`); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Effects{db: db}, nil
}

func (e *Effects) Close() error { return e.db.Close() }

func (e *Effects) Execute(ctx context.Context, scope controller.ToolCallContext, call api.ToolCall) (api.ToolResult, error) {
	if scope.SessionUID == "" || call.IdempotencyKey == "" {
		return api.ToolResult{}, errors.New("fixture effect requires session and key")
	}
	receipt := api.ToolResult{Output: map[string]any{"session": scope.SessionUID, "key": call.IdempotencyKey, "args": call.Args}}
	b, err := json.Marshal(receipt)
	if err != nil {
		return api.ToolResult{}, err
	}
	if _, err := e.db.ExecContext(ctx, `INSERT INTO effects(session,key,receipt) VALUES(?,?,?) ON CONFLICT(session,key) DO NOTHING`, scope.SessionUID, call.IdempotencyKey, b); err != nil {
		return api.ToolResult{}, err
	}
	if err := e.db.QueryRowContext(ctx, `SELECT receipt FROM effects WHERE session=? AND key=?`, scope.SessionUID, call.IdempotencyKey).Scan(&b); err != nil {
		return api.ToolResult{}, err
	}
	err = json.Unmarshal(b, &receipt)
	return receipt, err
}

func (e *Effects) Count(uid string) (int, error) {
	var count int
	err := e.db.QueryRow(`SELECT count(*) FROM effects WHERE session=?`, uid).Scan(&count)
	return count, err
}
