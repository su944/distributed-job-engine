// Package migrations applies reviewed SQL explicitly; the API never migrates on startup.
package migrations

import (
	"context"
	"crypto/sha256"
	"database/sql"
	_ "embed"
	"fmt"
)

//go:embed 001_submission.up.sql
var submissionSQL string

//go:embed 002_dispatch.up.sql
var dispatchSQL string

// Apply serializes migration runners and detects edits to an already applied migration.
func Apply(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin migration: %w", err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(84217001)`); err != nil {
		return fmt.Errorf("migration lock: %w", err)
	}
	if _, err = tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (version integer PRIMARY KEY, checksum text NOT NULL, applied_at timestamptz NOT NULL DEFAULT clock_timestamp())`); err != nil {
		return fmt.Errorf("migration ledger: %w", err)
	}
	for _, migration := range []struct {
		version int
		sql     string
	}{{1, submissionSQL}, {2, dispatchSQL}} {
		want := fmt.Sprintf("%x", sha256.Sum256([]byte(migration.sql)))
		var checksum string
		err = tx.QueryRowContext(ctx, `SELECT checksum FROM schema_migrations WHERE version=$1`, migration.version).Scan(&checksum)
		switch err {
		case nil:
			if checksum != want {
				return fmt.Errorf("migration %d checksum mismatch", migration.version)
			}
		case sql.ErrNoRows:
			if _, err = tx.ExecContext(ctx, migration.sql); err != nil {
				return fmt.Errorf("apply migration %d: %w", migration.version, err)
			}
			if _, err = tx.ExecContext(ctx, `INSERT INTO schema_migrations(version,checksum) VALUES ($1,$2)`, migration.version, want); err != nil {
				return fmt.Errorf("record migration %d: %w", migration.version, err)
			}
		default:
			return fmt.Errorf("read migration ledger: %w", err)
		}
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit migration: %w", err)
	}
	return nil
}
