package storage

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/su944/distributed-job-engine/internal/config"
)

func Open(ctx context.Context, c config.Config) (*sql.DB, error) {
	parsed, err := pgx.ParseConfig(c.DatabaseURL)
	if err != nil {
		return nil, errors.New("invalid DATABASE_URL")
	} // Parse errors may contain credentials.
	parsed.ConnectTimeout = c.DBTimeout
	db := stdlib.OpenDB(*parsed)
	db.SetMaxOpenConns(c.DBMaxOpen)
	db.SetMaxIdleConns(c.DBMaxIdle)
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)
	pingCtx, cancel := context.WithTimeout(ctx, c.DBTimeout)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		return nil, errors.New("database connection unavailable")
	}
	return db, nil
}

// Ready checks the migration ledger and submission tables without scanning job data.
func Ready(ctx context.Context, db *sql.DB) error {
	var version int
	return db.QueryRowContext(ctx, `SELECT version FROM schema_migrations WHERE version=1
        AND NOT EXISTS (SELECT id FROM jobs WHERE false)
        AND NOT EXISTS (SELECT id FROM outbox_events WHERE false)`).Scan(&version)
}
