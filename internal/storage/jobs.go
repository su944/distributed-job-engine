// Package storage implements the job ledger using pgx's database/sql adapter.
package storage

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/su944/distributed-job-engine/internal/jobs"
)

type Repository struct {
	db        *sql.DB
	namespace string
}

func NewRepository(db *sql.DB, namespace string) *Repository {
	return &Repository{db: db, namespace: namespace}
}

const columns = `id::text,status,attempt_count,max_attempts,created_at,updated_at`

type scanner interface{ Scan(...any) error }

func scan(s scanner) (jobs.Job, error) {
	var j jobs.Job
	err := s.Scan(&j.ID, &j.Status, &j.Attempts, &j.MaxAttempts, &j.CreatedAt, &j.UpdatedAt)
	j.CreatedAt = j.CreatedAt.UTC()
	j.UpdatedAt = j.UpdatedAt.UTC()
	return j, err
}

func (r *Repository) Get(ctx context.Context, id string) (jobs.Job, error) {
	j, err := scan(r.db.QueryRowContext(ctx, `SELECT `+columns+` FROM jobs WHERE namespace=$1 AND id=$2`, r.namespace, id))
	if errors.Is(err, sql.ErrNoRows) {
		return j, jobs.ErrNotFound
	}
	if err != nil {
		return j, fmt.Errorf("get job: %w", err)
	}
	return j, nil
}

func (r *Repository) Submit(ctx context.Context, s jobs.Submission) (jobs.Job, bool, error) {
	id, err := jobs.UUID()
	if err != nil {
		return jobs.Job{}, false, err
	}
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return jobs.Job{}, false, fmt.Errorf("begin submission: %w", err)
	}
	defer tx.Rollback()
	j, err := scan(tx.QueryRowContext(ctx, `INSERT INTO jobs(id,namespace,idempotency_key,request_hash,type,payload,max_attempts)
        VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(namespace,idempotency_key) DO NOTHING RETURNING `+columns,
		id, r.namespace, s.Key, s.Hash[:], s.Type, []byte(s.Payload), s.MaxAttempts))
	if errors.Is(err, sql.ErrNoRows) {
		// Separate READ COMMITTED statement sees the winning transaction after ON CONFLICT waits.
		var hash []byte
		err := tx.QueryRowContext(ctx, `SELECT `+columns+`,request_hash FROM jobs WHERE namespace=$1 AND idempotency_key=$2`, r.namespace, s.Key).Scan(&j.ID, &j.Status, &j.Attempts, &j.MaxAttempts, &j.CreatedAt, &j.UpdatedAt, &hash)
		if err != nil {
			return jobs.Job{}, false, fmt.Errorf("read existing submission: %w", err)
		}
		if !bytes.Equal(hash, s.Hash[:]) {
			return jobs.Job{}, false, jobs.ErrConflict
		}
		j.CreatedAt = j.CreatedAt.UTC()
		j.UpdatedAt = j.UpdatedAt.UTC()
		return j, false, nil // Deferred rollback releases this read-only transaction.
	}
	if err != nil {
		return jobs.Job{}, false, fmt.Errorf("insert job: %w", err)
	}
	eventID, err := jobs.UUID()
	if err != nil {
		return jobs.Job{}, false, err
	}
	event, err := json.Marshal(struct {
		Version    int       `json:"schema_version"`
		EventID    string    `json:"event_id"`
		JobID      string    `json:"job_id"`
		Kind       string    `json:"kind"`
		OccurredAt time.Time `json:"occurred_at"`
	}{1, eventID, j.ID, "JobReady", j.CreatedAt})
	if err != nil {
		return jobs.Job{}, false, fmt.Errorf("encode event: %w", err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO outbox_events(id,aggregatetype,aggregateid,type,timestamp,payload) VALUES($1,'jobs',$2,'JobReady',$3,$4)`, eventID, j.ID, j.CreatedAt, event); err != nil {
		return jobs.Job{}, false, fmt.Errorf("insert outbox: %w", err)
	}
	if err = tx.Commit(); err != nil {
		return jobs.Job{}, false, fmt.Errorf("%w: %w", jobs.ErrSubmissionUnknown, err)
	}
	return j, true, nil
}
