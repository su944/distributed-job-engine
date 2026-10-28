//go:build integration

package storage_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/su944/distributed-job-engine/internal/storage"
)

func TestLedgerConstraintsRejectInvalidWrites(t *testing.T) {
	db := database(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	j, _, err := storage.NewRepository(db, "default").Submit(ctx, submission(t, "constraint-key", body))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, query, code string }{
		{"object_payload", `UPDATE jobs SET payload='[]'::jsonb WHERE id=$1`, "23514"},
		{"negative_attempts", `UPDATE jobs SET attempt_count=-1 WHERE id=$1`, "23514"},
		{"ready_budget", `UPDATE jobs SET attempt_count=max_attempts WHERE id=$1`, "23514"},
		{"stray_owner", `UPDATE jobs SET lease_owner='unexpected' WHERE id=$1`, "23514"},
		{"negative_token", `UPDATE jobs SET lease_token=-1 WHERE id=$1`, "23514"},
		{"terminal_time", `UPDATE jobs SET status='FAILED' WHERE id=$1`, "23514"},
		{"hash_length", `UPDATE jobs SET request_hash=decode('00','hex') WHERE id=$1`, "23514"},
		{"event_object", `UPDATE outbox_events SET payload='[]'::jsonb WHERE aggregateid=$1`, "23514"},
		{"event_type", `UPDATE outbox_events SET type='Other' WHERE aggregateid=$1`, "23514"},
		{"event_foreign_key", `UPDATE outbox_events SET aggregateid='00000000-0000-0000-0000-000000000000' WHERE aggregateid=$1`, "23503"},
		{"retained_job", `DELETE FROM jobs WHERE id=$1`, "23503"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := db.ExecContext(ctx, tc.query, j.ID)
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != tc.code {
				t.Fatalf("expected PostgreSQL %s, got %v", tc.code, err)
			}
		})
	}
	count(t, db, "jobs", 1)
	count(t, db, "outbox_events", 1)
}

func TestSubmissionLookupIndexPlans(t *testing.T) {
	db := database(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Synthetic rows are only plan fixtures, isolated from production submission.
	if _, err := db.ExecContext(ctx, `INSERT INTO jobs(id,namespace,idempotency_key,request_hash,type,payload,max_attempts)
        SELECT md5(i::text)::uuid,'default','plan-'||i,decode(repeat('00',32),'hex'),'invoice.send','{}'::jsonb,5 FROM generate_series(1,1000) i`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `ANALYZE jobs`); err != nil {
		t.Fatal(err)
	}
	var id string
	if err := db.QueryRowContext(ctx, `SELECT id::text FROM jobs WHERE namespace=$1 AND idempotency_key=$2`, "default", "plan-42").Scan(&id); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, query, index string
		args               []any
	}{
		{"idempotency", `SELECT id FROM jobs WHERE namespace=$1 AND idempotency_key=$2`, "jobs_namespace_key_unique", []any{"default", "plan-42"}},
		{"status_read", `SELECT id,status,attempt_count,max_attempts,created_at,updated_at FROM jobs WHERE namespace=$1 AND id=$2`, "jobs_pkey", []any{"default", id}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var raw []byte
			if err := db.QueryRowContext(ctx, `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) `+tc.query, tc.args...).Scan(&raw); err != nil {
				t.Fatal(err)
			}
			var plan any
			if err := json.Unmarshal(raw, &plan); err != nil {
				t.Fatal(err)
			}
			if !planUsesIndex(plan, tc.index) {
				t.Fatalf("plan did not use %s: %s", tc.index, raw)
			}
			t.Logf("representative 1000-row lookup uses %s", tc.index)
		})
	}
}

func planUsesIndex(value any, want string) bool {
	switch x := value.(type) {
	case map[string]any:
		if x["Index Name"] == want {
			return true
		}
		for _, v := range x {
			if planUsesIndex(v, want) {
				return true
			}
		}
	case []any:
		for _, v := range x {
			if planUsesIndex(v, want) {
				return true
			}
		}
	}
	return false
}
