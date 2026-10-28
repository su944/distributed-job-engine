//go:build integration

package storage_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/su944/distributed-job-engine/internal/jobs"
)

func TestConcurrentHTTPLogicalRequests(t *testing.T) {
	for _, tc := range []struct {
		name   string
		bodies []string
	}{
		{"equivalent", []string{body, `{"payload":{"amount":1.00,"invoice_id":"inv_001"},"type":"invoice.send","max_attempts":5}`, `{"type":"invoice.send","payload":{"invoice_id":"inv_001","amount":10e-1}}`}},
		{"conflicting", []string{body, `{"type":"invoice.send","payload":{"invoice_id":"inv_002","amount":1}}`, `{"type":"invoice.send","payload":{"invoice_id":"inv_001","amount":1},"max_attempts":6}`}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := database(t)
			app := integrationAPI(db)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			hashes := make([][32]byte, len(tc.bodies))
			for i, b := range tc.bodies {
				hashes[i] = submission(t, "race-key", b).Hash
			}
			type result struct {
				response *httptest.ResponseRecorder
				hash     [32]byte
			}
			results := make(chan result, 24)
			start := make(chan struct{})
			var wg sync.WaitGroup
			for i := range 24 {
				wg.Go(func() {
					<-start
					index := i % len(tc.bodies)
					results <- result{postJob(ctx, app, "race-key", tc.bodies[index]), hashes[index]}
				})
			}
			close(start)
			wg.Wait()
			close(results)
			var winningID string
			var winningHash []byte
			if err := db.QueryRowContext(ctx, `SELECT id::text,request_hash FROM jobs WHERE namespace=$1 AND idempotency_key=$2`, "default", "race-key").Scan(&winningID, &winningHash); err != nil {
				t.Fatal(err)
			}
			created, conflicts := 0, 0
			for result := range results {
				w := result.response
				if !bytes.Equal(result.hash[:], winningHash) {
					expectCode(t, w, 409, "idempotency_conflict")
					conflicts++
					continue
				}
				if w.Code == 202 {
					created++
				} else if w.Code != 200 {
					t.Fatalf("equivalent request: %d %s", w.Code, w.Body)
				}
				var j jobs.Job
				if err := json.Unmarshal(w.Body.Bytes(), &j); err != nil {
					t.Fatal(err)
				}
				if j.ID != winningID || w.Header().Get("Location") != "/v1/jobs/"+winningID {
					t.Fatal("idempotent response changed identity")
				}
			}
			if created != 1 {
				t.Fatalf("created %d jobs", created)
			}
			if tc.name == "conflicting" && conflicts != 16 {
				t.Fatalf("got %d conflicting requests, want 16", conflicts)
			}
			if tc.name == "equivalent" && conflicts != 0 {
				t.Fatal("equivalent request conflicted")
			}
			count(t, db, "jobs", 1)
			count(t, db, "outbox_events", 1)
			var eventID, payloadEventID, payloadJobID, kind string
			if err := db.QueryRowContext(ctx, `SELECT id::text,payload->>'event_id',payload->>'job_id',payload->>'kind' FROM outbox_events WHERE aggregateid=$1`, winningID).Scan(&eventID, &payloadEventID, &payloadJobID, &kind); err != nil {
				t.Fatal(err)
			}
			if eventID != payloadEventID || payloadJobID != winningID || kind != "JobReady" {
				t.Fatal("outbox envelope does not identify the committed job and event")
			}
		})
	}
}

func TestCancellationWhileWaitingForUniqueKey(t *testing.T) {
	db := database(t)
	app := integrationAPI(db)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	owner, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Rollback()
	id, err := jobs.UUID()
	if err != nil {
		t.Fatal(err)
	}
	s := submission(t, "blocked-key", body)
	if _, err := owner.ExecContext(ctx, `INSERT INTO jobs(id,namespace,idempotency_key,request_hash,type,payload,max_attempts) VALUES($1,'default',$2,$3,$4,$5,$6)`, id, s.Key, s.Hash[:], s.Type, []byte(s.Payload), s.MaxAttempts); err != nil {
		t.Fatal(err)
	}
	var ownerPID int
	if err := owner.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&ownerPID); err != nil {
		t.Fatal(err)
	}
	waitCtx, stopWait := context.WithCancel(ctx)
	defer stopWait()
	result := make(chan *httptest.ResponseRecorder, 1)
	go func() { result <- postJob(waitCtx, app, s.Key, body) }()
	// Observe the actual PostgreSQL lock wait instead of assuming a sleep is enough.
	observed := false
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for !observed {
		if err := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid)))`, ownerPID).Scan(&observed); err != nil {
			t.Fatal(err)
		}
		if !observed {
			select {
			case <-ticker.C:
			case <-ctx.Done():
				t.Fatal("submission never waited on the unique key")
			}
		}
	}
	stopWait()
	select {
	case w := <-result:
		expectCode(t, w, 503, "request_canceled")
	case <-ctx.Done():
		t.Fatal("canceled SQL wait did not return")
	}
	if err := owner.Rollback(); err != nil {
		t.Fatal(err)
	}
	count(t, db, "jobs", 0)
	count(t, db, "outbox_events", 0)
	if w := postJob(ctx, app, s.Key, body); w.Code != 202 {
		t.Fatalf("retry after cancellation: %d %s", w.Code, w.Body)
	}
	count(t, db, "jobs", 1)
	count(t, db, "outbox_events", 1)
	if stats := db.Stats(); stats.InUse != 0 {
		t.Fatalf("canceled transaction leaked: %+v", stats)
	}
}

func TestPoolExhaustionDeadlineAndTransactionRelease(t *testing.T) {
	db := database(t)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	app := integrationAPI(db)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	held, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	deadlineCtx, stop := context.WithTimeout(ctx, 25*time.Millisecond)
	defer stop()
	expectCode(t, postJob(deadlineCtx, app, "pool-key", body), 503, "request_timeout")
	if stats := db.Stats(); stats.MaxOpenConnections != 1 || stats.OpenConnections != 1 || stats.InUse != 1 || stats.WaitCount < 1 {
		t.Fatalf("unexpected bounded pool: %+v", stats)
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	count(t, db, "jobs", 0)
	count(t, db, "outbox_events", 0)
	if w := postJob(ctx, app, "pool-key", body); w.Code != 202 {
		t.Fatalf("pool failed to recover: %d %s", w.Code, w.Body)
	}
	for range 5 {
		if w := postJob(ctx, app, "pool-key", body); w.Code != 200 {
			t.Fatalf("replay leaked transaction: %d %s", w.Code, w.Body)
		}
		expectCode(t, postJob(ctx, app, "pool-key", `{"type":"invoice.send","payload":{"changed":true}}`), 409, "idempotency_conflict")
	}
	count(t, db, "jobs", 1)
	count(t, db, "outbox_events", 1)
	if stats := db.Stats(); stats.InUse != 0 || stats.OpenConnections > 1 {
		t.Fatalf("pool/transaction leak: %+v", stats)
	}
}
