//go:build integration

package storage_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/su944/distributed-job-engine/internal/jobs"
	"github.com/su944/distributed-job-engine/internal/storage"
	"github.com/su944/distributed-job-engine/migrations"
)

func ownershipJob(t *testing.T, db *sql.DB) jobs.ClaimRequest {
	t.Helper()
	key, err := jobs.UUID()
	if err != nil {
		t.Fatal(err)
	}
	j, _, err := storage.NewRepository(db, "default").Submit(t.Context(), submission(t, key, body))
	if err != nil {
		t.Fatal(err)
	}
	return jobs.ClaimRequest{JobID: j.ID, Owner: "worker-a", LeaseDuration: 10 * time.Second, AttemptTimeout: time.Minute}
}

func execOwnership(t *testing.T, db interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, query string, args ...any) {
	t.Helper()
	if _, err := db.ExecContext(t.Context(), query, args...); err != nil {
		t.Fatal(err)
	}
}

func mustClaim(t *testing.T, r jobs.JobClaimer, q jobs.ClaimRequest) jobs.Claim {
	t.Helper()
	c, err := r.Claim(t.Context(), q)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func independentOwnershipDB(t *testing.T, db *sql.DB) *sql.DB {
	t.Helper()
	var schema string
	if err := db.QueryRowContext(t.Context(), `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgx.ParseConfig(os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal("invalid TEST_DATABASE_URL")
	}
	cfg.RuntimeParams["search_path"] = schema
	other := stdlib.OpenDB(*cfg)
	t.Cleanup(func() { other.Close() })
	return other
}

func TestOwnershipCompetingInstances(t *testing.T) {
	db := database(t)
	other := independentOwnershipDB(t, db)
	repos := []*storage.Repository{storage.NewRepository(db, "default"), storage.NewRepository(other, "default")}
	for round := 0; round < 5; round++ {
		q := ownershipJob(t, db)
		start := make(chan struct{})
		type result struct {
			c   jobs.Claim
			err error
		}
		results := make(chan result, 24)
		var wg sync.WaitGroup
		for i := 0; i < 24; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				req := q
				req.Owner = fmt.Sprintf("worker-%d", i)
				c, err := repos[i%2].Claim(t.Context(), req)
				results <- result{c, err}
			}(i)
		}
		close(start)
		wg.Wait()
		close(results)
		wins := 0
		for r := range results {
			if r.err == nil {
				wins++
				if r.c.Attempt != 1 || r.c.LeaseToken != 1 || r.c.JobID != q.JobID || r.c.Type != "invoice.send" || !json.Valid(r.c.Payload) {
					t.Fatalf("invalid claim: %+v", r.c)
				}
			} else if !errors.Is(r.err, jobs.ErrNotClaimable) {
				t.Fatal(r.err)
			}
		}
		if wins != 1 {
			t.Fatalf("claims succeeded=%d", wins)
		}
		var n int
		if err := db.QueryRowContext(t.Context(), `SELECT count(*) FROM job_attempts WHERE job_id=$1 AND ended_at IS NULL`, q.JobID).Scan(&n); err != nil || n != 1 {
			t.Fatalf("active attempts %d: %v", n, err)
		}
	}
}

func TestOwnershipEligibilityAndBudget(t *testing.T) {
	db := database(t)
	r := storage.NewRepository(db, "default")
	q := ownershipJob(t, db)
	if _, err := storage.NewRepository(db, "other").Claim(t.Context(), q); !errors.Is(err, jobs.ErrNotFound) {
		t.Fatal(err)
	}
	execOwnership(t, db, `UPDATE jobs SET available_at=clock_timestamp()+interval '1 hour' WHERE id=$1`, q.JobID)
	if _, err := r.Claim(t.Context(), q); !errors.Is(err, jobs.ErrNotClaimable) {
		t.Fatal(err)
	}
	execOwnership(t, db, `UPDATE jobs SET available_at=clock_timestamp(),attempt_count=max_attempts-1,lease_token=41 WHERE id=$1`, q.JobID)
	c := mustClaim(t, r, q)
	if c.Attempt != 5 || c.LeaseToken != 42 {
		t.Fatalf("budget/token %+v", c)
	}
	if _, err := r.Claim(t.Context(), q); !errors.Is(err, jobs.ErrNotClaimable) {
		t.Fatal(err)
	}
	if err := r.Complete(t.Context(), jobs.Completion{Ownership: c.Ownership, Outcome: jobs.CompletionPermanent, ErrorCode: "exhausted"}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Claim(t.Context(), q); !errors.Is(err, jobs.ErrNotClaimable) {
		t.Fatal(err)
	}
	for _, status := range []string{"RETRY_WAIT", "RECONCILE", "SUCCEEDED", "FAILED"} {
		q := ownershipJob(t, db)
		execOwnership(t, db, `UPDATE jobs SET status=$2,finished_at=CASE WHEN $2 IN ('SUCCEEDED','FAILED') THEN clock_timestamp() ELSE NULL END WHERE id=$1`, q.JobID, status)
		if _, err := r.Claim(t.Context(), q); !errors.Is(err, jobs.ErrNotClaimable) {
			t.Fatalf("%s: %v", status, err)
		}
	}
	q = ownershipJob(t, db)
	execOwnership(t, db, `UPDATE jobs SET lease_token=9223372036854775807 WHERE id=$1`, q.JobID)
	if _, err := r.Claim(t.Context(), q); !errors.Is(err, jobs.ErrNotClaimable) {
		t.Fatal(err)
	}
}

func TestOwnershipRenewalAndCompletion(t *testing.T) {
	db := database(t)
	r := storage.NewRepository(db, "default")
	for _, outcome := range []jobs.CompletionOutcome{jobs.CompletionSucceeded, jobs.CompletionPermanent, jobs.CompletionUnknown} {
		q := ownershipJob(t, db)
		q.LeaseDuration = time.Minute
		q.AttemptTimeout = 2 * time.Second
		c := mustClaim(t, r, q)
		if !c.LeaseExpiresAt.Equal(c.Deadline) {
			t.Fatal("initial lease exceeds deadline")
		}
		expires, err := r.Renew(t.Context(), c.Ownership, time.Hour)
		if err != nil || !expires.Equal(c.Deadline) {
			t.Fatalf("renew cap: %v %v", expires, err)
		}
		wrong := c.Ownership
		wrong.Owner = "wrong"
		if _, err := r.Renew(t.Context(), wrong, time.Second); !errors.Is(err, jobs.ErrLeaseLost) {
			t.Fatal(err)
		}
		if err := r.Complete(t.Context(), jobs.Completion{Ownership: wrong, Outcome: outcome}); !errors.Is(err, jobs.ErrLeaseLost) {
			t.Fatal(err)
		}
		completion := jobs.Completion{Ownership: c.Ownership, Outcome: outcome}
		want := jobs.Succeeded
		if outcome == jobs.CompletionSucceeded {
			completion.Result = json.RawMessage(`{"receipt":"ok"}`)
		}
		if outcome == jobs.CompletionPermanent {
			want = jobs.Failed
			completion.ErrorCode = "provider.rejected"
		}
		if outcome == jobs.CompletionUnknown {
			want = jobs.Reconcile
			completion.ErrorCode = "provider.unknown"
		}
		if err := r.Complete(t.Context(), completion); err != nil {
			t.Fatal(err)
		}
		j, err := r.Get(t.Context(), q.JobID)
		if err != nil || j.Status != want || j.Attempts != 1 {
			t.Fatalf("completed job %+v %v", j, err)
		}
		var recorded string
		var token int64
		var cleared bool
		if err := db.QueryRowContext(t.Context(), `SELECT a.outcome,j.lease_token,j.lease_owner IS NULL AND j.lease_expires_at IS NULL AND j.attempt_deadline IS NULL FROM jobs j JOIN job_attempts a ON a.job_id=j.id WHERE j.id=$1`, q.JobID).Scan(&recorded, &token, &cleared); err != nil {
			t.Fatal(err)
		}
		if recorded != string(outcome) || token != c.LeaseToken || !cleared {
			t.Fatal("completion history/ownership differs")
		}
		if err := r.Complete(t.Context(), completion); !errors.Is(err, jobs.ErrLeaseLost) {
			t.Fatal(err)
		}
		if _, err := r.Renew(t.Context(), c.Ownership, time.Second); !errors.Is(err, jobs.ErrLeaseLost) {
			t.Fatal(err)
		}
	}
	q := ownershipJob(t, db)
	c := mustClaim(t, r, q)
	expires, err := r.Renew(t.Context(), c.Ownership, time.Microsecond)
	if err != nil || expires.Before(c.LeaseExpiresAt) {
		t.Fatalf("renew shortened lease: %v %v", expires, err)
	}
	execOwnership(t, db, `UPDATE jobs SET lease_expires_at=clock_timestamp() WHERE id=$1`, q.JobID)
	if _, err := r.Renew(t.Context(), c.Ownership, time.Second); !errors.Is(err, jobs.ErrLeaseLost) {
		t.Fatal(err)
	}
	if err := r.Complete(t.Context(), jobs.Completion{Ownership: c.Ownership, Outcome: jobs.CompletionSucceeded}); !errors.Is(err, jobs.ErrLeaseLost) {
		t.Fatal(err)
	}
	if _, err := r.Claim(t.Context(), q); !errors.Is(err, jobs.ErrNotClaimable) {
		t.Fatal("expired job was reclaimed", err)
	}
}

func TestOwnershipOldTokenCannotFinishNewAttempt(t *testing.T) {
	db := database(t)
	r := storage.NewRepository(db, "default")
	q := ownershipJob(t, db)
	a := mustClaim(t, r, q)
	// Fixture simulates a future recovery transition. No recovery code is added.
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	execOwnership(t, tx, `UPDATE jobs SET status='READY',lease_owner=NULL,lease_expires_at=NULL,attempt_deadline=NULL WHERE id=$1`, q.JobID)
	execOwnership(t, tx, `UPDATE job_attempts SET ended_at=clock_timestamp(),outcome='LEASE_EXPIRED' WHERE job_id=$1`, q.JobID)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	b := mustClaim(t, r, q) // Same owner deliberately isolates the token check.
	if b.LeaseToken <= a.LeaseToken || b.Attempt != 2 {
		t.Fatal("token/attempt did not increase")
	}
	if _, err := r.Renew(t.Context(), a.Ownership, time.Second); !errors.Is(err, jobs.ErrLeaseLost) {
		t.Fatal(err)
	}
	if err := r.Complete(t.Context(), jobs.Completion{Ownership: a.Ownership, Outcome: jobs.CompletionSucceeded}); !errors.Is(err, jobs.ErrLeaseLost) {
		t.Fatal(err)
	}
	if err := r.Complete(t.Context(), jobs.Completion{Ownership: b.Ownership, Outcome: jobs.CompletionSucceeded}); err != nil {
		t.Fatal(err)
	}
}

func TestOwnershipHistoryFailureRollsBack(t *testing.T) {
	db := database(t)
	r := storage.NewRepository(db, "default")
	q := ownershipJob(t, db)
	execOwnership(t, db, `CREATE FUNCTION fail_attempt() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected history failure'; END $$;
        CREATE TRIGGER fail_history BEFORE INSERT OR UPDATE ON job_attempts FOR EACH ROW EXECUTE FUNCTION fail_attempt()`)
	if _, err := r.Claim(t.Context(), q); err == nil {
		t.Fatal("claim history failure ignored")
	}
	j, err := r.Get(t.Context(), q.JobID)
	if err != nil || j.Status != jobs.Ready || j.Attempts != 0 {
		t.Fatalf("partial claim %+v %v", j, err)
	}
	var token int64
	if err := db.QueryRowContext(t.Context(), `SELECT lease_token FROM jobs WHERE id=$1`, q.JobID).Scan(&token); err != nil || token != 0 {
		t.Fatalf("token consumed: %d %v", token, err)
	}
	count(t, db, "job_attempts", 0)
	execOwnership(t, db, `DROP TRIGGER fail_history ON job_attempts`)
	c := mustClaim(t, r, q)
	execOwnership(t, db, `CREATE TRIGGER fail_history BEFORE UPDATE ON job_attempts FOR EACH ROW EXECUTE FUNCTION fail_attempt()`)
	if err := r.Complete(t.Context(), jobs.Completion{Ownership: c.Ownership, Outcome: jobs.CompletionSucceeded}); err == nil {
		t.Fatal("completion history failure ignored")
	}
	j, err = r.Get(t.Context(), q.JobID)
	if err != nil || j.Status != jobs.Running {
		t.Fatalf("partial completion %+v %v", j, err)
	}
	execOwnership(t, db, `DROP TRIGGER fail_history ON job_attempts`)
	if err := r.Complete(t.Context(), jobs.Completion{Ownership: c.Ownership, Outcome: jobs.CompletionSucceeded}); err != nil {
		t.Fatal(err)
	}
	if err := migrations.Apply(t.Context(), db); err != nil {
		t.Fatal("migration replay", err)
	}
}

func lockJobForTest(t *testing.T, db *sql.DB, id string) (*sql.Tx, int) {
	t.Helper()
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tx.Rollback() })
	var pid int
	if err := tx.QueryRowContext(t.Context(), `SELECT pg_backend_pid() FROM jobs WHERE id=$1 FOR UPDATE`, id).Scan(&pid); err != nil {
		t.Fatal(err)
	}
	return tx, pid
}

func awaitOwnershipBlock(t *testing.T, db *sql.DB, pid int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	for {
		var blocked bool
		if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))`, pid).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("ownership lock wait not observed")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestOwnershipTimeAfterLockWait(t *testing.T) {
	db := database(t)
	r := storage.NewRepository(db, "default")
	t.Run("claim_due_and_lease_after_wait", func(t *testing.T) {
		q := ownershipJob(t, db)
		q.LeaseDuration = time.Second
		tx, pid := lockJobForTest(t, db, q.JobID)
		execOwnership(t, tx, `UPDATE jobs SET available_at=clock_timestamp()+interval '100 milliseconds' WHERE id=$1`, q.JobID)
		type result struct {
			c   jobs.Claim
			err error
		}
		done := make(chan result, 1)
		go func() { c, err := r.Claim(t.Context(), q); done <- result{c, err} }()
		awaitOwnershipBlock(t, db, pid)
		execOwnership(t, tx, `SELECT pg_sleep(0.15)`)
		var release time.Time
		if err := tx.QueryRowContext(t.Context(), `SELECT clock_timestamp()`).Scan(&release); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		resultValue := <-done
		if resultValue.err != nil {
			t.Fatal(resultValue.err)
		}
		if resultValue.c.LeaseExpiresAt.Before(release.Add(q.LeaseDuration)) {
			t.Fatal("lease used pre-lock time")
		}
	})
	for _, operation := range []string{"renew", "complete"} {
		t.Run(operation+"_expired_during_wait", func(t *testing.T) {
			q := ownershipJob(t, db)
			c := mustClaim(t, r, q)
			tx, pid := lockJobForTest(t, db, q.JobID)
			execOwnership(t, tx, `UPDATE jobs SET lease_expires_at=clock_timestamp()+interval '100 milliseconds' WHERE id=$1`, q.JobID)
			done := make(chan error, 1)
			go func() {
				if operation == "renew" {
					_, err := r.Renew(t.Context(), c.Ownership, time.Second)
					done <- err
				} else {
					done <- r.Complete(t.Context(), jobs.Completion{Ownership: c.Ownership, Outcome: jobs.CompletionSucceeded})
				}
			}()
			awaitOwnershipBlock(t, db, pid)
			execOwnership(t, tx, `SELECT pg_sleep(0.15)`)
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			if err := <-done; !errors.Is(err, jobs.ErrLeaseLost) {
				t.Fatalf("expired while locked: %v", err)
			}
		})
	}
}

func TestOwnershipCancellationAndTimeouts(t *testing.T) {
	db := database(t)
	r := storage.NewRepository(db, "default")
	q := ownershipJob(t, db)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := r.Claim(ctx, q); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	tx, pid := lockJobForTest(t, db, q.JobID)
	ctx, cancel = context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { _, err := r.Claim(ctx, q); done <- err }()
	awaitOwnershipBlock(t, db, pid)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled lock waiter did not return")
	}
	// A caller without a deadline is still bounded by the server lock_timeout.
	start := time.Now()
	_, err := r.Claim(t.Context(), q)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
		t.Fatalf("lock timeout: %v", err)
	}
	if time.Since(start) > 3*time.Second {
		t.Fatal("lock timeout exceeded budget")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	held, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel = context.WithTimeout(t.Context(), 25*time.Millisecond)
	_, err = r.Claim(ctx, q)
	cancel()
	held.Close()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("pool timeout: %v", err)
	}
	c := mustClaim(t, r, q)
	if _, err := r.Renew(t.Context(), c.Ownership, time.Second); err != nil {
		t.Fatal(err)
	}
	if err := r.Complete(t.Context(), jobs.Completion{Ownership: c.Ownership, Outcome: jobs.CompletionSucceeded}); err != nil {
		t.Fatal(err)
	}
	if stats := db.Stats(); stats.InUse != 0 {
		t.Fatalf("connection leak: %+v", stats)
	}
}

func TestOwnershipUncertainClaimGrantsNoPermission(t *testing.T) {
	for _, before := range []bool{true, false} {
		t.Run(fmt.Sprintf("before_commit_%t", before), func(t *testing.T) {
			db := database(t)
			q := ownershipJob(t, db)
			fault := &commitFault{before: before}
			broken := faultDatabase(t, db, fault)
			c, err := storage.NewRepository(broken, "default").Claim(t.Context(), q)
			if !errors.Is(err, jobs.ErrOwnershipUnknown) || c.JobID != "" || !fault.fired.Load() {
				t.Fatalf("claim granted on uncertain commit: %+v %v", c, err)
			}
			want := jobs.Running
			attempts := 1
			if before {
				want = jobs.Ready
				attempts = 0
			}
			j, err := storage.NewRepository(db, "default").Get(t.Context(), q.JobID)
			if err != nil || j.Status != want || j.Attempts != attempts {
				t.Fatalf("persisted outcome %+v %v", j, err)
			}
			count(t, db, "job_attempts", attempts)
			if err := broken.PingContext(t.Context()); err != nil {
				t.Fatal("faulted pool failed to recover", err)
			}
		})
	}
}

func TestOwnershipUncertainRenewalAndCompletion(t *testing.T) {
	for _, operation := range []string{"renew", "complete"} {
		for _, before := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s_before_commit_%t", operation, before), func(t *testing.T) {
				db := database(t)
				q := ownershipJob(t, db)
				c := mustClaim(t, storage.NewRepository(db, "default"), q)
				fault := &commitFault{before: before}
				broken := faultDatabase(t, db, fault)
				r := storage.NewRepository(broken, "default")
				var err error
				if operation == "renew" {
					var expires time.Time
					expires, err = r.Renew(t.Context(), c.Ownership, time.Minute)
					if !expires.IsZero() {
						t.Fatal("uncertain renewal returned confirmed expiry")
					}
				} else {
					err = r.Complete(t.Context(), jobs.Completion{Ownership: c.Ownership, Outcome: jobs.CompletionSucceeded})
				}
				if !errors.Is(err, jobs.ErrOwnershipUnknown) || !fault.fired.Load() {
					t.Fatalf("missing uncertain outcome: %v", err)
				}
				var status jobs.Status
				var expires sql.NullTime
				var ended sql.NullTime
				if err := db.QueryRowContext(t.Context(), `SELECT j.status,j.lease_expires_at,a.ended_at FROM jobs j JOIN job_attempts a ON a.job_id=j.id WHERE j.id=$1`, q.JobID).Scan(&status, &expires, &ended); err != nil {
					t.Fatal(err)
				}
				if operation == "complete" && !before {
					if status != jobs.Succeeded || expires.Valid || !ended.Valid {
						t.Fatal("completion did not persist atomically")
					}
				} else {
					if status != jobs.Running || !expires.Valid || ended.Valid {
						t.Fatal("unexpected persisted ownership")
					}
					if operation == "renew" && !before && !expires.Time.Equal(c.Deadline) {
						t.Fatal("renewal not persisted")
					}
					if before && !expires.Time.Equal(c.LeaseExpiresAt) {
						t.Fatal("uncommitted renewal changed expiry")
					}
				}
				if err := broken.PingContext(t.Context()); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestOwnershipMigrationAndHistoryGuards(t *testing.T) {
	db := database(t)
	r := storage.NewRepository(db, "default")
	q := ownershipJob(t, db)
	c := mustClaim(t, r, q)
	if _, err := storage.NewRepository(db, "other").Renew(t.Context(), c.Ownership, time.Second); !errors.Is(err, jobs.ErrLeaseLost) {
		t.Fatal(err)
	}
	if err := storage.NewRepository(db, "other").Complete(t.Context(), jobs.Completion{Ownership: c.Ownership, Outcome: jobs.CompletionSucceeded}); !errors.Is(err, jobs.ErrLeaseLost) {
		t.Fatal(err)
	}
	_, err := db.ExecContext(t.Context(), `INSERT INTO job_attempts(job_id,attempt_no,lease_token,worker_id,started_at,deadline_at) SELECT job_id,2,2,worker_id,started_at,deadline_at FROM job_attempts WHERE job_id=$1`, q.JobID)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
		t.Fatalf("multiple active attempts allowed: %v", err)
	}
	_, err = db.ExecContext(t.Context(), `UPDATE job_attempts SET ended_at=clock_timestamp() WHERE job_id=$1`, q.JobID)
	if !errors.As(err, &pgErr) || pgErr.Code != "23514" {
		t.Fatalf("partial history completion allowed: %v", err)
	}
	execOwnership(t, db, `UPDATE schema_migrations SET checksum='changed' WHERE version=3`)
	if err := migrations.Apply(t.Context(), db); err == nil {
		t.Fatal("ownership checksum drift ignored")
	}
	// Missing history is treated as an integrity failure, never a completed job.
	execOwnership(t, db, `DELETE FROM job_attempts WHERE job_id=$1`, q.JobID)
	if err := r.Complete(t.Context(), jobs.Completion{Ownership: c.Ownership, Outcome: jobs.CompletionSucceeded}); err == nil {
		t.Fatal("missing history ignored")
	}
	j, err := r.Get(t.Context(), q.JobID)
	if err != nil || j.Status != jobs.Running {
		t.Fatalf("history mismatch partially completed job: %+v %v", j, err)
	}
}
