//go:build integration

package worker_test

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/su944/distributed-job-engine/internal/dispatch"
	"github.com/su944/distributed-job-engine/internal/jobs"
	"github.com/su944/distributed-job-engine/internal/storage"
	"github.com/su944/distributed-job-engine/internal/worker"
	"github.com/su944/distributed-job-engine/migrations"
)

type fixture struct {
	db, hb *sql.DB
	repo   *storage.Repository
}

func database(t *testing.T) fixture {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("TEST_DATABASE_URL required; real PostgreSQL is never skipped")
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal("invalid TEST_DATABASE_URL")
	}
	admin := stdlib.OpenDB(*cfg)
	id, err := jobs.UUID()
	if err != nil {
		t.Fatal(err)
	}
	schema := "worker_test_" + strings.ReplaceAll(id, "-", "")
	if _, err := admin.ExecContext(t.Context(), `CREATE SCHEMA "`+schema+`"`); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := admin.ExecContext(ctx, `DROP SCHEMA "`+schema+`" CASCADE`); err != nil {
			t.Error(err)
		}
		admin.Close()
	})
	cfg.RuntimeParams["search_path"] = schema
	db := stdlib.OpenDB(*cfg)
	db.SetMaxOpenConns(6)
	db.SetMaxIdleConns(6)
	hb := stdlib.OpenDB(*cfg)
	hb.SetMaxOpenConns(2)
	hb.SetMaxIdleConns(2)
	t.Cleanup(func() { db.Close(); hb.Close() })
	if err := migrations.Apply(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	return fixture{db, hb, storage.NewRepository(db, "default")}
}

func config(id string, capacity int) worker.Config {
	return worker.Config{ID: id, Capacity: capacity, PollInterval: 10 * time.Millisecond, LeaseDuration: 2 * time.Second, HeartbeatInterval: 100 * time.Millisecond, AttemptTimeout: 6 * time.Second, IOTimeout: 500 * time.Millisecond, DrainTimeout: time.Second}
}

func submit(t *testing.T, f fixture, typ string, receipt bool, max int) string {
	t.Helper()
	key, err := jobs.UUID()
	if err != nil {
		t.Fatal(err)
	}
	body := fmt.Sprintf(`{"type":%q,"payload":{"message":"test-secret"},"max_attempts":%d}`, typ, max)
	s, err := jobs.ParseSubmission(key, []byte(body), map[string]bool{typ: true})
	if err != nil {
		t.Fatal(err)
	}
	j, _, err := f.repo.Submit(t.Context(), s)
	if err != nil {
		t.Fatal(err)
	}
	if receipt {
		receive(t, f, j.ID)
	}
	return j.ID
}

func receive(t *testing.T, f fixture, id string) {
	t.Helper()
	var payload []byte
	if err := f.db.QueryRowContext(t.Context(), `SELECT payload FROM outbox_events WHERE aggregateid=$1 ORDER BY timestamp LIMIT 1`, id).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	store := dispatch.NewSQLStore(f.db, "worker-test", "worker-test")
	r := dispatch.Record{Topic: dispatch.Topic, Key: []byte(id), Value: payload}
	for i := 0; i < 2; i++ {
		outcome, err := store.Handle(t.Context(), r)
		if err != nil {
			t.Fatal(err)
		}
		if (i == 0 && outcome != dispatch.Received && outcome != dispatch.Duplicate) || (i == 1 && outcome != dispatch.Duplicate) {
			t.Fatalf("receipt %s", outcome)
		}
	}
}

func start(t *testing.T, f fixture, c worker.Config, registry worker.Registry) (*worker.Pool, context.CancelFunc, <-chan error) {
	t.Helper()
	p, err := worker.New(c, f.repo, storage.NewRepository(f.hb, "default"), registry, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	t.Cleanup(cancel)
	return p, cancel, done
}

func wait(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func stopped(t *testing.T, done <-chan error, wantError bool) {
	t.Helper()
	select {
	case err := <-done:
		if (err != nil) != wantError {
			t.Fatalf("worker exit %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("worker failed to stop")
	}
}

func status(t *testing.T, f fixture, id string) jobs.Job {
	t.Helper()
	j, err := f.repo.Get(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func TestDurableReceiptInboxSurvivesDispatcherAndWorkerRestart(t *testing.T) {
	f := database(t)
	id := submit(t, f, "demo.echo", false, 3)
	_, stop, done := start(t, f, config("before-receipt", 1), worker.Registry{"demo.echo": {Handler: worker.Echo{}, ReplaySafe: true}})
	// No receipt: no claim, even though the API's durable job exists.
	time.Sleep(60 * time.Millisecond)
	stop()
	stopped(t, done, false)
	if j := status(t, f, id); j.Status != jobs.Ready || j.Attempts != 0 {
		t.Fatal("job without receipt was executed")
	}
	receive(t, f, id) // durable boundary; no in-memory handoff survives this point
	_, stop, done = start(t, f, config("after-restart", 1), worker.Registry{"demo.echo": {Handler: worker.Echo{}, ReplaySafe: true}})
	wait(t, func() bool { return status(t, f, id).Status == jobs.Succeeded })
	stop()
	stopped(t, done, false)
	receive(t, f, id) // duplicate receipt after execution must not replay the handler
	_, stop, done = start(t, f, config("second-restart", 1), worker.Registry{"demo.echo": {Handler: worker.Echo{}, ReplaySafe: true}})
	time.Sleep(60 * time.Millisecond)
	stop()
	stopped(t, done, false)
	if j := status(t, f, id); j.Attempts != 1 {
		t.Fatalf("duplicate execution %+v", j)
	}
	unsupported := submit(t, f, "invoice.send", true, 3)
	_, stop, done = start(t, f, config("registered-only", 1), worker.Registry{"demo.echo": {Handler: worker.Echo{}, ReplaySafe: true}})
	time.Sleep(60 * time.Millisecond)
	stop()
	stopped(t, done, false)
	if j := status(t, f, unsupported); j.Status != jobs.Ready || j.Attempts != 0 {
		t.Fatal("unregistered handler claimed")
	}
}

func TestTwoWorkerPoolsBoundCapacityAndCompete(t *testing.T) {
	f := database(t)
	var active, maxActive, calls atomic.Int64
	var mu sync.Mutex
	seen := map[string]int{}
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	h := worker.HandlerFunc(func(ctx context.Context, c jobs.Claim) worker.Result {
		mu.Lock()
		seen[c.JobID]++
		mu.Unlock()
		calls.Add(1)
		n := active.Add(1)
		defer active.Add(-1)
		for old := maxActive.Load(); n > old && !maxActive.CompareAndSwap(old, n); old = maxActive.Load() {
		}
		select {
		case <-release:
			return worker.Result{Class: worker.Success}
		case <-ctx.Done():
			return worker.Result{Class: worker.UnknownOutcome}
		}
	})
	ids := make([]string, 12)
	for i := range ids {
		ids[i] = submit(t, f, "demo.echo", true, 3)
	}
	registry := worker.Registry{"demo.echo": {Handler: h, ReplaySafe: true}}
	p1, stop1, done1 := start(t, f, config("instance-a", 2), registry)
	p2, stop2, done2 := start(t, f, config("instance-b", 2), registry)
	wait(t, func() bool { return active.Load() == 4 })
	if calls.Load() != 4 || maxActive.Load() > 4 {
		t.Fatal("slots did not bound claims/executions")
	}
	if err := p1.Ready(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := p2.Ready(t.Context()); err != nil {
		t.Fatal(err)
	}
	releaseOnce.Do(func() { close(release) })
	wait(t, func() bool {
		for _, id := range ids {
			if status(t, f, id).Status != jobs.Succeeded {
				return false
			}
		}
		return true
	})
	stop1()
	stop2()
	stopped(t, done1, false)
	stopped(t, done2, false)
	mu.Lock()
	defer mu.Unlock()
	if calls.Load() != 12 || maxActive.Load() > 4 {
		t.Fatalf("calls=%d max=%d", calls.Load(), maxActive.Load())
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("duplicate handler %s: %d", id, n)
		}
	}
	for _, id := range ids {
		if status(t, f, id).Attempts != 1 {
			t.Fatal("concurrent duplicate ownership")
		}
	}
}

func TestWorkerClassifiedOutcomes(t *testing.T) {
	f := database(t)
	cases := []struct {
		typ     string
		class   worker.Classification
		safe    bool
		max     int
		want    jobs.Status
		history string
	}{
		{"demo.success", worker.Success, true, 3, jobs.Succeeded, "SUCCEEDED"},
		{"demo.permanent", worker.PermanentFailure, false, 3, jobs.Failed, "PERMANENT"},
		{"demo.unknown", worker.UnknownOutcome, true, 1, jobs.Reconcile, "UNKNOWN"},
		{"demo.retry", worker.RetryableFailure, true, 3, jobs.RetryWait, "RETRYABLE"},
		{"demo.exhausted", worker.RetryableFailure, true, 1, jobs.Failed, "RETRYABLE"},
		{"demo.unsafe", worker.RetryableFailure, false, 3, jobs.Reconcile, "UNKNOWN"},
	}
	registry := worker.Registry{}
	ids := make([]string, len(cases))
	for i, tc := range cases {
		class := tc.class
		registry[tc.typ] = worker.Registration{ReplaySafe: tc.safe, Handler: worker.HandlerFunc(func(context.Context, jobs.Claim) worker.Result {
			if class == worker.Success {
				return worker.Result{Class: class, Value: json.RawMessage(`{"ok":true}`)}
			}
			return worker.Result{Class: class, Code: "demo.result"}
		})}
		ids[i] = submit(t, f, tc.typ, true, tc.max)
	}
	_, stop, done := start(t, f, config("outcomes", 2), registry)
	wait(t, func() bool {
		for i, tc := range cases {
			if status(t, f, ids[i]).Status != tc.want {
				return false
			}
		}
		return true
	})
	stop()
	stopped(t, done, false)
	for i, tc := range cases {
		var outcome string
		if err := f.db.QueryRowContext(t.Context(), `SELECT outcome FROM job_attempts WHERE job_id=$1`, ids[i]).Scan(&outcome); err != nil || outcome != tc.history {
			t.Fatalf("%s history=%s %v", tc.typ, outcome, err)
		}
	}
	// No scheduler is present: RETRY_WAIT remains unchanged after another drain.
	_, stop, done = start(t, f, config("no-retry-scheduler", 1), registry)
	time.Sleep(50 * time.Millisecond)
	stop()
	stopped(t, done, false)
	if j := status(t, f, ids[3]); j.Status != jobs.RetryWait || j.Attempts != 1 {
		t.Fatal("retry ran automatically")
	}
}

func TestHeartbeatAndExecutionOutsideTransactions(t *testing.T) {
	f := database(t)
	id := submit(t, f, "demo.echo", true, 3)
	errorsCh := make(chan error, 1)
	h := worker.HandlerFunc(func(ctx context.Context, c jobs.Claim) worker.Result {
		// A separate connection must lock the job immediately. If the claim's
		// transaction were held across execution this would fail with NOWAIT.
		tx, err := f.hb.BeginTx(ctx, nil)
		if err == nil {
			var locked string
			err = tx.QueryRowContext(ctx, `SELECT id::text FROM jobs WHERE id=$1 FOR UPDATE NOWAIT`, c.JobID).Scan(&locked)
			tx.Rollback()
		}
		if err != nil {
			errorsCh <- err
			return worker.Result{Class: worker.UnknownOutcome}
		}
		select {
		case <-time.After(1300 * time.Millisecond):
			return worker.Result{Class: worker.Success}
		case <-ctx.Done():
			errorsCh <- ctx.Err()
			return worker.Result{Class: worker.UnknownOutcome}
		}
	})
	c := config("heartbeats", 1)
	c.LeaseDuration = time.Second
	_, stop, done := start(t, f, c, worker.Registry{"demo.echo": {Handler: h, ReplaySafe: true}})
	wait(t, func() bool { return status(t, f, id).Status == jobs.Succeeded })
	stop()
	stopped(t, done, false)
	select {
	case err := <-errorsCh:
		t.Fatal("execution retained SQL lock or lease failed", err)
	default:
	}
}

func TestGracefulDrainStopsClaimsAndRenewsActiveWork(t *testing.T) {
	f := database(t)
	id := submit(t, f, "demo.echo", true, 3)
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	h := worker.HandlerFunc(func(ctx context.Context, _ jobs.Claim) worker.Result {
		close(started)
		select {
		case <-release:
			return worker.Result{Class: worker.Success}
		case <-ctx.Done():
			return worker.Result{Class: worker.UnknownOutcome}
		}
	})
	p, stop, done := start(t, f, config("drain", 1), worker.Registry{"demo.echo": {Handler: h}})
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("handler never started")
	}
	queued := submit(t, f, "demo.echo", true, 3)
	stop()
	wait(t, func() bool { return p.Ready(t.Context()) != nil })
	time.Sleep(150 * time.Millisecond) // active execution keeps its context while draining
	once.Do(func() { close(release) })
	stopped(t, done, false)
	if status(t, f, id).Status != jobs.Succeeded || status(t, f, queued).Attempts != 0 {
		t.Fatal("drain canceled active work or claimed queued job")
	}
}

func TestForcedDrainNeverReleasesLiveOwnership(t *testing.T) {
	f := database(t)
	id := submit(t, f, "demo.echo", true, 3)
	started := make(chan struct{})
	release := make(chan struct{})
	returned := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	h := worker.HandlerFunc(func(context.Context, jobs.Claim) worker.Result {
		close(started)
		<-release
		close(returned)
		return worker.Result{Class: worker.Success}
	})
	c := config("forced-drain", 1)
	c.DrainTimeout = 150 * time.Millisecond
	_, stop, done := start(t, f, c, worker.Registry{"demo.echo": {Handler: h}})
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("handler never started")
	}
	stop()
	select {
	case err := <-done:
		if !errors.Is(err, worker.ErrDrainTimeout) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("unbounded shutdown")
	}
	if j := status(t, f, id); j.Status != jobs.Running || j.Attempts != 1 {
		t.Fatal("released ownership while handler running")
	}
	once.Do(func() { close(release) })
	<-returned
	// Allow the fixed slot to unwind; it must not complete after cancellation.
	time.Sleep(40 * time.Millisecond)
	if status(t, f, id).Status != jobs.Running {
		t.Fatal("late handler completion was accepted")
	}
}

func TestLeaseLossAndDatabaseFailureCancelHandlers(t *testing.T) {
	for _, failure := range []string{"expired", "database"} {
		t.Run(failure, func(t *testing.T) {
			f := database(t)
			id := submit(t, f, "demo.echo", true, 3)
			started := make(chan struct{})
			canceled := make(chan struct{})
			h := worker.HandlerFunc(func(ctx context.Context, _ jobs.Claim) worker.Result {
				close(started)
				<-ctx.Done()
				close(canceled)
				return worker.Result{Class: worker.Success}
			})
			_, stop, done := start(t, f, config("lease-loss", 1), worker.Registry{"demo.echo": {Handler: h}})
			defer stop()
			select {
			case <-started:
			case <-time.After(3 * time.Second):
				t.Fatal("handler not started")
			}
			if failure == "expired" {
				if _, err := f.db.ExecContext(t.Context(), `UPDATE jobs SET lease_expires_at=clock_timestamp() WHERE id=$1`, id); err != nil {
					t.Fatal(err)
				}
			} else {
				f.hb.Close()
			}
			stopped(t, done, true)
			select {
			case <-canceled:
			default:
				t.Fatal("handler not canceled")
			}
			if status(t, f, id).Status != jobs.Running {
				t.Fatal("canceled handler completed/released lease")
			}
		})
	}
}

func TestAbandonedClaimWaitsForM5Recovery(t *testing.T) {
	f := database(t)
	id := submit(t, f, "demo.echo", true, 3)
	claim, err := f.repo.ClaimDispatched(t.Context(), jobs.DispatchClaimRequest{Owner: "crashed-instance", Types: []string{"demo.echo"}, LeaseDuration: 100 * time.Millisecond, AttemptTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	} // model process loss immediately after its claim committed
	time.Sleep(120 * time.Millisecond)
	if err := f.repo.Complete(t.Context(), jobs.Completion{Ownership: claim.Ownership, Outcome: jobs.CompletionSucceeded}); !errors.Is(err, jobs.ErrLeaseLost) {
		t.Fatal("expired completion", err)
	}
	_, stop, done := start(t, f, config("replacement", 1), worker.Registry{"demo.echo": {Handler: worker.Echo{}, ReplaySafe: true}})
	time.Sleep(60 * time.Millisecond)
	stop()
	stopped(t, done, false)
	if j := status(t, f, id); j.Status != jobs.Running || j.Attempts != 1 {
		t.Fatal("replacement stole abandoned claim")
	}
}

// This helper is run only in the child test process below. It uses the real
// PostgreSQL schema supplied by its parent, not a production command hook.
func TestWorkerProcessHelper(t *testing.T) {
	mode := os.Getenv("WORKER_PROCESS_TEST_MODE")
	if mode == "" {
		return
	}
	cfg, err := pgx.ParseConfig(os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal("invalid test DB")
	}
	cfg.RuntimeParams["search_path"] = os.Getenv("WORKER_PROCESS_TEST_SCHEMA")
	db := stdlib.OpenDB(*cfg)
	defer db.Close()
	hb := stdlib.OpenDB(*cfg)
	defer hb.Close()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()
	h := worker.HandlerFunc(func(ctx context.Context, _ jobs.Claim) worker.Result {
		fmt.Fprintln(os.Stdout, "CLAIMED")
		if mode == "sigterm" {
			select {
			case <-time.After(350 * time.Millisecond):
				return worker.Result{Class: worker.Success}
			case <-ctx.Done():
				return worker.Result{Class: worker.UnknownOutcome}
			}
		}
		<-ctx.Done()
		return worker.Result{Class: worker.UnknownOutcome}
	})
	p, err := worker.New(config("process-test", 1), storage.NewRepository(db, "default"), storage.NewRepository(hb, "default"), worker.Registry{"demo.echo": {Handler: h}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if mode == "before-claim" {
		fmt.Fprintln(os.Stdout, "BEFORE_CLAIM")
		time.Sleep(time.Hour)
		return
	}
	if err := p.Run(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerProcessCrashAndSIGTERM(t *testing.T) {
	for _, mode := range []string{"before-claim", "after-claim", "sigterm"} {
		t.Run(mode, func(t *testing.T) {
			f := database(t)
			id := submit(t, f, "demo.echo", true, 3)
			var schema string
			if err := f.db.QueryRowContext(t.Context(), `SELECT current_schema()`).Scan(&schema); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestWorkerProcessHelper$")
			cmd.Env = append(os.Environ(), "WORKER_PROCESS_TEST_MODE="+mode, "WORKER_PROCESS_TEST_SCHEMA="+schema)
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if cmd.ProcessState == nil {
					cmd.Process.Kill()
					cmd.Wait()
				}
			}()
			line, err := bufio.NewReader(stdout).ReadString('\n')
			want := "CLAIMED\n"
			if mode == "before-claim" {
				want = "BEFORE_CLAIM\n"
			}
			if err != nil || line != want {
				t.Fatalf("child failed to reach boundary: %q %v", line, err)
			}
			if mode == "sigterm" {
				if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
					t.Fatal(err)
				}
				if err := cmd.Wait(); err != nil {
					t.Fatal("SIGTERM drain failed", err)
				}
				if j := status(t, f, id); j.Status != jobs.Succeeded || j.Attempts != 1 {
					t.Fatalf("SIGTERM interrupted owned attempt: %+v", j)
				}
				return
			}
			if err := cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			if err := cmd.Wait(); err == nil {
				t.Fatal("SIGKILL did not terminate child")
			}
			if mode == "before-claim" {
				if j := status(t, f, id); j.Status != jobs.Ready || j.Attempts != 0 {
					t.Fatal("preclaim crash changed job")
				}
				_, stop, done := start(t, f, config("replacement", 1), worker.Registry{"demo.echo": {Handler: worker.Echo{}, ReplaySafe: true}})
				wait(t, func() bool { return status(t, f, id).Status == jobs.Succeeded })
				stop()
				stopped(t, done, false)
			} else {
				if j := status(t, f, id); j.Status != jobs.Running || j.Attempts != 1 {
					t.Fatal("postclaim crash lost ownership record")
				}
				wait(t, func() bool {
					var expired bool
					if err := f.db.QueryRowContext(t.Context(), `SELECT lease_expires_at<=clock_timestamp() FROM jobs WHERE id=$1`, id).Scan(&expired); err != nil {
						t.Fatal(err)
					}
					return expired
				})
				_, stop, done := start(t, f, config("replacement", 1), worker.Registry{"demo.echo": {Handler: worker.Echo{}, ReplaySafe: true}})
				time.Sleep(50 * time.Millisecond)
				stop()
				stopped(t, done, false)
				if j := status(t, f, id); j.Status != jobs.Running || j.Attempts != 1 {
					t.Fatal("replacement reclaimed crashed job without recovery")
				}
			}
		})
	}
}
