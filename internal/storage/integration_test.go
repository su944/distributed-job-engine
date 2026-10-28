//go:build integration

package storage_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/su944/distributed-job-engine/internal/api"
	"github.com/su944/distributed-job-engine/internal/config"
	"github.com/su944/distributed-job-engine/internal/jobs"
	"github.com/su944/distributed-job-engine/internal/storage"
	"github.com/su944/distributed-job-engine/migrations"
)

// Every test owns an isolated schema. Missing infrastructure fails this explicit suite.
func database(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("TEST_DATABASE_URL is required for integration tests")
	}
	load := func(dsn string) config.Config {
		c, err := config.Load(func(k string) string {
			if k == "DATABASE_URL" {
				return dsn
			}
			return ""
		})
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	admin, err := storage.Open(ctx, load(dsn))
	if err != nil {
		t.Fatal(err)
	}
	id, err := jobs.UUID()
	if err != nil {
		t.Fatal(err)
	}
	schema := "test_" + strings.ReplaceAll(id, "-", "")
	if _, err := admin.ExecContext(ctx, `CREATE SCHEMA "`+schema+`"`); err != nil {
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
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal("TEST_DATABASE_URL must be a PostgreSQL URL")
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	db, err := storage.Open(ctx, load(u.String()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if err := migrations.Apply(ctx, db); err != nil {
		t.Fatal(err)
	}
	return db
}

func submission(t *testing.T, key, body string) jobs.Submission {
	t.Helper()
	s, err := jobs.ParseSubmission(key, []byte(body), map[string]bool{"invoice.send": true})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

const body = `{"type":"invoice.send","payload":{"invoice_id":"inv_001","amount":1}}`

func count(t *testing.T, db *sql.DB, table string, want int) {
	t.Helper()
	var n int
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	// table names are fixed test constants, never request data.
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM `+table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != want {
		t.Fatalf("%s count %d, want %d", table, n, want)
	}
}

func TestConcurrentIdempotencyAndPersistence(t *testing.T) {
	db := database(t)
	repo := storage.NewRepository(db, "default")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	s := submission(t, "same-key", body)
	type result struct {
		job     jobs.Job
		created bool
		err     error
	}
	results := make(chan result, 24)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 24 {
		wg.Go(func() { <-start; j, c, e := repo.Submit(ctx, s); results <- result{j, c, e} })
	}
	close(start)
	wg.Wait()
	close(results)
	id := ""
	created := 0
	for r := range results {
		if r.err != nil {
			t.Fatal(r.err)
		}
		if id == "" {
			id = r.job.ID
		}
		if r.job.ID != id {
			t.Fatal("different IDs for same key")
		}
		if r.created {
			created++
		}
	}
	if created != 1 {
		t.Fatalf("created %d times", created)
	}
	count(t, db, "jobs", 1)
	count(t, db, "outbox_events", 1)
	j, err := storage.NewRepository(db, "default").Get(ctx, id)
	if err != nil || j.ID != id || j.Status != jobs.Ready || j.Attempts != 0 {
		t.Fatalf("persisted %+v %v", j, err)
	}
	if _, err := storage.NewRepository(db, "another").Get(ctx, id); !errors.Is(err, jobs.ErrNotFound) {
		t.Fatalf("namespace leak: %v", err)
	}
	equivalent := submission(t, "same-key", `{"max_attempts":5,"payload":{"amount":1.0,"invoice_id":"inv_001"},"type":"invoice.send"}`)
	if j, c, err := repo.Submit(ctx, equivalent); err != nil || c || j.ID != id {
		t.Fatalf("logical replay: %+v %t %v", j, c, err)
	}
	changed := submission(t, "same-key", strings.Replace(body, "inv_001", "inv_002", 1))
	if _, _, err := repo.Submit(ctx, changed); !errors.Is(err, jobs.ErrConflict) {
		t.Fatalf("expected conflict: %v", err)
	}
	// Same key in a different trusted namespace is a different submission.
	if _, c, err := storage.NewRepository(db, "another").Submit(ctx, s); err != nil || !c {
		t.Fatalf("namespace submission %t %v", c, err)
	}
	count(t, db, "jobs", 2)
	count(t, db, "outbox_events", 2)
	var eventJob string
	if err := db.QueryRowContext(ctx, `SELECT payload->>'job_id' FROM outbox_events WHERE aggregateid=$1`, id).Scan(&eventJob); err != nil || eventJob != id {
		t.Fatalf("outbox envelope: %s %v", eventJob, err)
	}
}

func TestOutboxFailureRollsBackJob(t *testing.T) {
	db := database(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, `ALTER TABLE outbox_events ADD CONSTRAINT reject_all CHECK(false)`); err != nil {
		t.Fatal(err)
	}
	if _, _, err := storage.NewRepository(db, "default").Submit(ctx, submission(t, "key", body)); err == nil {
		t.Fatal("expected outbox failure")
	}
	count(t, db, "jobs", 0)
	count(t, db, "outbox_events", 0)
}

func TestCanceledContextAndMigrationRepeat(t *testing.T) {
	db := database(t)
	if err := migrations.Apply(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := storage.NewRepository(db, "default").Submit(ctx, submission(t, "key", body)); err == nil {
		t.Fatal("canceled insert succeeded")
	}
	count(t, db, "jobs", 0)
	if _, err := db.ExecContext(context.Background(), `UPDATE schema_migrations SET checksum='changed' WHERE version=1`); err != nil {
		t.Fatal(err)
	}
	if err := migrations.Apply(context.Background(), db); err == nil {
		t.Fatal("checksum drift not detected")
	}
}

func TestSQLConstraints(t *testing.T) {
	db := database(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	query := `INSERT INTO jobs(id,namespace,idempotency_key,request_hash,type,payload,max_attempts,status) VALUES($1,'default',$2,$3,'invoice.send','{}', $4,$5)`
	for i, tc := range []struct {
		key    string
		hash   []byte
		max    int
		status string
	}{
		{"", make([]byte, 32), 5, "READY"}, {"x", make([]byte, 31), 5, "READY"}, {"x", make([]byte, 32), 0, "READY"},
		{"x", make([]byte, 32), 21, "READY"}, {"x", make([]byte, 32), 5, "BOGUS"}, {"x", make([]byte, 32), 5, "RUNNING"},
		{"x", make([]byte, 32), 5, "SUCCEEDED"},
	} {
		id, _ := jobs.UUID()
		if _, err := db.ExecContext(ctx, query, id, tc.key, tc.hash, tc.max, tc.status); err == nil {
			t.Fatalf("constraint case %d accepted", i)
		}
	}
	id, _ := jobs.UUID()
	if _, err := db.ExecContext(ctx, query, id, "unique", make([]byte, 32), 5, "READY"); err != nil {
		t.Fatal(err)
	}
	id, _ = jobs.UUID()
	if _, err := db.ExecContext(ctx, query, id, "unique", make([]byte, 32), 5, "READY"); err == nil {
		t.Fatal("SQL uniqueness not enforced")
	}
}

func TestHTTPWithPostgreSQL(t *testing.T) {
	db := database(t)
	app := api.New(storage.NewRepository(db, "default"), func(ctx context.Context) error { return storage.Ready(ctx, db) }, slog.New(slog.NewJSONHandler(io.Discard, nil)), map[string]bool{"invoice.send": true}, time.Second)
	srv := httptest.NewServer(app)
	defer srv.Close()
	client := srv.Client()
	client.Timeout = 5 * time.Second
	post := func(body string) (int, jobs.Job) {
		t.Helper()
		req, err := http.NewRequest("POST", srv.URL+"/v1/jobs", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "api-key")
		r, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Body.Close()
		var j jobs.Job
		if r.StatusCode == 202 || r.StatusCode == 200 {
			if err := json.NewDecoder(r.Body).Decode(&j); err != nil {
				t.Fatal(err)
			}
			if r.Header.Get("Location") != "/v1/jobs/"+j.ID {
				t.Fatal("missing location")
			}
		}
		return r.StatusCode, j
	}
	status, j := post(body)
	if status != 202 {
		t.Fatal(status)
	}
	status, replay := post(body)
	if status != 200 || replay.ID != j.ID {
		t.Fatal("replay failed")
	}
	status, _ = post(strings.Replace(body, "inv_001", "different", 1))
	if status != 409 {
		t.Fatal(status)
	}
	for path, want := range map[string]int{"/v1/jobs/" + j.ID: 200, "/health/live": 200, "/health/ready": 200, "/v1/jobs/00000000-0000-0000-0000-000000000000": 404} {
		r, err := client.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		r.Body.Close()
		if r.StatusCode != want {
			t.Fatal(fmt.Sprintf("%s: %d", path, r.StatusCode))
		}
	}
	count(t, db, "jobs", 1)
	count(t, db, "outbox_events", 1)
}

func TestReadinessRequiresMigrationAndDatabase(t *testing.T) {
	db := database(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := storage.Ready(ctx, db); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `ALTER TABLE schema_migrations RENAME TO saved_migrations`); err != nil {
		t.Fatal(err)
	}
	if err := storage.Ready(ctx, db); err == nil {
		t.Fatal("missing schema reported ready")
	}
	if _, err := db.ExecContext(ctx, `ALTER TABLE saved_migrations RENAME TO schema_migrations`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := storage.Ready(ctx, db); err == nil {
		t.Fatal("closed database reported ready")
	}
}
