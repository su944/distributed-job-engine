//go:build integration

package dispatch

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/su944/distributed-job-engine/internal/config"
	"github.com/su944/distributed-job-engine/internal/jobs"
	"github.com/su944/distributed-job-engine/internal/storage"
	"github.com/su944/distributed-job-engine/migrations"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// This suite deliberately requires the explicitly configured disposable CDC
// stack. It does not silently skip or replace Kafka/Connect/PostgreSQL with mocks.
func TestCDCDispatchIntegration(t *testing.T) {
	for _, name := range []string{"TEST_DATABASE_URL", "TEST_KAFKA_BROKERS", "TEST_CONNECT_URL"} {
		if os.Getenv(name) == "" {
			t.Fatalf("%s required; use the disposable CDC stack in docs/cdc-runbook.md", name)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	base, err := config.Load(func(k string) string {
		if k == "DATABASE_URL" {
			return os.Getenv("TEST_DATABASE_URL")
		}
		return ""
	})
	if err != nil {
		t.Fatal(err)
	}
	db, err := storage.Open(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := migrations.Apply(ctx, db); err != nil {
		t.Fatal(err)
	}
	brokers := strings.Split(os.Getenv("TEST_KAFKA_BROKERS"), ",")
	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	admin := kadm.NewClient(producer)
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	nonce, _ := jobs.UUID()
	prefix := "test-" + nonce
	collector, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumerGroup(prefix+"-observe"), kgo.ConsumeTopics(Topic), kgo.DisableAutoCommit(), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.FetchMaxBytes(1<<20))
	if err != nil {
		t.Fatal(err)
	}
	defer collector.Close()
	connectURL := strings.TrimRight(os.Getenv("TEST_CONNECT_URL"), "/")
	waitConnector(t, ctx, connectURL, "RUNNING")
	request, err := jobs.ParseSubmission(prefix, []byte(`{"type":"invoice.send","payload":{"invoice_id":"cdc-integration"}}`), map[string]bool{"invoice.send": true})
	if err != nil {
		t.Fatal(err)
	}
	job, _, err := storage.NewRepository(db, "default").Submit(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	var initialID string
	if err := db.QueryRowContext(ctx, `SELECT id::text FROM outbox_events WHERE aggregateid=$1`, job.ID).Scan(&initialID); err != nil {
		t.Fatal(err)
	}
	initial := awaitEvent(t, ctx, collector, initialID, "")
	store := NewSQLStore(db, "test-cdc", prefix)
	t.Run("committed_insert_and_duplicate", func(t *testing.T) {
		if outcome, err := store.Handle(ctx, toRecord(initial)); err != nil || outcome != Received {
			t.Fatalf("first receipt: %s %v", outcome, err)
		}
		copy := &kgo.Record{Topic: Topic, Key: initial.Key, Value: initial.Value}
		if err := producer.ProduceSync(ctx, copy).FirstErr(); err != nil {
			t.Fatal(err)
		}
		replayed := awaitEvent(t, ctx, collector, initialID, "")
		if outcome, err := store.Handle(ctx, toRecord(replayed)); err != nil || outcome != Duplicate {
			t.Fatalf("replay: %s %v", outcome, err)
		}
		var n int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM dispatch_events WHERE event_id=$1`, initialID).Scan(&n); err != nil || n != 1 {
			t.Fatalf("logical notification count %d %v", n, err)
		}
	})
	t.Run("rollback_is_not_published", func(t *testing.T) {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		rolled := insertWakeup(t, ctx, tx, job.ID)
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		barrier := insertWakeup(t, ctx, db, job.ID)
		// Both events have the same key/partition. The later committed WAL barrier
		// proves CDC progressed beyond the rolled-back operation without emitting it.
		awaitEvent(t, ctx, collector, barrier, rolled)
	})
	t.Run("connector_restart_resumes", func(t *testing.T) {
		connectAction(t, ctx, connectURL, "PUT", "/pause")
		waitConnector(t, ctx, connectURL, "PAUSED")
		id := insertWakeup(t, ctx, db, job.ID)
		connectAction(t, ctx, connectURL, "PUT", "/resume")
		connectAction(t, ctx, connectURL, "POST", "/restart?includeTasks=true&onlyFailed=false")
		waitConnector(t, ctx, connectURL, "RUNNING")
		awaitEvent(t, ctx, collector, id, "")
	})
	t.Run("conflicting_event_id_is_quarantined", func(t *testing.T) {
		e, err := Decode(toRecord(initial))
		if err != nil {
			t.Fatal(err)
		}
		e.OccurredAt = e.OccurredAt.Add(time.Second)
		value, err := json.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
		bad := &kgo.Record{Topic: Topic, Key: initial.Key, Value: value}
		if err := producer.ProduceSync(ctx, bad).FirstErr(); err != nil {
			t.Fatal(err)
		}
		r := awaitEvent(t, ctx, collector, e.EventID, "")
		for range 2 {
			if outcome, err := store.Handle(ctx, toRecord(r)); err != nil || outcome != Quarantined {
				t.Fatalf("ID conflict: %s %v", outcome, err)
			}
		}
		var reason string
		if err := db.QueryRowContext(ctx, `SELECT reason FROM dispatch_quarantine WHERE source=$1 AND consumer_group=$2 AND topic=$3 AND partition=$4 AND record_offset=$5`, store.source, store.group, Topic, r.Partition, r.Offset).Scan(&reason); err != nil || reason != "event_id_conflict" {
			t.Fatalf("ID conflict quarantine: %s %v", reason, err)
		}
	})
	start := func(c Config, s Store) (*Dispatcher, context.CancelFunc, <-chan error) {
		d, err := New(c, s, func(c context.Context) error { return store.Ready(c) }, log)
		if err != nil {
			t.Fatal(err)
		}
		runCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- d.Run(runCtx) }()
		return d, stop, done
	}
	offset := func(group string, r *kgo.Record) int64 {
		positions, err := admin.FetchOffsets(ctx, group)
		if err != nil {
			t.Fatal(err)
		}
		p, ok := positions.Lookup(Topic, r.Partition)
		if !ok {
			return -1
		}
		if p.Err != nil {
			t.Fatal(p.Err)
		}
		return p.At
	}
	t.Run("quarantine_before_acknowledgement", func(t *testing.T) {
		group := prefix + "-quarantine"
		c := Config{brokers, group, "test-cdc"}
		sqlStore := NewSQLStore(db, c.Source, c.Group)
		bad := &kgo.Record{Topic: Topic, Key: []byte(job.ID), Value: []byte(`{"schema_version":99,"secret":"do-not-log"}`)}
		if err := producer.ProduceSync(ctx, bad).FirstErr(); err != nil {
			t.Fatal(err)
		}
		failing := storeFunc(func(c context.Context, r Record) (Outcome, error) {
			if r.Partition == bad.Partition && r.Offset == bad.Offset && bytes.Equal(r.Value, bad.Value) {
				return "", errors.New("injected persistence outage")
			}
			return sqlStore.Handle(c, r)
		})
		d, stop, done := start(c, failing)
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("failed persistence ignored")
			}
		case <-ctx.Done():
			t.Fatal("consumer did not stop on persistence failure")
		}
		stop()
		d.Close()
		if offset(group, bad) > bad.Offset {
			t.Fatal("invalid record acknowledged without quarantine")
		}
		var n int
		query := `SELECT count(*) FROM dispatch_quarantine WHERE source=$1 AND consumer_group=$2 AND topic=$3 AND partition=$4 AND record_offset=$5`
		if err := db.QueryRowContext(ctx, query, c.Source, c.Group, Topic, bad.Partition, bad.Offset).Scan(&n); err != nil || n != 0 {
			t.Fatalf("quarantine persisted during failure: %d %v", n, err)
		}
		d, stop, done = start(c, sqlStore)
		waitUntil(t, ctx, func() bool {
			if err := db.QueryRowContext(ctx, query, c.Source, c.Group, Topic, bad.Partition, bad.Offset).Scan(&n); err != nil {
				t.Fatal(err)
			}
			return n == 1 && offset(group, bad) > bad.Offset
		})
		stop()
		expectStopped(t, ctx, done)
		d.Close()
	})
	t.Run("shutdown_and_rebalance_replay_unacknowledged_record", func(t *testing.T) {
		id := insertWakeup(t, ctx, db, job.ID)
		target := awaitEvent(t, ctx, collector, id, "")
		group := prefix + "-rebalance"
		c := Config{brokers, group, "test-cdc"}
		sqlStore := NewSQLStore(db, c.Source, c.Group)
		started := make(chan struct{})
		var once atomic.Bool
		blocked := storeFunc(func(c context.Context, r Record) (Outcome, error) {
			e, _ := Decode(r)
			if e.EventID == id && once.CompareAndSwap(false, true) {
				close(started)
				<-c.Done()
				return "", c.Err()
			}
			return sqlStore.Handle(c, r)
		})
		a, stopA, doneA := start(c, blocked)
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("first consumer never received target")
		}
		b, stopB, doneB := start(c, sqlStore)
		// The second member joins while A holds a poll, forcing a real rebalance.
		waitUntil(t, ctx, func() bool {
			groups, err := admin.DescribeGroups(ctx, group)
			if err != nil {
				t.Fatal(err)
			}
			g, ok := groups[group]
			return ok && g.State != "Stable"
		})
		stopA()
		expectStopped(t, ctx, doneA)
		a.Close()
		waitUntil(t, ctx, func() bool {
			var n int
			if err := db.QueryRowContext(ctx, `SELECT count(*) FROM dispatch_events WHERE event_id=$1`, id).Scan(&n); err != nil {
				t.Fatal(err)
			}
			return n == 1 && offset(group, target) > target.Offset
		})
		stopB()
		expectStopped(t, ctx, doneB)
		b.Close()
	})
	current, err := storage.NewRepository(db, "default").Get(ctx, job.ID)
	if err != nil || current.Status != jobs.Ready || current.Attempts != 0 {
		t.Fatalf("dispatcher changed job state: %+v %v", current, err)
	}
}

func toRecord(r *kgo.Record) Record { return Record{r.Topic, r.Partition, r.Offset, r.Key, r.Value} }

type sqlExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func insertWakeup(t *testing.T, ctx context.Context, db sqlExecer, jobID string) string {
	t.Helper()
	id, err := jobs.UUID()
	if err != nil {
		t.Fatal(err)
	}
	e := Event{1, id, jobID, "JobReady", time.Now().UTC()}
	payload, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO outbox_events(id,aggregatetype,aggregateid,type,payload) VALUES($1,'jobs',$2,'JobReady',$3)`, id, jobID, payload); err != nil {
		t.Fatal(err)
	}
	return id
}

func awaitEvent(t *testing.T, parent context.Context, client *kgo.Client, want, forbidden string) *kgo.Record {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	for ctx.Err() == nil {
		fetches := client.PollRecords(ctx, 32)
		if errs := fetches.Errors(); len(errs) > 0 && ctx.Err() == nil {
			t.Fatal("Kafka observation fetch failed")
		}
		for _, r := range fetches.Records() {
			e, err := Decode(toRecord(r))
			if err != nil {
				continue
			}
			if forbidden != "" && e.EventID == forbidden {
				t.Fatal("rolled back outbox was published")
			}
			if e.EventID == want {
				return r
			}
		}
	}
	t.Fatalf("CDC event %s not observed before deadline", want)
	return nil
}

func connectAction(t *testing.T, ctx context.Context, base, method, path string) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, method, base+"/connectors/job-engine-outbox"+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		t.Fatal("Connect REST request failed")
	}
	defer r.Body.Close()
	if r.StatusCode < 200 || r.StatusCode >= 300 {
		t.Fatalf("Connect REST status %d", r.StatusCode)
	}
}

func waitConnector(t *testing.T, ctx context.Context, base, state string) {
	t.Helper()
	waitUntil(t, ctx, func() bool {
		req, err := http.NewRequestWithContext(ctx, "GET", base+"/connectors/job-engine-outbox/status", nil)
		if err != nil {
			t.Fatal(err)
		}
		r, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
		if err != nil {
			return false
		}
		defer r.Body.Close()
		var status struct {
			Connector struct {
				State string `json:"state"`
			} `json:"connector"`
			Tasks []struct {
				State string `json:"state"`
			} `json:"tasks"`
		}
		if r.StatusCode != 200 || json.NewDecoder(io.LimitReader(r.Body, 65536)).Decode(&status) != nil {
			return false
		}
		if status.Connector.State != state || len(status.Tasks) != 1 {
			return false
		}
		for _, task := range status.Tasks {
			if task.State != state {
				return false
			}
		}
		return true
	})
}

func waitUntil(t *testing.T, parent context.Context, condition func() bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for !condition() {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("condition not met before deadline")
		}
	}
}

func expectStopped(t *testing.T, ctx context.Context, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("shutdown exceeded bound")
	case <-ctx.Done():
		t.Fatal("integration deadline exceeded")
	}
}
