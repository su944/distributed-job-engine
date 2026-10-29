//go:build integration

package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/su944/distributed-job-engine/internal/config"
	"github.com/su944/distributed-job-engine/internal/jobs"
	"github.com/su944/distributed-job-engine/internal/storage"
	"github.com/su944/distributed-job-engine/internal/worker"
	"github.com/su944/distributed-job-engine/migrations"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// This uses real CDC, Kafka group offsets and rebalance, but no new Kafka worker
// group: the unchanged dispatcher commits only after SQL receipt/quarantine.
func TestWorkerDurableHandoffAndKafkaRebalance(t *testing.T) {
	for _, name := range []string{"TEST_DATABASE_URL", "TEST_KAFKA_BROKERS", "TEST_CONNECT_URL"} {
		if os.Getenv(name) == "" {
			t.Fatalf("%s required; real dependencies are never skipped", name)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	c, err := config.Load(func(k string) string {
		if k == "DATABASE_URL" {
			return os.Getenv("TEST_DATABASE_URL")
		}
		return ""
	})
	if err != nil {
		t.Fatal(err)
	}
	db, err := storage.Open(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	heartbeats, err := storage.Open(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	defer heartbeats.Close()
	if err := migrations.Apply(ctx, db); err != nil {
		t.Fatal(err)
	}
	waitConnector(t, ctx, strings.TrimRight(os.Getenv("TEST_CONNECT_URL"), "/"), "RUNNING")
	id, err := jobs.UUID()
	if err != nil {
		t.Fatal(err)
	}
	namespace := "worker-" + id
	repo := storage.NewRepository(db, namespace)
	request, err := jobs.ParseSubmission("worker-handoff-"+id, []byte(`{"type":"demo.echo","payload":{"message":"real-cdc"}}`), map[string]bool{"demo.echo": true})
	if err != nil {
		t.Fatal(err)
	}
	job, _, err := repo.Submit(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	var eventID string
	var payload []byte
	if err := db.QueryRowContext(ctx, `SELECT id::text,payload FROM outbox_events WHERE aggregateid=$1`, job.ID).Scan(&eventID, &payload); err != nil {
		t.Fatal(err)
	}
	brokers := strings.Split(os.Getenv("TEST_KAFKA_BROKERS"), ",")
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	admin := kadm.NewClient(client)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	group := "worker-handoff-" + id
	kc := Config{Brokers: brokers, Group: group, Source: "worker-integration"}
	store := NewSQLStore(db, kc.Source, kc.Group)
	// Interpose only to observe the target's durable receipt and Kafka coordinate.
	// Offset commits still run through the real Dispatch.Run implementation.
	target := make(chan Record, 1)
	observing := storeFunc(func(ctx context.Context, r Record) (Outcome, error) {
		outcome, err := store.Handle(ctx, r)
		if err == nil {
			e, decodeErr := Decode(r)
			if decodeErr == nil && e.EventID == eventID {
				select {
				case target <- r:
				default:
				}
			}
		}
		return outcome, err
	})
	startDispatcher := func(s Store) (*Dispatcher, context.CancelFunc, <-chan error) {
		d, err := New(kc, s, store.Ready, log)
		if err != nil {
			t.Fatal(err)
		}
		runCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- d.Run(runCtx) }()
		t.Cleanup(func() { stop(); d.Close() })
		return d, stop, done
	}
	d, stop, dDone := startDispatcher(observing)
	var coordinate Record
	select {
	case coordinate = <-target:
	case <-ctx.Done():
		t.Fatal("CDC receipt not delivered")
	}
	waitUntil(t, ctx, func() bool {
		offsets, err := admin.FetchOffsets(ctx, group)
		if err != nil {
			t.Fatal(err)
		}
		o, ok := offsets.Lookup(Topic, coordinate.Partition)
		return ok && o.Err == nil && o.At > coordinate.Offset
	})
	stop()
	expectStopped(t, ctx, dDone)
	d.Close()
	current, err := repo.Get(ctx, job.ID)
	if err != nil || current.Status != jobs.Ready || current.Attempts != 0 {
		t.Fatal("receipt changed ownership", err)
	}
	// Boundary restart: the dispatcher is gone and its offset is already past
	// the notification. Only the persisted receipt can now wake a new worker.
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	handlerCanceled := make(chan struct{}, 1)
	h := worker.HandlerFunc(func(ctx context.Context, c jobs.Claim) worker.Result {
		close(started)
		select {
		case <-release:
			return worker.Result{Class: worker.Success, Value: json.RawMessage(`{"ok":true}`)}
		case <-ctx.Done():
			handlerCanceled <- struct{}{}
			return worker.Result{Class: worker.UnknownOutcome}
		}
	})
	wc := worker.Config{ID: "worker-handoff-" + id, Capacity: 1, PollInterval: 20 * time.Millisecond, LeaseDuration: 10 * time.Second, HeartbeatInterval: time.Second, AttemptTimeout: time.Minute, IOTimeout: 2 * time.Second, DrainTimeout: 2 * time.Second}
	pool, err := worker.New(wc, repo, storage.NewRepository(heartbeats, namespace), worker.Registry{"demo.echo": {Handler: h, ReplaySafe: true}}, log)
	if err != nil {
		t.Fatal(err)
	}
	workerCtx, stopWorker := context.WithCancel(ctx)
	defer stopWorker()
	workerDone := make(chan error, 1)
	go func() { workerDone <- pool.Run(workerCtx) }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("receipt was permanently skipped")
	}
	a, stopA, doneA := startDispatcher(store)
	waitUntil(t, ctx, func() bool { return a.assigned.Load() == 3 })
	b, stopB, doneB := startDispatcher(store)
	waitUntil(t, ctx, func() bool {
		return a.assigned.Load() > 0 && b.assigned.Load() > 0 && a.assigned.Load()+b.assigned.Load() == 3
	})
	duplicate := &kgo.Record{Topic: Topic, Key: []byte(job.ID), Value: payload}
	if err := client.ProduceSync(ctx, duplicate).FirstErr(); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, ctx, func() bool {
		offsets, err := admin.FetchOffsets(ctx, group)
		if err != nil {
			t.Fatal(err)
		}
		o, ok := offsets.Lookup(Topic, duplicate.Partition)
		return ok && o.Err == nil && o.At > duplicate.Offset
	})
	select {
	case <-handlerCanceled:
		t.Fatal("Kafka rebalance canceled database-owned handler")
	default:
	}
	if j, err := repo.Get(ctx, job.ID); err != nil || j.Status != jobs.Running || j.Attempts != 1 {
		t.Fatal("duplicate/rebalance changed claim", err)
	}
	releaseOnce.Do(func() { close(release) })
	waitUntil(t, ctx, func() bool { j, err := repo.Get(ctx, job.ID); return err == nil && j.Status == jobs.Succeeded })
	stopWorker()
	select {
	case err := <-workerDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("worker failed to drain")
	}
	stopA()
	stopB()
	expectStopped(t, ctx, doneA)
	expectStopped(t, ctx, doneB)
	a.Close()
	b.Close()
	var attempts int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM job_attempts WHERE job_id=$1`, job.ID).Scan(&attempts); err != nil || attempts != 1 {
		t.Fatalf("attempts=%d %v", attempts, err)
	}
	// Real DB unavailability at receipt time must still leave the offset pending.
	failureGroup := group + "-db-failure"
	kc.Group = failureGroup
	failingStore := storeFunc(func(context.Context, Record) (Outcome, error) { return "", errors.New("injected database unavailable") })
	failing, stopFailure, doneFailure := startDispatcher(failingStore)
	select {
	case err := <-doneFailure:
		if err == nil {
			t.Fatal("DB failure ignored")
		}
	case <-ctx.Done():
		t.Fatal("dispatcher did not fail closed")
	}
	stopFailure()
	failing.Close()
	offsets, err := admin.FetchOffsets(ctx, failureGroup)
	if err != nil {
		t.Fatal(err)
	}
	if o, ok := offsets.Lookup(Topic, coordinate.Partition); ok && o.At > coordinate.Offset {
		t.Fatal("offset advanced on failed durable handoff")
	}
}
