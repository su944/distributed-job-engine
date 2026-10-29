package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/su944/distributed-job-engine/internal/jobs"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Config struct {
	Brokers       []string
	Group, Source string
}

func LoadConfig(getenv func(string) string) (Config, error) {
	c := Config{Brokers: strings.Split(getenv("KAFKA_BROKERS"), ","), Group: getenv("DISPATCH_GROUP"), Source: getenv("KAFKA_SOURCE")}
	if c.Group == "" {
		c.Group = "job-engine-dispatch-v1"
	}
	if c.Source == "" {
		c.Source = "local-v1"
	}
	if !jobs.ValidType(c.Group) || !jobs.ValidType(c.Source) || len(c.Brokers) > 8 {
		return c, errors.New("invalid dispatcher identity or broker count")
	}
	for i, b := range c.Brokers {
		b = strings.TrimSpace(b)
		host, port, err := net.SplitHostPort(b)
		n, e := strconv.Atoi(port)
		if err != nil || e != nil || host == "" || n < 1 || n > 65535 {
			return c, errors.New("KAFKA_BROKERS requires host:port addresses")
		}
		c.Brokers[i] = b
	}
	return c, nil
}

type Dispatcher struct {
	client                                      *kgo.Client
	store                                       Store
	storeReady                                  func(context.Context) error
	log                                         *slog.Logger
	running, draining                           atomic.Bool
	assigned                                    atomic.Int64
	received, duplicates, quarantined, failures atomic.Uint64
}

func New(c Config, store Store, ready func(context.Context) error, logger *slog.Logger) (*Dispatcher, error) {
	d := &Dispatcher{store: store, storeReady: ready, log: logger}
	client, err := kgo.NewClient(kgo.SeedBrokers(c.Brokers...), kgo.ConsumerGroup(c.Group), kgo.ConsumeTopics(Topic),
		kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(), kgo.Balancers(kgo.RangeBalancer()), kgo.ConsumeResetOffset(kgo.NoResetOffset().AtStart()),
		kgo.FetchMaxBytes(1<<20), kgo.FetchMaxPartitionBytes(1<<20), kgo.MaxConcurrentFetches(1), kgo.BrokerMaxReadBytes(2<<20), kgo.MaxDecompressBatchBytes(1<<20),
		kgo.RebalanceTimeout(30*time.Second), kgo.SessionTimeout(10*time.Second), kgo.HeartbeatInterval(3*time.Second),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, parts map[string][]int32) {
			n := 0
			for _, p := range parts {
				n += len(p)
			}
			d.assigned.Store(int64(n))
			logger.Info("dispatcher partitions assigned", "count", n)
		}),
		kgo.OnPartitionsRevoked(func(context.Context, *kgo.Client, map[string][]int32) {
			d.assigned.Store(0)
			logger.Info("dispatcher partitions revoked")
		}),
		kgo.OnPartitionsLost(func(context.Context, *kgo.Client, map[string][]int32) {
			d.assigned.Store(0)
			logger.Warn("dispatcher partitions lost")
		}),
	)
	if err != nil {
		return nil, err
	}
	d.client = client
	return d, nil
}

// Run handles one polled record at a time. Failures stop this instance rather than
// advancing a failed partition or accumulating in-memory retry work. The caller
// owns Close, so health probes can remain active until HTTP drains.
func (d *Dispatcher) Run(ctx context.Context) error {
	d.running.Store(true)
	defer d.running.Store(false)
	for {
		fetches := d.client.PollRecords(ctx, 1)
		if ctx.Err() != nil {
			d.client.AllowRebalance()
			return ctx.Err()
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			d.failures.Add(1)
			d.client.AllowRebalance()
			return errors.New("Kafka fetch failed")
		}
		for _, record := range fetches.Records() {
			if d.draining.Load() {
				d.client.AllowRebalance()
				return context.Canceled
			}
			err := d.handle(ctx, record, func(ctx context.Context, r *kgo.Record) error { return d.client.CommitRecords(ctx, r) })
			d.client.AllowRebalance()
			if err != nil {
				return err
			}
		}
		d.client.AllowRebalance()
	}
}

func (d *Dispatcher) handle(ctx context.Context, r *kgo.Record, commit func(context.Context, *kgo.Record) error) error {
	opCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	outcome, err := d.store.Handle(opCtx, Record{r.Topic, r.Partition, r.Offset, r.Key, r.Value})
	cancel()
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		d.failures.Add(1)
		d.log.Error("notification persistence failed; offset uncommitted")
		return errors.New("notification persistence failed")
	}
	switch outcome {
	case Received:
		d.received.Add(1)
	case Duplicate:
		d.duplicates.Add(1)
	case Quarantined:
		d.quarantined.Add(1)
	default:
		return errors.New("unknown durable outcome")
	}
	commitCtx, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	if err := commit(commitCtx, r); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		d.failures.Add(1)
		d.log.Error("offset commit failed; replay is safe")
		return errors.New("offset commit failed")
	}
	d.log.Info("notification durably handled", "topic", r.Topic, "partition", r.Partition, "offset", r.Offset, "outcome", outcome)
	return nil
}

func (d *Dispatcher) Drain() { d.draining.Store(true) }
func (d *Dispatcher) Close() {
	d.client.AllowRebalance()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = d.client.LeaveGroupContext(ctx)
	d.client.CloseAllowingRebalance()
}
func (d *Dispatcher) Ready(ctx context.Context) error {
	if !d.running.Load() || d.draining.Load() || d.assigned.Load() == 0 {
		return errors.New("dispatcher not assigned or draining")
	}
	if err := d.storeReady(ctx); err != nil {
		return errors.New("dispatcher database unavailable")
	}
	return d.client.Ping(ctx)
}

func (d *Dispatcher) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Allow", "GET")
		w.WriteHeader(405)
		_, _ = w.Write([]byte(`{"error":{"code":"method_not_allowed"}}`))
		return
	}
	if r.URL.Path == "/metrics" {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "dispatch_received_total %d\ndispatch_duplicates_total %d\ndispatch_quarantined_total %d\ndispatch_errors_total %d\ndispatch_assigned_partitions %d\n", d.received.Load(), d.duplicates.Load(), d.quarantined.Load(), d.failures.Load(), d.assigned.Load())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	switch r.URL.Path {
	case "/health/live":
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "live"})
	case "/health/ready":
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		if err := d.Ready(ctx); err != nil {
			w.WriteHeader(503)
			_, _ = w.Write([]byte(`{"error":{"code":"not_ready"}}`))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
	default:
		w.WriteHeader(404)
		_, _ = w.Write([]byte(`{"error":{"code":"not_found"}}`))
	}
}
