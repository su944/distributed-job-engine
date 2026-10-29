package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/su944/distributed-job-engine/internal/jobs"
)

func testConfig() Config {
	return Config{ID: "test-worker", Capacity: 2, PollInterval: 10 * time.Millisecond, LeaseDuration: time.Second, HeartbeatInterval: 50 * time.Millisecond, AttemptTimeout: 3 * time.Second, IOTimeout: 100 * time.Millisecond, DrainTimeout: time.Second}
}

func TestRegistryAndClassifications(t *testing.T) {
	r := Registry{}
	if err := r.Register("demo.echo", Echo{}, true); err != nil {
		t.Fatal(err)
	}
	if err := r.Register("demo.echo", Echo{}, true); err == nil {
		t.Fatal("duplicate allowed")
	}
	if err := r.Register("bad type", Echo{}, true); err == nil {
		t.Fatal("invalid type allowed")
	}
	c := jobs.Claim{Ownership: jobs.Ownership{JobID: "0b4027ce-0686-4191-8e77-d3bc5a9d944d", Owner: "test", LeaseToken: 1}, Payload: json.RawMessage(`{"message":"hello"}`)}
	if got := invoke(t.Context(), Echo{}, c); got.Class != Success || string(got.Value) != string(c.Payload) {
		t.Fatalf("echo: %+v", got)
	}
	for _, tc := range []struct {
		class Classification
		safe  bool
		want  jobs.CompletionOutcome
	}{
		{Success, true, jobs.CompletionSucceeded}, {PermanentFailure, false, jobs.CompletionPermanent},
		{UnknownOutcome, true, jobs.CompletionUnknown}, {RetryableFailure, true, jobs.CompletionRetryable}, {RetryableFailure, false, jobs.CompletionUnknown}, {"bad", true, jobs.CompletionUnknown},
	} {
		if got := completion(c, Registration{Handler: Echo{}, ReplaySafe: tc.safe}, Result{Class: tc.class}); got.Outcome != tc.want {
			t.Fatalf("classification %s: %+v", tc.class, got)
		}
	}
	if got := completion(c, r["demo.echo"], Result{Class: Success, Value: json.RawMessage(`[]`)}); got.Outcome != jobs.CompletionUnknown {
		t.Fatal("malformed success accepted")
	}
	if got := invoke(t.Context(), HandlerFunc(func(context.Context, jobs.Claim) Result { panic("secret panic") }), c); got.Class != UnknownOutcome || strings.Contains(got.Code, "secret") {
		t.Fatal("unsafe panic handling")
	}
}

func TestWorkerConfiguration(t *testing.T) {
	if _, err := LoadConfig(func(string) string { return "" }); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, value string }{
		{"WORKER_CAPACITY", "0"}, {"WORKER_CAPACITY", "65"}, {"WORKER_CAPACITY", "abc"},
		{"WORKER_ID", "bad id"}, {"WORKER_POLL_INTERVAL", "1ms"}, {"WORKER_LEASE_DURATION", "1s"},
		{"WORKER_HEARTBEAT_INTERVAL", "60s"}, {"WORKER_ATTEMPT_TIMEOUT", "1s"}, {"WORKER_IO_TIMEOUT", "invalid"},
	} {
		if _, err := LoadConfig(func(k string) string {
			if k == tc.name {
				return tc.value
			}
			return ""
		}); err == nil {
			t.Fatalf("accepted %s=%s", tc.name, tc.value)
		}
	}
}

type fakeRepository struct {
	claims   atomic.Int64
	renew    func(context.Context, jobs.Ownership, time.Duration) (time.Time, error)
	complete atomic.Int64
}

func (f *fakeRepository) ClaimDispatched(ctx context.Context, q jobs.DispatchClaimRequest) (jobs.Claim, error) {
	if f.claims.Add(1) > 1 {
		return jobs.Claim{}, jobs.ErrNotClaimable
	}
	now := time.Now()
	return jobs.Claim{Ownership: jobs.Ownership{JobID: "0b4027ce-0686-4191-8e77-d3bc5a9d944d", Owner: q.Owner, LeaseToken: 1}, Type: "demo.echo", AcquiredAt: now, Deadline: now.Add(q.AttemptTimeout), LeaseExpiresAt: now.Add(q.LeaseDuration), Payload: json.RawMessage(`{}`)}, nil
}
func (f *fakeRepository) Renew(ctx context.Context, o jobs.Ownership, d time.Duration) (time.Time, error) {
	if f.renew != nil {
		return f.renew(ctx, o, d)
	}
	return time.Now().Add(d), nil
}
func (f *fakeRepository) Complete(context.Context, jobs.Completion) error {
	f.complete.Add(1)
	return nil
}
func (f *fakeRepository) WorkerReady(context.Context) error { return nil }

func TestRenewalUncertaintyCancelsWithoutCompletion(t *testing.T) {
	f := &fakeRepository{renew: func(context.Context, jobs.Ownership, time.Duration) (time.Time, error) {
		return time.Time{}, jobs.ErrOwnershipUnknown
	}}
	canceled := make(chan struct{})
	h := HandlerFunc(func(ctx context.Context, _ jobs.Claim) Result {
		<-ctx.Done()
		close(canceled)
		return Result{Class: Success}
	})
	p, err := New(testConfig(), f, f, Registry{"demo.echo": {Handler: h}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if err := p.Run(ctx); err == nil {
		t.Fatal("uncertain renewal ignored")
	}
	select {
	case <-canceled:
	default:
		t.Fatal("handler did not observe cancellation")
	}
	if f.complete.Load() != 0 {
		t.Fatal("completed after uncertain renewal")
	}
}

func TestLeaseGuardCancelsBlockedRenewal(t *testing.T) {
	f := &fakeRepository{renew: func(ctx context.Context, _ jobs.Ownership, _ time.Duration) (time.Time, error) {
		<-ctx.Done()
		return time.Time{}, ctx.Err()
	}}
	c := testConfig()
	c.Capacity = 1
	c.LeaseDuration = 150 * time.Millisecond
	c.HeartbeatInterval = 30 * time.Millisecond
	c.IOTimeout = 100 * time.Millisecond
	p, err := New(c, f, f, Registry{"demo.echo": {Handler: HandlerFunc(func(ctx context.Context, _ jobs.Claim) Result { <-ctx.Done(); return Result{Class: UnknownOutcome} })}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := p.Run(t.Context()); err == nil {
		t.Fatal("blocked renewal ignored")
	}
	if time.Since(started) > time.Second || f.complete.Load() != 0 {
		t.Fatal("lease uncertainty did not bound execution")
	}
}

func TestWorkerHealthAndSingleRun(t *testing.T) {
	f := &fakeRepository{}
	p, err := New(testConfig(), f, f, Registry{"demo.echo": {Handler: Echo{}}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	for path, want := range map[string]int{"/health/live": 200, "/health/ready": 503, "/metrics": 200, "/missing": 404} {
		w := httptest.NewRecorder()
		p.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != want {
			t.Fatalf("%s status %d", path, w.Code)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := p.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := p.Run(t.Context()); err == nil || errors.Is(err, ErrDrainTimeout) {
		t.Fatal("pool restarted")
	}
}
