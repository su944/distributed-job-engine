package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/su944/distributed-job-engine/internal/jobs"
)

var ErrDrainTimeout = errors.New("worker drain deadline exceeded; process must exit")

type Repository interface {
	jobs.DispatchedClaimer
	jobs.LeaseRenewer
	jobs.JobCompleter
}

type Pool struct {
	c          Config
	repo       Repository
	heartbeats jobs.LeaseRenewer
	registry   Registry
	log        *slog.Logger
	started    atomic.Bool
	running    atomic.Bool
	draining   atomic.Bool
	active     atomic.Int64
	completed  atomic.Uint64
	failed     atomic.Uint64
}

// heartbeatRepo should use a separate bounded connection pool so claims and
// completion cannot occupy all renewal connections. Tests may inject a fake.
func New(c Config, repo Repository, heartbeatRepo jobs.LeaseRenewer, registry Registry, log *slog.Logger) (*Pool, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	if repo == nil || heartbeatRepo == nil || log == nil || len(registry) == 0 || len(registry) > 128 {
		return nil, errors.New("worker requires repository, heartbeat repository, logger and handlers")
	}
	copyRegistry := make(Registry, len(registry))
	for typ, r := range registry {
		if err := copyRegistry.Register(typ, r.Handler, r.ReplaySafe); err != nil {
			return nil, err
		}
	}
	return &Pool{c: c, repo: repo, heartbeats: heartbeatRepo, registry: copyRegistry, log: log}, nil
}

// Run starts exactly Capacity slot goroutines and no work queue. Each slot
// reserves its execution capacity by existing before it claims. A separate
// execution lifetime lets SIGTERM stop claims while active work drains/renews.
func (p *Pool) Run(ctx context.Context) error {
	if !p.started.CompareAndSwap(false, true) {
		return errors.New("worker pool cannot be restarted")
	}
	intake, stopIntake := context.WithCancel(ctx)
	defer stopIntake()
	executions, stopExecutions := context.WithCancel(context.Background())
	defer stopExecutions()
	errorsCh := make(chan error, 1)
	var wg sync.WaitGroup
	p.running.Store(true)
	defer p.running.Store(false)
	for i := 0; i < p.c.Capacity; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := p.slot(intake, executions); err != nil {
				select {
				case errorsCh <- err:
				default:
				}
				stopIntake()
			}
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	<-intake.Done()
	p.draining.Store(true)
	timer := time.NewTimer(p.c.DrainTimeout)
	defer timer.Stop()
	select {
	case <-done:
		select {
		case err := <-errorsCh:
			return err
		default:
			return nil
		}
	case <-timer.C:
		stopExecutions() // no lease release or synthetic completion while a handler may still run
		return ErrDrainTimeout
	}
}

func (p *Pool) slot(intake, executions context.Context) error {
	q := jobs.DispatchClaimRequest{Owner: p.c.ID, Types: p.registry.types(), LeaseDuration: p.c.LeaseDuration, AttemptTimeout: p.c.AttemptTimeout}
	for intake.Err() == nil {
		started := time.Now()
		opCtx, cancel := context.WithTimeout(intake, p.c.IOTimeout)
		claim, err := p.repo.ClaimDispatched(opCtx, q)
		cancel()
		if intake.Err() != nil {
			return nil
		}
		if errors.Is(err, jobs.ErrNotClaimable) {
			timer := time.NewTimer(p.c.PollInterval)
			select {
			case <-intake.Done():
				timer.Stop()
				return nil
			case <-timer.C:
			}
			continue
		}
		if err != nil {
			p.failed.Add(1)
			return errors.New("worker claim failed; inspect database/ownership state")
		}
		p.active.Add(1)
		err = p.execute(executions, claim, started)
		p.active.Add(-1)
		if err != nil {
			p.failed.Add(1)
			return err
		}
	}
	return nil
}

func (p *Pool) execute(parent context.Context, c jobs.Claim, claimStarted time.Time) error {
	registration, ok := p.registry[c.Type]
	if !ok {
		return errors.New("claimed unregistered job type")
	}
	// Use DB-relative durations on the local monotonic clock, anchored BEFORE
	// claim I/O. This is conservative under network latency and host clock skew.
	deadline := claimStarted.Add(c.Deadline.Sub(c.AcquiredAt))
	leaseUntil := claimStarted.Add(c.LeaseExpiresAt.Sub(c.AcquiredAt))
	ctx, cancel := context.WithDeadline(parent, deadline)
	defer cancel()
	ctx, loseOwnership := context.WithCancelCause(ctx)
	defer loseOwnership(nil)
	if !time.Now().Before(leaseUntil) || ctx.Err() != nil {
		return jobs.ErrLeaseLost
	}
	guard := time.AfterFunc(time.Until(leaseUntil), func() { loseOwnership(jobs.ErrLeaseLost) })
	defer guard.Stop()
	stopHeartbeat := make(chan struct{})
	heartbeatDone := make(chan struct{})
	go func() { defer close(heartbeatDone); p.renew(ctx, stopHeartbeat, c, guard, loseOwnership) }()
	result := invoke(ctx, registration.Handler, c) // same fixed slot; no goroutine per queued notification
	close(stopHeartbeat)
	<-heartbeatDone
	if ctx.Err() != nil {
		return errors.New("worker execution canceled; ownership left for recovery")
	}
	// Guard remains active through completion. Never return a connection or a
	// transaction to a handler, and never complete after an uncertain renewal.
	q := completion(c, registration, result)
	opCtx, stop := context.WithTimeout(ctx, p.c.IOTimeout)
	defer stop()
	if err := p.repo.Complete(opCtx, q); err != nil {
		return errors.New("worker completion failed; inspect authoritative state")
	}
	p.completed.Add(1)
	p.log.Info("job attempt recorded", "job_id", c.JobID, "attempt", c.Attempt, "outcome", q.Outcome)
	return nil
}

func (p *Pool) renew(execution context.Context, stop <-chan struct{}, c jobs.Claim, guard *time.Timer, lose context.CancelCauseFunc) {
	ticker := time.NewTicker(p.c.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-execution.Done():
			return
		case <-ticker.C:
			started := time.Now()
			opCtx, cancel := context.WithTimeout(execution, p.c.IOTimeout)
			_, err := p.heartbeats.Renew(opCtx, c.Ownership, p.c.LeaseDuration)
			cancel()
			if err != nil {
				lose(err)
				return
			}
			if execution.Err() != nil {
				return
			}
			// The attempt deadline remains a separate context deadline. Successful
			// renewals are valid at least from request start for LeaseDuration,
			// except at that fixed deadline, which always cancels independently.
			guard.Reset(time.Until(started.Add(p.c.LeaseDuration)))
		}
	}
}
