package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/su944/distributed-job-engine/internal/jobs"
)

// ClaimDispatched drains the durable receipt inbox. A fixed worker slot invokes
// it only when idle; neither a Kafka replay nor a process restart needs an
// in-memory notification. The job row, not a receipt or Kafka partition, owns work.
func (r *Repository) ClaimDispatched(ctx context.Context, q jobs.DispatchClaimRequest) (jobs.Claim, error) {
	if !jobs.ValidOwner(q.Owner) || !jobs.ValidOwnershipDuration(q.LeaseDuration) || !jobs.ValidOwnershipDuration(q.AttemptTimeout) || len(q.Types) == 0 || len(q.Types) > 128 {
		return jobs.Claim{}, jobs.ErrInvalidOwnership
	}
	for _, typ := range q.Types {
		if !jobs.ValidType(typ) {
			return jobs.Claim{}, jobs.ErrInvalidOwnership
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tx, err := r.beginOwnership(ctx)
	if err != nil {
		return jobs.Claim{}, err
	}
	defer tx.Rollback()
	var id string
	err = tx.QueryRowContext(ctx, `SELECT j.id::text FROM jobs j
        WHERE j.namespace=$1 AND j.type=ANY($2::text[]) AND j.status='READY'
            AND j.available_at<=clock_timestamp() AND j.attempt_count<j.max_attempts
            AND j.lease_token<9223372036854775807
            AND EXISTS(SELECT 1 FROM dispatch_events e WHERE e.job_id=j.id)
        ORDER BY j.available_at,j.id LIMIT 1 FOR UPDATE OF j SKIP LOCKED`, r.namespace, q.Types).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return jobs.Claim{}, jobs.ErrNotClaimable
	}
	if err != nil {
		return jobs.Claim{}, fmt.Errorf("select dispatched job: %w", err)
	}
	// Re-evaluate eligibility and database time after locking; share the exact
	// claim-by-ID transition and atomic history insert instead of duplicating it.
	return r.claimLocked(ctx, tx, jobs.ClaimRequest{JobID: id, Owner: q.Owner, LeaseDuration: q.LeaseDuration, AttemptTimeout: q.AttemptTimeout})
}

func (r *Repository) WorkerReady(ctx context.Context) error {
	var version int
	return r.db.QueryRowContext(ctx, `SELECT version FROM schema_migrations WHERE version=4
		AND NOT EXISTS(SELECT id FROM jobs WHERE false)
        AND NOT EXISTS(SELECT job_id FROM dispatch_events WHERE false)
        AND NOT EXISTS(SELECT job_id FROM job_attempts WHERE false)`).Scan(&version)
}
