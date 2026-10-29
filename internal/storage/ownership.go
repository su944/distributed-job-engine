package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/su944/distributed-job-engine/internal/jobs"
)

var (
	_ jobs.JobClaimer        = (*Repository)(nil)
	_ jobs.LeaseRenewer      = (*Repository)(nil)
	_ jobs.JobCompleter      = (*Repository)(nil)
	_ jobs.DispatchedClaimer = (*Repository)(nil)
)

// Ownership operations have a five-second total budget including pool waits.
// Lock waits have a shorter server-side bound. All paths release the transaction.
func (r *Repository) beginOwnership(ctx context.Context) (*sql.Tx, error) {
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin ownership: %w", err)
	}
	ok := false
	defer func() {
		if !ok {
			_ = tx.Rollback()
		}
	}()
	if _, err := tx.ExecContext(ctx, `SET LOCAL lock_timeout = '1s'; SET LOCAL statement_timeout = '4s'`); err != nil {
		return nil, fmt.Errorf("bound ownership waits: %w", err)
	}
	ok = true
	return tx, nil
}

func (r *Repository) lockOwnership(ctx context.Context, id string) (*sql.Tx, error) {
	tx, err := r.beginOwnership(ctx)
	if err != nil {
		return nil, err
	}
	var found string
	err = tx.QueryRowContext(ctx, `SELECT id::text FROM jobs WHERE namespace=$1 AND id=$2 FOR UPDATE`, r.namespace, id).Scan(&found)
	if errors.Is(err, sql.ErrNoRows) {
		tx.Rollback()
		return nil, jobs.ErrNotFound
	}
	if err != nil {
		tx.Rollback()
		return nil, fmt.Errorf("lock ownership: %w", err)
	}
	return tx, nil
}

func commitOwnership(tx *sql.Tx) error {
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit ownership: %w", errors.Join(jobs.ErrOwnershipUnknown, err))
	}
	return nil
}

func (r *Repository) Claim(ctx context.Context, q jobs.ClaimRequest) (jobs.Claim, error) {
	if !jobs.ValidID(q.JobID) || !jobs.ValidOwner(q.Owner) || !jobs.ValidOwnershipDuration(q.LeaseDuration) || !jobs.ValidOwnershipDuration(q.AttemptTimeout) {
		return jobs.Claim{}, jobs.ErrInvalidOwnership
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tx, err := r.lockOwnership(ctx, q.JobID)
	if err != nil {
		return jobs.Claim{}, err
	}
	defer tx.Rollback()
	return r.claimLocked(ctx, tx, q)
}

func (r *Repository) claimLocked(ctx context.Context, tx *sql.Tx, q jobs.ClaimRequest) (jobs.Claim, error) {
	var c jobs.Claim
	var started time.Time
	// This statement starts only AFTER the row lock was acquired. Materialize a
	// single wall-clock sample; transaction/statement-start time before waiting
	// for a lock could grant a lease that has already expired.
	err := tx.QueryRowContext(ctx, `WITH t AS MATERIALIZED (SELECT clock_timestamp() AS now)
        UPDATE jobs SET status='RUNNING', attempt_count=attempt_count+1, lease_token=lease_token+1,
            lease_owner=$3, lease_expires_at=t.now+LEAST($4::bigint,$5::bigint)*interval '1 microsecond',
            attempt_deadline=t.now+$5::bigint*interval '1 microsecond', updated_at=t.now
        FROM t WHERE namespace=$1 AND id=$2 AND status='READY' AND available_at<=t.now
            AND attempt_count<max_attempts AND lease_token<9223372036854775807
        RETURNING id::text,lease_owner,lease_token,attempt_count,type,payload,lease_expires_at,attempt_deadline,updated_at`,
		r.namespace, q.JobID, q.Owner, q.LeaseDuration.Microseconds(), q.AttemptTimeout.Microseconds()).Scan(
		&c.JobID, &c.Owner, &c.LeaseToken, &c.Attempt, &c.Type, &c.Payload, &c.LeaseExpiresAt, &c.Deadline, &started)
	if errors.Is(err, sql.ErrNoRows) {
		return jobs.Claim{}, jobs.ErrNotClaimable
	}
	if err != nil {
		return jobs.Claim{}, fmt.Errorf("claim job: %w", err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO job_attempts(job_id,attempt_no,lease_token,worker_id,started_at,deadline_at) VALUES($1,$2,$3,$4,$5,$6)`,
		c.JobID, c.Attempt, c.LeaseToken, c.Owner, started, c.Deadline)
	if err != nil {
		return jobs.Claim{}, fmt.Errorf("record claimed attempt: %w", err)
	}
	if err := commitOwnership(tx); err != nil {
		return jobs.Claim{}, err
	}
	c.LeaseExpiresAt = c.LeaseExpiresAt.UTC()
	c.Deadline = c.Deadline.UTC()
	c.AcquiredAt = started.UTC()
	return c, nil
}

func (r *Repository) Renew(ctx context.Context, o jobs.Ownership, duration time.Duration) (time.Time, error) {
	if !o.Valid() || !jobs.ValidOwnershipDuration(duration) {
		return time.Time{}, jobs.ErrInvalidOwnership
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tx, err := r.lockOwnership(ctx, o.JobID)
	if errors.Is(err, jobs.ErrNotFound) {
		return time.Time{}, jobs.ErrLeaseLost
	}
	if err != nil {
		return time.Time{}, err
	}
	defer tx.Rollback()
	var expires time.Time
	err = tx.QueryRowContext(ctx, `WITH t AS MATERIALIZED (SELECT clock_timestamp() AS now)
        UPDATE jobs SET lease_expires_at=LEAST(attempt_deadline,GREATEST(lease_expires_at,t.now+$5::bigint*interval '1 microsecond')), updated_at=t.now
        FROM t WHERE namespace=$1 AND id=$2 AND status='RUNNING' AND lease_owner=$3 AND lease_token=$4
            AND lease_expires_at>t.now AND attempt_deadline>t.now
        RETURNING lease_expires_at`, r.namespace, o.JobID, o.Owner, o.LeaseToken, duration.Microseconds()).Scan(&expires)
	if errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, jobs.ErrLeaseLost
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("renew lease: %w", err)
	}
	if err := commitOwnership(tx); err != nil {
		return time.Time{}, err
	}
	return expires.UTC(), nil
}

func (r *Repository) Complete(ctx context.Context, q jobs.Completion) error {
	if !q.Valid() {
		return jobs.ErrInvalidOwnership
	}
	status := jobs.Succeeded
	switch q.Outcome {
	case jobs.CompletionPermanent:
		status = jobs.Failed
	case jobs.CompletionUnknown:
		status = jobs.Reconcile
	case jobs.CompletionRetryable:
		status = jobs.RetryWait
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tx, err := r.lockOwnership(ctx, q.JobID)
	if errors.Is(err, jobs.ErrNotFound) {
		return jobs.ErrLeaseLost
	}
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var result any
	if len(q.Result) > 0 {
		result = []byte(q.Result)
	}
	var ended time.Time
	var attempt int
	err = tx.QueryRowContext(ctx, `WITH t AS MATERIALIZED (SELECT clock_timestamp() AS now)
        UPDATE jobs SET status=CASE WHEN $5='RETRY_WAIT' AND attempt_count>=max_attempts THEN 'FAILED' ELSE $5 END,
            result=$6, last_error_code=NULLIF($7,''),
            finished_at=CASE WHEN $5 IN ('SUCCEEDED','FAILED') OR ($5='RETRY_WAIT' AND attempt_count>=max_attempts) THEN t.now ELSE NULL END,
            lease_owner=NULL,lease_expires_at=NULL,attempt_deadline=NULL,updated_at=t.now
        FROM t WHERE namespace=$1 AND id=$2 AND status='RUNNING' AND lease_owner=$3 AND lease_token=$4
            AND lease_expires_at>t.now AND attempt_deadline>t.now
        RETURNING attempt_count,updated_at`, r.namespace, q.JobID, q.Owner, q.LeaseToken, status, result, q.ErrorCode).Scan(&attempt, &ended)
	if errors.Is(err, sql.ErrNoRows) {
		return jobs.ErrLeaseLost
	}
	if err != nil {
		return fmt.Errorf("complete job: %w", err)
	}
	updated, err := tx.ExecContext(ctx, `UPDATE job_attempts SET ended_at=$5,outcome=$6,error_code=NULLIF($7,'')
        WHERE job_id=$1 AND attempt_no=$2 AND worker_id=$3 AND lease_token=$4 AND outcome IS NULL AND ended_at IS NULL AND deadline_at>$5`,
		q.JobID, attempt, q.Owner, q.LeaseToken, ended, q.Outcome, q.ErrorCode)
	if err != nil {
		return fmt.Errorf("complete attempt: %w", err)
	}
	n, err := updated.RowsAffected()
	if err != nil {
		return fmt.Errorf("count completed attempts: %w", err)
	}
	if n != 1 {
		return errors.New("active attempt history differs from job ownership")
	}
	return commitOwnership(tx)
}
