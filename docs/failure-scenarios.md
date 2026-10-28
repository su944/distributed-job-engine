# Failure scenarios and verification obligations

Worker/dispatch scenarios remain proposed behavior and future tests. M1/M2
submission checks have executed coverage recorded in the milestone verification
documents. Each
critical transition requires real PostgreSQL concurrency coverage. See
[transaction boundaries](database-design.md) for authoritative guards.

## API crash and ambiguous submission

- **Before commit:** neither job nor outbox persists, including a crash between
  inserts. Inject a process failure before COMMIT and verify both absent.
- **After commit, before response:** job and event persist. Retry the same key;
  assert one job and identical identity, including simultaneous submissions.
- **Commit response lost:** return an unknown-submission error if possible;
  lookup/retry using the same key. Never compensate by deleting the job.
- **Key conflict:** a different canonical request returns 409 without a new event.

M2 tests the submission ambiguity using real PostgreSQL protocol I/O: suppress
COMMIT before sending, or hide the server's successful reply. Both return 503
`submission_outcome_unknown`; retrying the original key leaves one job/event pair.
This does not simulate operating-system process death, server failover, or power
loss. Existing outbox-write failure coverage separately proves atomic rollback.

M2 also observes an actual unique-key lock wait before canceling its request,
verifying cancellation returns promptly without a waiter job/event. Exhausting
a one-connection pool produces `request_timeout`; releasing the held connection
allows a new submission and repeated replays/conflicts without a transaction leak.
Cancellation before/during SQL is not evidence that an already-started COMMIT
failed; the uncertain-commit error classification always takes precedence.

## Debezium or Kafka outage

Committed jobs remain READY. CDC resumes from durable offsets; replay is allowed.
Monitor connector state, oldest READY age, Kafka lag, outbox growth, slot-retained
WAL, and disk. Bound scheduler re-notification rates and reject new admissions
when reviewed disk/backlog thresholds are crossed. Service can accept only while
durability capacity is safe; it must not promise execution latency during outage.
Test stop/resume and connector restart, verifying eventual dispatch without lost
jobs. Lost WAL/slot state requires the resnapshot/READY repair runbook, not blind
offset advancement. Backup restoration must reconcile DB and external outcomes.

## Duplicated, stale, malformed, or missing events

Repeated notifications race on a conditional DB claim; only one current attempt
owns the row. A notification for terminal/RUNNING/RETRY_WAIT is a no-op. DB errors
are retried without committing past the affected Kafka offset. Test reordered
notifications, duplicate offsets, concurrent workers, and contiguous offset
tracking when processing finishes out of order. Malformed/unsupported versions
go to durable quarantine before offset advancement; failures pause the partition.
No notification can strand a READY job forever after dependencies recover:
scheduler repair produces another wake-up. Quarantine design is a review gate.

## Worker crash, lease expiry, and stale workers

Crash before claim commit leaves no owner. Crash after commit (including after
Kafka acknowledgement) leaves RUNNING until expiry. Recovery closes the attempt,
invalidates its token, and applies safe retry or reconciliation policy.

Pause worker A beyond expiry, recover and claim with B, then resume A: A's renewal
and completion must update zero rows. Test exact expiry boundary and lock waits
using DB time. A may still make an external call after losing ownership; DB
fencing cannot prevent that. Stable provider keys or explicit provider fencing
are required. Network partition/ambiguous heartbeat cancels work conservatively;
if cancellation is ignored, isolate/terminate the handler process operationally.

## Retry storms and dependency saturation

All workers share capped jittered retry policy and finite attempt budgets.
Schedulers batch with SKIP LOCKED, while worker slots, fetch buffers, DB pools,
and submission admission remain bounded. Test many simultaneous failures and
lease expiries; assert retry due times are spread, no attempt budget is exceeded,
and active handlers never exceed configured concurrency. Measure scheduler lag
and DB pressure; no unbounded goroutine per event. Circuit breaking or separate
per-handler quotas require later evidence and review, not new infrastructure now.

## Unknown external side-effect outcome

Provider accepts a request but its response is lost, or worker dies before
recording success. For registered replay-safe handlers, retry using the identical
logical effect key and reconcile provider receipts. For other handlers, use
RECONCILE and alert an operator. Do not infer failure from timeout or exhausted
budget. Test provider-applied/response-dropped and provider deduplication expiry.
Document provider retention and lookup semantics before enabling automatic retry.
An operator must record evidence before resolving or retrying an uncertain job.

## Database loss and shutdown

DB outage prevents claims and authoritative completion. Pause intake, bound
retries, cancel handlers when lease certainty is lost, and reconcile ambiguous
commits when DB returns. A non-durable PostgreSQL failover can lose acknowledged
work; replication/backup durability targets must be approved before production.

On SIGTERM, fail readiness and stop submissions/intake/scans; let in-flight DB
transactions settle within deadlines. Keep renewing draining attempts until the
drain deadline. Finish completed handlers with normal fences, then cancel the
rest and stop renewals. Never immediately release a lease while a handler could
still execute effects. Expiry recovery handles unfinished attempts. Test shutdown
during claim, provider call, completion commit, and Kafka rebalance; ensure the
process exits within its shutdown budget and uncertain effects are reconciled.
