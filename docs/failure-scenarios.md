# Failure scenarios and verification obligations

Submission, CDC/dispatch, atomic ownership and bounded worker checks now have
executed coverage in their milestone verification documents. Recovery/scheduler
and external-provider scenarios remain proposed behavior and future tests. Each
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

M3 stops unacknowledged dispatch on persistence/commit/fatal-fetch errors; the
client's bounded broker retries may wait while readiness fails. Restart the
dispatcher after repairing dependencies. Durable receipts deduplicate event IDs;
durable quarantine makes malformed records acknowledgeable. The current M3
consumer does not execute or claim a job, and scheduler repair is not implemented.
See [CDC operations](cdc-runbook.md) before attempting slot or offset recovery.

Committed jobs remain READY. CDC resumes from durable offsets; replay is allowed.
Monitor connector state, oldest READY age, Kafka lag, outbox growth, slot-retained
WAL, and disk. Bound scheduler re-notification rates and reject new admissions
when reviewed disk/backlog thresholds are crossed. Service can accept only while
durability capacity is safe; it must not promise execution latency during outage.
Test stop/resume and connector restart, verifying eventual dispatch without lost
jobs. Lost WAL/slot state requires the resnapshot/READY repair runbook, not blind
offset advancement. Backup restoration must reconcile DB and external outcomes.

## Duplicated, stale, malformed, or missing events

Implemented M4 uses persisted receipts as a durable inbox, rather than passing
Kafka records into execution channels. A receipt committed before claim is
retained across crashes and found by replacement worker slots. Rebalances affect
dispatcher membership, not database-owned handler contexts. Actual Kafka tests
verify offset-after-receipt, replay and rebalance isolation. Child-process tests
SIGKILL before claim (replacement executes) and after claim (RUNNING persists),
and SIGTERM while a handler drains. See [M4 evidence](milestone-4-worker-pool.md).
The direct Kafka-worker proposal below remains an alternative, not current code.

Repeated notifications race on a conditional DB claim; only one current attempt
owns the row. A notification for terminal/RUNNING/RETRY_WAIT is a no-op. DB errors
are retried without committing past the affected Kafka offset. Test reordered
notifications, duplicate offsets, concurrent workers, and contiguous offset
tracking when processing finishes out of order. Malformed/unsupported versions
go to durable quarantine before offset advancement; failures pause the partition.
No notification can strand a READY job forever after dependencies recover:
scheduler repair produces another wake-up. Quarantine design is a review gate.

## Worker crash, lease expiry, and stale workers

M4 now installs a fixed-capacity worker with active renewal and conservative
cancellation on renewal failure/uncertainty. Forced drain cancels the context
without releasing ownership, even when an intentionally uncooperative handler
continues. The command treats timeout as fatal and exits. RETRY_WAIT and expired
RUNNING jobs are intentionally not promoted/recovered yet; M5 is still required.

M4 Part 1's repository tests now verify competing independent database pools,
old-token renewal/completion rejection, expiry during an observed row-lock wait,
atomic rollback on history failure, and cancellation/lock/pool timeout release.
Real protocol faults before COMMIT transmission and after the server commits
verify claim/renew/complete report unknown outcomes without returning ownership
permission or confirmed expiry. This is transport fault injection, not a process
crash or failover test. The newer-attempt fixture simulates a future recovery
transition; no recovery daemon or worker exists yet. See
[actual Part 1 checks](milestone-4-part-1.md).

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
