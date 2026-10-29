# Architecture proposal — Phase 0

Status: target design with the submission slice implemented in Milestone 1
under the user's explicit revised scope ([issue #3](https://github.com/su944/distributed-job-engine/issues/3)).
M3 implements CDC and durable notification receipt. M4 adds atomic ownership and
bounded registered-handler execution; retry scheduling and recovery remain proposals. See the
[diagrams](architecture-diagram.md), [database design](database-design.md),
[failure analysis](failure-scenarios.md), and [plan](implementation-plan.md).

## Scope and guarantees

Accept durable jobs, dispatch eligible jobs, run bounded handlers, recover lost
owners, and expose durable status. Execution is at least once. Under available
dependencies, finite outages, and runnable handlers, eligible jobs eventually
progress. Ordering across jobs is not guaranteed. No workflow DAGs, cron,
priorities, cancellation, arbitrary user code, or multi-region ownership in v1.

PostgreSQL is the authority for job state, attempt budgets, due times, and
ownership. An accepted response means submission committed, not execution
completed. Kafka records are replaceable wake-ups; they confer no ownership.

## Components and responsibilities

- **REST API:** validate registered job types, payload size, retry policy, and
  idempotency key; commit job plus initial outbox event atomically; read status.
  Never publish directly to Kafka. Proposed deployment assumes a single trusted
  namespace behind authenticated ingress; public/multi-tenant use requires review.
- **PostgreSQL:** persist jobs, attempts, and append-only outbox inserts. Serialize
  per-job transitions through short row-locking transactions. Use database time.
- **Debezium on Kafka Connect:** capture committed outbox inserts from logical
  replication; route `JobReady` envelopes to Kafka. Snapshots and connector
  recovery may repeat events. No application-level publisher competes with CDC.
- **Kafka:** retain wake-ups in `jobs.ready.v1`, keyed by job UUID, with a shared
  worker consumer group. Partition order aids dispatch but is not correctness.
- **Worker:** reserve capacity, conditionally claim in PostgreSQL, then execute
  outside transactions. Heartbeat the lease, enforce handler timeout, and commit
  the result only with the current unexpired fencing token.
- **Recovery scheduler:** bounded database scans promote due retries, recover
  expired leases, and republish overdue READY jobs. Multiple schedulers use
  `FOR UPDATE SKIP LOCKED`; no singleton leader is required.
- **Handlers/external services:** accept stable per-job side-effect keys; report
  success, permanent failure, retryable failure, or unknown outcome. Provider
  reconciliation is part of each handler's contract.

Proposed Go layout: `cmd/api`, `cmd/worker`, `cmd/scheduler`; implementation in
`internal/` packages for jobs, storage, dispatch, handlers, and telemetry. These
worker and scheduler directories remain deferred.

### Milestone 1 scope adjustment

The original plan deferred persistence/API to M2 and infrastructure to M3. The
user explicitly requested them in M1, together with HTTP health, timeouts, and
shutdown. Only PostgreSQL Compose is introduced: real concurrency tests require
it. The implementation retains atomic job/outbox submission without adding CDC
or publishing. Jobs remain READY indefinitely until later dispatch milestones.

Configured `JOB_TYPES` names are an admission allow-list, not installed handlers;
all jobs default to `replay_safe=false`. Authentication remains an ingress
responsibility; the API and PostgreSQL bind to loopback for development. The
single configured namespace is trusted server configuration, never a client header.
Future attempt/audit tables and scheduler indexes are deferred until needed.

## Dispatch and bounded execution

### Current M3 dispatch boundary

The 2026-10-29 Part 2 request authorizes the PostgreSQL → Debezium → Kafka
pipeline and its verification only. Existing uncommitted dispatcher work is
preserved; Part 3 remains gated on review. The router now uses an explicit
source-topic predicate so heartbeat records bypass it; event IDs are retained
in the Kafka `id` header and the existing v1 payload. See
[Part 2 procedure and evidence](milestone-3-part-2.md).

The explicit M3 request authorizes this scope after the merged M2 implementation
and successful revalidation. PR #8 remains an open documentation review; no earlier
acceptance issue is closed. See [issue #9](https://github.com/su944/distributed-job-engine/issues/9).

Kafka Connect/Debezium routes committed outbox INSERTs (and initial snapshot rows)
to `jobs.ready.v1`, keyed by aggregateid. A separate `cmd/dispatcher` consumes in
group `job-engine-dispatch-v1`. Before synchronous offset acknowledgement, it
commits either an immutable `dispatch_events` receipt or `dispatch_quarantine`.
It does not claim jobs, change status/attempts, or execute handlers. Receipts are
durable notifications, not completion evidence. M4 must explicitly define the
claim handoff from retained receipts or a separate worker consumer group; it
cannot assume this group's already committed offsets represent runnable claims.

This adds database writes per notification to make M3 acknowledgement durable.
An in-memory log or Kafka-only acknowledgement would lose the notification's
application outcome on a crash. PostgreSQL quarantine avoids coupling malformed
record handling to a second Kafka topic's availability. Neither choice promises
exactly-once delivery. Duplicate envelope IDs compare a canonical envelope hash;
conflicting ID reuse is quarantined rather than replacing the original receipt.

Processing is sequential: PollRecords(1), one concurrent 1 MiB fetch, a 2 MiB
broker-read limit and 1 MiB decompression limit. Broker batches are capped at
1 MiB; valid envelopes at 8 KiB. Each store call and offset commit has a 5-second
deadline; rebalances are blocked only around this bounded record handling and
released afterward. Storage/commit/fatal fetch failures stop the instance with
no in-memory retry queue; restart replays safely. Offset-out-of-range stops
consumption instead of silently skipping a retention gap.

Health separates process liveness from assignment + database + broker readiness.
Low-cardinality counters expose received, duplicate, quarantine, error outcomes
and assigned partitions. Local Compose is a pinned, single-node development
stack, not a production HA deployment. See the [CDC runbook](cdc-runbook.md) for
versions, credentials, replay, WAL retention and operational limits.

### M4 Part 1: ownership repository only

M3 PR #10 was merged at `b5d2856`; the user's merge confirmation clears the gate
for [M4 Part 1, issue #11](https://github.com/su944/distributed-job-engine/issues/11).
`jobs.JobClaimer`, `LeaseRenewer` and `JobCompleter` are narrow internal contracts
implemented by the PostgreSQL repository. None is wired into Kafka or HTTP.
They lock a namespace-scoped job row in a short READ COMMITTED transaction,
then evaluate database wall-clock time and atomically update job/attempt history.
No execution runs in a transaction. Completion supports success, permanent
failure and unknown outcome requiring reconciliation. Retry policy and recovery,
worker execution and Kafka handoff remain deferred. See
[the Part 1 design and verification](milestone-4-part-1.md).

### Implemented M4 durable handoff and bounded execution

The user authorized full M4 under [issue #12](https://github.com/su944/distributed-job-engine/issues/12),
including reuse of Part 1 commit `55e26b8` if available. The new branch preserves
that prerequisite on merged M3. The chosen handoff uses `dispatch_events` as a
durable inbox: each idle execution slot selects one registered, due READY job
with a receipt using FOR UPDATE SKIP LOCKED, then invokes the shared atomic claim
and history insert. Receipt rows have no consumed flag; job state is authoritative.
Kafka polling, offset commits and rebalance code remain unchanged in the dispatcher.

This adds bounded SQL polling/indexes in exchange for avoiding another Kafka
consumer group or a volatile handoff. A receipt committed before a process crash
remains claimable after restart even when Kafka already acknowledged the record.
Duplicate receipts cannot claim RUNNING/terminal jobs. An abandoned RUNNING job
still requires M5 recovery; this does not provide eventual execution after every
crash until recovery exists. Fixed goroutines reserve capacity before claiming;
handlers run outside transactions with renewal on a separate bounded DB pool.
Only the side-effect-free demo.echo handler is installed. See
[design, limits and actual checks](milestone-4-worker-pool.md).

### Future Kafka-worker handoff alternative (not implemented)

The paragraphs below describe the earlier direct Kafka-worker proposal. M4 uses
the durable inbox drain above instead; no worker Kafka offsets are introduced.

Reserve one of N worker slots before claiming. Pause assigned partitions when
slots are full, continue required consumer polling, and bound fetch buffers.
After a claim commits, a record is handled for dispatch purposes; commit only the
highest contiguous handled offset per partition. A DB-confirmed stale/noneligible
event is also handled. DB errors leave offsets uncommitted. A crashed claimed
job is recovered from its lease even if its Kafka offset was committed.

Rebalances stop intake from revoked partitions; already claimed jobs remain
owned through PostgreSQL leases and may finish. Offset commits must respect the
current consumer generation. Malformed records require durable quarantine before
offset advancement; quarantine failure pauses that partition and raises an alert.
The quarantine mechanism and chosen Go client's polling behavior require review.

Initial proposed limits: N=16 executions per worker, maximum 64 buffered events,
30-second leases, heartbeat every 10 seconds, 5-minute handler timeout, 20-second
shutdown drain. These are tunable proposals, not throughput promises. Heartbeats
cannot extend beyond the attempt deadline. Use separate bounded heartbeat/DB
capacity so saturated handlers cannot starve renewals.

## Lifecycle and retries

States: `READY`, `RUNNING`, `RETRY_WAIT`, `SUCCEEDED`, `FAILED`, `RECONCILE`.
Submission creates READY. Claim increments attempt count and token. Successful
completion is terminal. A known permanent error or exhausted budget becomes
FAILED. Retryable failure schedules RETRY_WAIT; the scheduler promotes it to
READY with a new outbox event. Expired ownership uses the same policy, but an
unresolved external outcome enters RECONCILE unless the handler is explicitly
safe to replay. Unknown handler categories default to reconciliation.

Use capped exponential full jitter: random delay from zero to
`min(5 minutes, 1 second * 2^(attempt-1))`, with overflow-safe arithmetic and a
bounded attempt budget (default 5, maximum 20). Retry-After may raise the delay
within an approved maximum. RECONCILE consumes no automatic execution attempts.
Operator reconciliation records evidence and either proves success, proves
failure, or authorizes RETRY_WAIT with budget remaining; no public retry endpoint
is proposed in v1. Terminal jobs are immutable.

## Proposed REST contract

Submission and status endpoints now run locally; UUIDs below are example IDs.
The SUCCEEDED example illustrates future execution: M1 only creates READY jobs.

```http
POST /v1/jobs HTTP/1.1
Content-Type: application/json
Idempotency-Key: invoice-2026-10-28-001

{"type":"invoice.send","payload":{"invoice_id":"inv_001"},"max_attempts":5}
```

```http
HTTP/1.1 202 Accepted
Location: /v1/jobs/0b4027ce-0686-4191-8e77-d3bc5a9d944d
Content-Type: application/json

{"id":"0b4027ce-0686-4191-8e77-d3bc5a9d944d","status":"READY","attempts":0}
```

Same key and canonical request returns `200 OK` with the original job's current
representation and Location. Same key with different type, payload, or retry
policy returns `409 Conflict` with `{"error":{"code":"idempotency_conflict"}}`.
Missing key, invalid JSON/type, or policy returns `400`; payload over proposed
256 KiB limit returns `413`; bounded admission rejection returns `429` with
Retry-After; unavailable DB returns `503`. An ambiguous commit returns `503` with
`submission_outcome_unknown`; clients retry the same key, never a fresh key.

```http
GET /v1/jobs/0b4027ce-0686-4191-8e77-d3bc5a9d944d HTTP/1.1
```

```http
HTTP/1.1 200 OK
Content-Type: application/json

{"id":"0b4027ce-0686-4191-8e77-d3bc5a9d944d","status":"SUCCEEDED","attempts":2,"max_attempts":5,"created_at":"2026-10-28T10:00:00Z","updated_at":"2026-10-28T10:00:15Z","result":{"receipt_id":"r_001"}}
```

Unknown ID returns `404`. Poll responses expose safe error codes and next due
time when execution is implemented, not raw payloads, secrets, tokens, or provider errors. Example trajectory:
`READY -> RUNNING (attempt 1) -> RETRY_WAIT -> READY -> RUNNING (attempt 2) -> SUCCEEDED`.

M1 makes the limit concrete as 256 KiB for the entire request body. Payload must
be a JSON object; unknown fields, duplicate keys at any depth, invalid UTF-8,
NUL characters, nesting beyond 64, numeric strings longer than 1024 bytes, and exponents outside
[-1000,1000] are rejected. Object key ordering, whitespace, equivalent decimal
payload numbers, and omitted/default max_attempts are canonicalized without
float64 rounding. `max_attempts` itself must be an integer token between 1 and 20.
Submission keys are 1–128 printable ASCII bytes excluding spaces and are opaque.
Content-Type must be application/json (415 otherwise). Malformed IDs return 400,
unknown routes 404, unsupported methods 405, and all handled responses are JSON.
Readiness checks the migration ledger and both submission tables through PostgreSQL
without scanning their rows; liveness is independent
of it. Admission caps active job requests at 64; excess requests receive 429.

### Milestone 2 reliability hardening

The user authorized a review of merged M1 PR #4 and focused fixes in
[issue #5](https://github.com/su944/distributed-job-engine/issues/5), not new
execution components. M2 preserves the submission transaction, SHA-256 canonical
format, default policy, SQL migrations, indexes, and single database/sql pool.
Golden hash vectors guard compatibility with persisted requests across upgrades.

Context cancellation and deadlines map to 503 `request_canceled` and
`request_timeout`, including body reads; uncertain COMMIT classification takes
precedence. A disconnected client may not receive a response at all and must
retry the original key. Readiness detects missing submission relations but is
not a full catalog/constraint drift audit. No additional infrastructure is added.

## Decisions and alternatives

1. **CDC over normal database polling:** transactional outbox closes the dual
   write gap and CDC offers low-latency dispatch without continuous hot-table
   polling. Polling is simpler operationally and remains a valid alternative for
   low volume; CDC adds Connect, replication-slot, and Kafka operations. The
   recovery scheduler polls bounded exceptional/due work, not a second executor.
2. **PostgreSQL ownership:** atomic row updates coordinate Kafka replays, multiple
   workers, schedulers, and API reads. Kafka partition assignment alone cannot
   fence a paused worker or atomically update job history.
3. **Kafka delivery versus completion:** offsets acknowledge dispatch handling.
   Durable completion requires a fenced database transition. Kafka transactions
   cannot atomically commit arbitrary external HTTP side effects.
4. **Scheduler retained:** delayed retries and expired owners exist independently
   of event delivery. READY republishing repairs lost notifications and connector
   rebuild gaps; it does not bypass a broken Kafka path by running handlers.
5. **At least once:** stable side-effect keys and provider reconciliation mitigate
   duplicates. Exactly-once external outcomes cannot be promised without provider
   cooperation. A database fence prevents stale DB writes, not an HTTP call.
6. **Bounded concurrency:** fixed slots, bounded buffers, bounded scans, retry
   jitter, and admission controls protect dependencies. Unlimited goroutines and
   immediate requeueing would amplify outages.

## Operations, security, and open decisions

Emit structured `slog` logs with job/event/attempt IDs; no payloads or secrets.
Measure READY age, retry backlog, expired leases, claim conflicts, execution
latency, active slots, Kafka lag, CDC lag, WAL bytes retained, reconciliation
backlog, and quarantine count. Trace submission through event IDs and attempts;
avoid job IDs as metric labels. Distinguish liveness from dependency readiness.

Use least-privilege API, worker, scheduler, and replication identities; TLS and
secret injection. Backup/restore, connector offset/slot recovery, outbox pruning,
and coordinated restore/replay need tested runbooks before production.

Review must settle: supported versions and Go Kafka client; authentication and
namespace model; handler side-effect contracts; quarantine storage; retention
and idempotency lifetime; SLO/load envelope; lease/timeout tuning; payload/result
limits; and whether deployment targets require stronger PostgreSQL durability
than primary-local synchronous commits. No capacity or cross-region SLA is yet
claimed.

## Primary references

- [PostgreSQL locking clauses](https://www.postgresql.org/docs/current/sql-select.html#SQL-FOR-UPDATE-SHARE): basis for bounded queue-like scans with SKIP LOCKED.
- [PostgreSQL replication slots](https://www.postgresql.org/docs/17/warm-standby.html#STREAMING-REPLICATION-SLOTS): retained WAL requires monitoring and limits.
- [Debezium outbox event router](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html): event IDs and aggregate keys support this proposed envelope.
- [Kafka delivery semantics](https://kafka.apache.org/design/): Kafka delivery guarantees do not encompass arbitrary external side effects.
