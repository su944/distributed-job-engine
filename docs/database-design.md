# Proposed database design

Milestone 1 implements `jobs` and `outbox_events` in migration 001 using pgx/v5
through `database/sql`. Attempts, reconciliation audit, and scheduler indexes
remain proposed until their owning milestones. PostgreSQL is authoritative;
Kafka contains notifications, not the job ledger. All timestamps are `timestamptz`
in UTC. Use UUID application-generated IDs and explicit checks rather than a
database enum to ease later lifecycle evolution.

## Jobs

Proposed `jobs` columns:

- `id uuid PRIMARY KEY`; `namespace text NOT NULL` (fixed trusted namespace in v1).
- `idempotency_key text NOT NULL` (1–128 bytes); `request_hash bytea NOT NULL`
  (SHA-256 of canonical validated type, payload, and effective retry policy).
- `type text NOT NULL`, `payload jsonb NOT NULL`, `status text NOT NULL`.
- `attempt_count integer NOT NULL DEFAULT 0`, `max_attempts integer NOT NULL`.
- `replay_safe boolean NOT NULL`, frozen from trusted handler registration;
  clients cannot assert side-effect safety.
- `available_at timestamptz NOT NULL`; `last_dispatched_at timestamptz NOT NULL`.
- `lease_token bigint NOT NULL DEFAULT 0` (monotonic per job, never reset);
  `lease_owner text`, `lease_expires_at timestamptz`, `attempt_deadline timestamptz`.
- `result jsonb`, `last_error_code text`, `reconciliation_note text`;
  `created_at`, `updated_at` nonnull timestamps; `finished_at` nullable timestamp.

Constraints: unique `(namespace, idempotency_key)`; known lifecycle states;
`0 <= attempt_count <= max_attempts`, `1 <= max_attempts <= 20`, token nonnegative.
RUNNING requires nonnull owner, expiry, deadline and positive attempt count;
all other states require those three lease fields null. SUCCEEDED/FAILED require
`finished_at`; other states forbid it. READY/RETRY_WAIT require budget remaining.
Enforce request and result size limits at the API/handler boundary and add DB
size checks in migrations after canonical encoding limits are reviewed.

Partial B-tree indexes proposed:

- `(available_at, id) WHERE status = 'RETRY_WAIT'` for due promotions.
- `(lease_expires_at, id) WHERE status = 'RUNNING'` for expiry recovery.
- `(last_dispatched_at, id) WHERE status = 'READY'` for lost-wakeup repair.
- `(updated_at, id) WHERE status = 'RECONCILE'` for operator backlog.

Primary-key lookup handles claims and status reads. Avoid payload indexes and
extra hot-row indexes until justified by measured queries.

## Attempt history

`job_attempts`: `job_id uuid REFERENCES jobs(id) ON DELETE RESTRICT`,
`attempt_no integer`, `lease_token bigint`, `worker_id text`, `started_at`,
`deadline_at`, nullable `ended_at`, `outcome`, `error_code`, and `provider_ref`.
Primary key `(job_id, attempt_no)`; unique `(job_id, lease_token)`; positive
attempt/token checks; allowed outcomes `SUCCEEDED`, `RETRYABLE`, `PERMANENT`,
`LEASE_EXPIRED`, `UNKNOWN`. An active attempt has null end/outcome; finished
attempts require both. Index `(job_id, started_at)` only if history queries need it.
Attempt writes occur in the same transaction as their job transition. Audit
operator reconciliation with actor, timestamp, evidence reference, and decision
in a proposed append-only `job_reconciliations` table referencing the job.

## Outbox and event envelope

`outbox_events` follows the Debezium router shape: `id uuid PRIMARY KEY`,
`aggregatetype text NOT NULL` fixed to `jobs`, `aggregateid uuid NOT NULL
REFERENCES jobs(id) ON DELETE RESTRICT`, `type text NOT NULL` fixed to
`JobReady`, `timestamp timestamptz NOT NULL`, `payload jsonb NOT NULL`.
Index `(timestamp, id)` supports controlled retention. Every new notification
gets a fresh event ID, including READY repair; CDC replay retains the same ID.
Do not use a unique job ID constraint on outbox rows: jobs need multiple wake-ups.

Logical payload:

```json
{"schema_version":1,"event_id":"78aad7c3-8c81-4a65-88b4-a4ba1e115f77","job_id":"0b4027ce-0686-4191-8e77-d3bc5a9d944d","kind":"JobReady","occurred_at":"2026-10-28T10:00:00Z"}
```

Configure the router to use `aggregateid` as Kafka key, carry event ID, and route
to `jobs.ready.v1`; exact transform settings belong to the infrastructure review.
Avoid putting job payloads in Kafka. Consumers read current job data from DB;
old wake-ups may validly claim a currently eligible READY job.

Outbox rows are insert-only for the application. Pruning is a separate reviewed
maintenance operation, not a `published` flag inferred from time. Retain beyond
confirmed CDC progress and the chosen disaster-recovery window; filter deletion
events appropriately. Monitor replication-slot WAL retention. Never delete
unexported rows just because they are old. If connector state is lost, resnapshot
retained rows and republish current READY jobs; duplicates are expected.

## Transaction boundaries and fencing

### M3 receipt and quarantine transactions

Migration 002 adds `dispatch_events`: UUID event primary key, job foreign key,
version=1, timestamp, 32-byte canonical envelope hash, receipt timestamp. Same ID
and hash is a duplicate; different hash is quarantined. No job columns are updated.
The FK protects references and unknown job IDs are quarantined explicitly.

`dispatch_quarantine` is keyed by source epoch, consumer group, topic, partition,
offset. It stores a safe reason code, full key/value byte counts and SHA-256
hashes, plus prefixes capped at 1 KiB/8 KiB. These bytes may be sensitive: restrict
table access and never log them. Coordinate reuse with different bytes stops
processing; cluster/topic recreation requires a new source epoch and deliberate
replay. Quarantine persistence is idempotent across ambiguous database commits.

Each notification transaction commits one receipt or quarantine before Kafka's
offset+1 is committed. If persistence or its acknowledgement is uncertain, Kafka
is not acknowledged and replay resolves the durable state. A later Kafka commit
failure also replays safely. This is not an atomic PostgreSQL/Kafka transaction.
Migration 001 and submission hashing/SQL remain unchanged. Down migrations, when
explicitly approved for disposable databases, must run 002 before 001.

### Existing submission transaction

Implemented submission uses an explicit READ COMMITTED transaction and a SQL
unique constraint. `ON CONFLICT DO NOTHING` waits for the winner; a separate
SELECT sees that committed winner under a fresh statement snapshot. There is no
check-then-insert race and no no-op UPDATE of an existing job. Matching hashes
return the original current status without another event; conflicts return 409.
Commit errors conservatively return `submission_outcome_unknown`; callers retry
the same key. No automatic transaction replay is performed in M1 or M2.

M2 verifies the uncertain-commit contract against real PostgreSQL: a test-only
protocol wrapper either prevents sending COMMIT or consumes the server's COMMIT
success before reporting EOF to pgx. The same key then creates the missing job or
retrieves the already committed job. In both cases exactly one job/outbox pair
remains, and the pool replaces its broken connection. No production fault hooks
or extra transaction abstraction are introduced.

SQLSTATE assertions cover payload/hash/attempt/state checks and outbox foreign
keys. Representative `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` lookups over 1,000
synthetic rows use `jobs_namespace_key_unique` for submission-key reads and
`jobs_pkey` for status reads. No new indexes or migration edits are warranted by
these query shapes; these small tests are not a production capacity benchmark.

The explicit migration command embeds reviewed SQL, takes a transaction-scoped
advisory lock, and records a checksum in `schema_migrations`. Re-running is safe;
editing applied SQL fails the checksum check. The API does not migrate on startup.
The down SQL is destructive and manual-only; no production rollback command is
provided. No job or outbox cleanup is implemented, preserving idempotency keys.

The remaining ownership transitions below are target behavior, not M1 code.

Use short READ COMMITTED transactions, lock jobs before attempt rows, and retry
bounded deadlock/serialization errors. Evaluate database time after acquiring the
row lock (`clock_timestamp()`); transaction-start time is unsafe after lock waits.
No handler/network side effect runs inside a database transaction.

1. **Submit:** resolve the unique idempotency key with insert-on-conflict, then
   compare canonical hash against the committed row. Winner inserts job and first
   outbox row in one transaction. Conflicting requests return 409; identical
   requests return the existing job. Roll back everything on any outbox error.
2. **Claim:** lock job by ID. Require READY, due time reached, and attempts below
   budget. Increment attempt count and lease token; set owner, deadline, and
   expiry to `min(now + lease_duration, deadline)`; insert attempt; commit.
   A zero-match claim is a safe no-op, not permission to execute.
3. **Renew:** lock and require RUNNING, owner/token match, lease strictly after
   DB now, and deadline in the future. Extend only up to deadline. A failed or
   ambiguous renewal stops new effects and cancels the handler until ownership
   is positively established; do not renew an already expired lease.
4. **Finish:** lock and require the same owner/token and unexpired lease/deadline.
   Update attempt plus job atomically; clear lease fields. Success -> SUCCEEDED;
   permanent/exhausted known failure -> FAILED; safe transient failure ->
   RETRY_WAIT with jitter; unknown unsafe outcome -> RECONCILE even at budget
   exhaustion. Reject stale results. Re-read after an ambiguous commit before
   retrying any transition; never rerun an effect to resolve a DB ambiguity.
5. **Recover:** bounded `FOR UPDATE SKIP LOCKED` batches recheck expiry after
   locking. Close the active attempt; increment token to invalidate the owner;
   clear lease fields. Replay-safe expired jobs -> RETRY_WAIT or FAILED if
   exhausted. Unsafe expired jobs -> RECONCILE. Conservative quarantine includes
   crashes before any provider call because the DB cannot prove no call occurred.
6. **Promote:** lock due RETRY_WAIT rows; set READY and insert a new outbox row in
   the same transaction; update dispatch timestamp. Never emit future retries early.
7. **Repair:** lock READY rows older than the proposed 60-second dispatch interval,
   insert new wake-ups, and advance dispatch timestamp in one transaction. Bound
   batch size (proposed 100), cadence, and event rate to avoid outage amplification.
8. **Reconcile:** audited privileged operation locks RECONCILE row and records
   evidence; proven success -> SUCCEEDED, permanent failure -> FAILED, safe retry
   with remaining budget -> RETRY_WAIT. No direct RUNNING transition or budget
   reset. Corrections to terminal jobs require a separately reviewed mechanism.

A token fences database writes only. Provider keys use stable `job_id + logical
effect name`, never attempt number or lease token. Token-aware external systems
may additionally reject older tokens. Idempotency rows must outlive client retry
windows and provider deduplication windows; until retention is agreed, do not
delete jobs or reuse submission keys. No exactly-once claim is made for effects.
