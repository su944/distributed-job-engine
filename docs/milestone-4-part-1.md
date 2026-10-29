# Milestone 4 Part 1: atomic claiming and fenced ownership

Tracking [issue #11](https://github.com/su944/distributed-job-engine/issues/11).
Branch `feat/milestone-4-atomic-ownership-11`, base merged main `b5d2856`.

## M3 review and approval gate

The user requested M4 conditionally on M3 acceptance. The initial inspection
found M3 unmerged/unaccepted, so work stopped at a technical plan without edits.
The user then confirmed the merge. Fetch and GitHub verification confirmed
[M3 PR #10](https://github.com/su944/distributed-job-engine/pull/10) merged into
main, containing `6af1f0d`. Reviewed AGENTS, architecture, database design,
failure scenarios, implementation plan, dispatcher/store, job model, migrations
and real dependency tests. This review found no prerequisite change needed to
implement the independent ownership repository. Fresh M3 regression tests below
also passed. No prior issue is automatically closed and no production approval
is implied. `.idea/` remains untracked and untouched.

## Design and scope

`internal/jobs/ownership.go` defines separate one-method JobClaimer, LeaseRenewer
and JobCompleter interfaces. The existing storage.Repository implements them
without expanding the submission interface or public JSON Job representation.
Claim returns an internal payload-bearing structure with job ID/type, attempt,
owner/token, expiry and deadline. No HTTP endpoint or dispatcher integration is
added; no production caller starts execution. Job payloads never enter Kafka.

Migration 003 adds attempt history with job FK, job/attempt primary key,
job/token uniqueness, bounded owner, positive attempt/token, start/deadline,
end/outcome pairing and a partial unique index allowing one active attempt per
job. Migrations 001/002 are unchanged. The migration runner checks all three;
down migration 003 is destructive/manual-only and must precede earlier down SQL.

Internal owner identifiers are 1–128 printable ASCII non-space bytes. Durations
are explicit trusted caller policy, from one microsecond through 24 hours at
microsecond precision; there are no newly exposed client policy fields. Result
JSON is optional, object-only and capped at the existing 256 KiB body bound.
Only safe error codes matching the job-type identifier shape are accepted.

## SQL locks, time and transaction boundaries

Every operation obtains a pool connection within a five-second context budget,
begins READ COMMITTED and sets transaction-local lock_timeout=1s and
statement_timeout=4s. The total context bounds multiple statements and pool
waits; caller cancellation/shorter deadlines take precedence. Transaction-local
settings do not contaminate reused pooled connections.

1. `SELECT id FROM jobs WHERE namespace=… AND id=… FOR UPDATE` locks exactly
   the requested job. Primary-key lookup needs no new job index. Waiting rather
   than SKIP LOCKED gives a competing caller the committed state; timeout is an
   I/O error, not a misleading ineligible result.
2. A subsequent statement materializes one `clock_timestamp()` sample. The
   clock is sampled after lock acquisition, avoiding transaction-start time
   from before a potentially long lock wait. Guards and timestamps within the
   update use that same sample.
3. Claim requires READY, available_at <= sampled time, remaining attempt budget,
   and a token below bigint maximum. It increments count/token and sets RUNNING,
   owner, deadline=time+attempt timeout and expiry=min(time+lease,deadline).
4. Insert the matching attempt. Only after COMMIT succeeds return the claim.
   Failure in history insertion rolls back the job update and consumed counters.
5. Renewal similarly locks then checks RUNNING, owner/token, expiry > time and
   deadline > time. New expiry=min(deadline,max(old expiry,time+duration)); renewal
   cannot revive or shorten a lease. History's fixed attempt deadline is unchanged.
6. Completion uses the same identity/time guards, records completion at the guarded
   update time, clears owner/expiry/deadline, retains the fencing token and closes
   exactly the matching active history row. Missing/mismatched history or any SQL
   error rolls back both changes. Locks are always acquired job first, history second.

The guarded job UPDATE is the operation's decision point. COMMIT makes its
atomic result visible; a lease is not promised to remain valid indefinitely
after the method returns. Future execution must respect its returned expiry,
renew on time and stop when ownership is uncertain. Tiny caller-supplied leases
may already have expired by the time a response is received.

All methods defer rollback for unsuccessful paths. No transaction or connection
is returned to the caller. External execution must happen after successful claim
commit and outside all these transactions. There is no automatic SQL replay.

## Outcomes, errors and deferred behavior

Success maps to job SUCCEEDED/history SUCCEEDED; permanent failure maps to
FAILED/PERMANENT; unknown external outcome maps to RECONCILE/UNKNOWN, even at the
attempt limit. Terminal completion sets finished_at; RECONCILE leaves it null.
Retry policy, provider calls and scheduler promotion are excluded. Expired jobs
remain RUNNING; this part does not steal/reclaim an expired lease.

Missing claims return ErrNotFound; existing noneligible jobs return
ErrNotClaimable. Renewal/completion with missing, wrong-namespace, stale owner/token,
expired or finished ownership return ErrLeaseLost. Invalid input is rejected
before SQL. Overflowing the token refuses the claim instead of resetting it.
Underlying I/O/context/SQL errors remain wrapped for errors.Is/As.

A COMMIT error returns ErrOwnershipUnknown together with its underlying error.
Claim returns an empty claim; renewal returns zero confirmed expiry. Do not
execute or repeat effects after an ambiguous claim, renewal or completion.
Inspect authoritative job/history state deliberately before deciding what is
safe; there is no automatic recovery workflow or new ownership inspection API.
Repeated completion after an already committed result returns ErrLeaseLost,
so it must not be interpreted as proof that the earlier completion failed.

The M3 receipt/quarantine dispatcher, Kafka offsets, API/hash/idempotency, job
schema and outbox router remain unchanged. No worker pool, retry scheduler,
recovery daemon, Redis or Kubernetes is added. Database fencing cannot fence an
external HTTP call or make effects exactly once.

## Go compared with Java

Small implicit interfaces and compile-time assertions replace a broad service
hierarchy. The existing constructor injects database/sql and the namespace;
there is no DI container or ORM entity tracking. Explicit BeginTx/Commit and
defer Rollback keep boundaries visible, instead of Spring @Transactional proxy
boundaries and exception-based rollback rules. Errors.Is/As preserves domain
outcomes and cancellation through wrapping; context travels through all I/O.

The database row lock coordinates separate processes; neither a Go mutex nor
Java synchronized is sufficient across instances. database/sql is the shared
bounded pool, comparable to a JDBC DataSource. Handler code will receive only
committed claim data, never a transaction-scoped entity or database connection.

## Actual verification

Executed 2026-10-29 on darwin/arm64 with Go 1.26.4. Real services were the
existing healthy disposable Compose project `job-engine-m1`: PostgreSQL 17 on
loopback 55432, Kafka 19092 and Connect 18083. PostgreSQL tests create and remove
only their own random schemas; CDC regression appends unique rows in the local
connector database and pauses/restarts its connector. No volume was deleted.
The global local CDC schema was migrated through 003 by the existing CDC test
setup. Existing service processes/volumes remain available; this work starts no
worker or external execution process.

All commands below exited 0. Go commands used
`GOMODCACHE=/private/tmp/job-engine-modcache` and
`GOCACHE=/private/tmp/job-engine-gocache`; dependency tests also used the existing
local TEST_DATABASE_URL, TEST_KAFKA_BROKERS and TEST_CONNECT_URL:

```sh
gofmt -w .
gofmt -l .                      # empty output
go build ./...
go vet ./...
go test ./... -count=1
go test -race ./... -count=1
go test -tags=integration ./internal/storage/... -run TestOwnership -count=1 -v
go test -tags=integration ./internal/storage/... ./internal/api/... ./internal/dispatch/... -count=1 -timeout=4m
go test -race -tags=integration ./internal/storage/... ./internal/api/... -count=1 -timeout=4m
git diff --check
```

The initial ownership run passed all eight test groups then present (2.843s).
The combined real regression passed storage (3.140s), API (0.387s) and CDC/dispatch
(5.366s). After adding renewal/completion COMMIT-fault and migration/history-guard
coverage, the final real PostgreSQL race run passed storage (5.671s) and API
(1.471s), including all ten ownership test groups. Full unit/race/build/vet checks
were rerun after those additions and passed. Command packages and migrations have
no separate unit files; their migration behavior is exercised by real DB setup.

Verified properties:

- Five rounds of 24 competing claims across independent database pools yield
  exactly one owner/active history row per job. Duplicate/stale claims consume
  no additional attempt/token.
- Future due times, wrong namespace, RUNNING/RETRY_WAIT/RECONCILE/terminal states,
  final attempt budget and bigint token overflow reject claims. Last-budget
  claim increments to the maximum, then cannot be claimed again.
- Lease/deadline capping, non-shortening renewal, wrong owner/namespace rejection,
  expired renewal/completion and repeated completion are tested. Expired jobs
  cannot be reclaimed by Claim.
- A newer-attempt fixture with the same owner isolates the older-token fence:
  old renewal/completion fail while the current attempt completes. The fixture
  closes old history and requeues under a test-only SQL transaction; no production
  recovery operation was implemented.
- Injected history INSERT/UPDATE failure rolls back job and attempt changes;
  no token/attempt is consumed on failed claim. Missing active history likewise
  rolls back completion. History uniqueness/pairing constraints and migration 003
  checksum drift detection execute against real PostgreSQL.
- Tests observe pg_blocking_pids before releasing locks/canceling. A job becomes
  due during a lock wait and receives a lease based on post-lock time. Renewal
  and completion reject leases that expired during the lock wait.
- Pre-canceled context, observed lock-wait cancellation, PostgreSQL lock_timeout
  (SQLSTATE 55P03) and pool-acquisition deadline all return errors; the pool then
  completes claim/renew/complete successfully and reports zero in-use connections.
- Real PostgreSQL protocol faults before COMMIT transmission and after COMMIT
  success hide replies for all three operations. Unknown outcomes return no
  confirmed authority; persisted state/history matches whether COMMIT reached
  the server, and the affected pools reconnect successfully.
- Existing submission/hash, API concurrency/constraint/commit-ambiguity and CDC
  receipt/quarantine/restart/rebalance regression suites pass. A diff comparison
  confirms migrations 001/002, public submission/domain/API, dispatcher/router
  and Compose files are byte-for-byte unchanged from the M3 base.
- Local links in the changed Markdown documents were checked successfully.

The first fetch required permitted access to Git metadata; rerunning with that
permission succeeded. No test failed during implementation. Not tested: process
or power loss, production failover/load, external effects/provider fencing,
worker execution or recovery. No exactly-once execution claim is made.

Issue #11 remains open pending review. No commit, push, merge, deployment or later
M4 part is performed. A remote PR awaits separate push approval.
