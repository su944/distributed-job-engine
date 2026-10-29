# Milestone 4: atomic ownership and bounded Go workers

Tracking [issue #12](https://github.com/su944/distributed-job-engine/issues/12),
branch `feat/milestone-4-worker-pool`. Stop before Milestone 5.

## Inspection and approval

Inspected git status, AGENTS, architecture, database design, failure scenarios,
implementation plan, migrations 001–003, ownership interfaces/SQL/tests, dispatcher
receipt/quarantine and Kafka commit/rebalance behavior. Fetched origin: latest
accepted main is merged M3 `b5d2856`. Ownership Part 1 `55e26b8` already exists
on the prior feature branch (issue #11), although not merged into main. The user's
new request explicitly authorizes reusing/implementing ownership prerequisites
before the full pool. The new branch preserves that commit and `.idea/` files;
no ZIP or missing-code assumption is made. No applied migration was edited.

## Chosen handoff: durable receipt inbox

The Go Kafka dispatcher remains unchanged: decode/validate; commit a receipt or
bounded quarantine; synchronously commit offset+1. Neither Kafka offset nor
receipt means execution completed. Worker slots do not consume Kafka or receive
volatile job-ID channels. They query for a READY, due, registered-type job in
their trusted namespace that has a persisted dispatch_events receipt.

Selection uses LIMIT 1 FOR UPDATE OF jobs SKIP LOCKED, followed by the shared
atomic claim/history insert in the same short transaction. Job state determines
eligibility; receipts do not get a consumed flag. A worker may claim after the
receipt commits but before Kafka's offset commits. Replay then finds a duplicate
receipt and cannot claim the same RUNNING job a second time. Historical receipts
remain useful for future eligible jobs; cleanup is not implemented.

This design adds indexed SQL polling (default four empty polls per slot per
second), plus one receipt job-ID index and one partial READY selection index in
additive migration 004. It avoids a second Kafka group, a new durable queue table,
and a correctness dependency on channel contents. The query is capped at one
result and the I/O/context budgets bound waits; this is not a production capacity
or scan-cost benchmark. Large unsupported-type backlogs need measured tuning.

Kafka polling/rebalances stay in cmd/dispatcher and remain independent of handler
duration, worker saturation and cancellation. Multiple workers in one namespace
compete through PostgreSQL, not Kafka assignments. Worker readiness checks SQL
schema/state and running/draining lifecycle; it does not prove CDC caught up.

## SQL transitions and fences

- Claim by ID retains the existing READY/due/budget checks. Inbox claims select
  only registered types with durable receipts. Both paths increment attempts and
  token atomically, set RUNNING/owner/expiry/deadline and insert job_attempts.
- Time is sampled with clock_timestamp() after obtaining the job row lock. The
  selection time is only a candidate filter; the guarded update rechecks time.
  Locks always go job first, attempt history second.
- Renewal requires current RUNNING owner/token and lease/deadline strictly after
  database time; expiry never shortens or exceeds the fixed attempt deadline.
- Completion requires the same live fence, clears ownership, preserves the token,
  and closes matching history atomically. Success maps to SUCCEEDED; permanent
  failure to FAILED; unknown outcome to RECONCILE even at the last attempt.
- Explicit retryable failure is allowed only by trusted ReplaySafe registration
  in the worker. It records RETRYABLE and enters RETRY_WAIT if budget remains,
  otherwise FAILED. No backoff/due-time algorithm, promotion or re-execution is
  provided. Unknown outcomes never become retryable merely because registration
  is replay-safe. Unsafe retry requests become UNKNOWN/RECONCILE.

The existing jobs.replay_safe submission field is unchanged (default false);
registry safety for a known returned failure is not a blanket recovery promise.
M5 must separately review how historical jobs and handler safety are reconciled.

Ownership I/O retains the Part 1 five-second overall bound, one-second SQL lock
timeout and four-second statement timeout. Worker I/O contexts can shorten this.
Any uncertain commit returns no confirmed claim/renewal permission and is not
blindly replayed. No handler receives a transaction, pool connection or Kafka record.

## Goroutines, cancellation and bounds

Run starts exactly Capacity fixed slot goroutines. Each idle slot claims and
executes one job itself, so capacity is reserved before claim. There is no
unbounded job channel, goroutine-per-notification or in-memory retry buffer.
Additional goroutines are bounded: one heartbeat per active slot, one WaitGroup
waiter, bounded timer callbacks and normal library connection/HTTP routines.
Payload memory is proportional to capacity, not backlog; receipt backlog remains
durable SQL storage requiring monitoring and retention review.

The registry is explicitly built before pool construction, which copies the map.
Handler implementations must be safe for concurrent calls, honor context, and
not detach external effects. Only deterministic, side-effect-free demo.echo is
installed. It returns the stored payload as a private object result; malformed or
oversized handler results and panics map to UNKNOWN/RECONCILE, never logged raw.
Results remain subject to a 256 KiB object bound; very large JSONB serialization
can exceed that bound and is handled conservatively rather than published/logged.

Claim returns its database acquisition sample. The worker converts lease/deadline
differences to local monotonic deadlines anchored before claim I/O. This avoids
assuming the host and database wall clocks match and conservatively includes I/O
delay. A local lease timer cancels the handler if renewal cannot confirm ownership.
Heartbeat errors, timeout, expiry and uncertain replies stop execution. Heartbeats
continue through graceful drain on a separate bounded SQL pool (up to four
connections by default, in addition to the main DB_MAX_OPEN pool). In-flight
renewal finishes before completion, avoiding renewal/completion races.

SIGTERM cancels intake and in-flight claim contexts, marks readiness unready and
drains active handlers while renewing them. An in-flight claim whose commit races
shutdown may remain RUNNING without execution; it is not released optimistically.
After DrainTimeout, execution contexts are canceled and Run returns ErrDrainTimeout.
Go cannot forcibly kill an uncooperative goroutine: the command exits nonzero,
leaving ownership intact until future recovery. Late handler success cannot
complete after cancellation. Do not restart the same pool object after timeout.

Claim/completion errors stop intake and drain the instance; ownership renewal
failures cancel the affected handler and stop the pool. Restart the process after
repair. No unbounded reconnect/transaction replay loop is introduced.

## Crash/replay behavior

- Before durable receipt: Kafka offset is uncommitted, so dispatcher replay is safe.
- After receipt, before claim: receipt survives even if the offset already committed;
  any replacement worker can claim the still-READY job without another Kafka event.
- During claim before commit: job/history roll back together. An ambiguous commit
  never grants execution permission.
- After claim: RUNNING and attempt history survive. A replacement cannot steal an
  expired lease. M5 must recover/reconcile it; eventual execution after every crash
  is not claimed yet.
- After completion before acknowledgement: SQL status/history are authoritative;
  receipt replay sees a non-READY job. Unknown completion commits require inspection.
- Kafka rebalance: membership changes affect the dispatcher only; active handlers
  retain PostgreSQL ownership and renewal until they finish, lose the lease or drain.
- Handler timeout/lease loss/forced drain: cancel, do not release ownership while
  effects may continue. No provider calls, exactly-once effect claim or automatic
  unsafe retry is introduced.

## Local operation and reproduction

Use the existing disposable CDC stack and run migrations explicitly. API defaults
are preserved; enable demo.echo in JOB_TYPES and use the same JOB_NAMESPACE in API
and worker. cmd/worker deliberately leaves invoice.send and other unregistered
types READY. Running a worker does not require KAFKA_BROKERS; the separate dispatcher
does. Each process needs DATABASE_URL. No secrets belong in Git or logs.

Worker environment defaults:

- WORKER_ID: generated per process, or an explicit 1–128 printable non-space ASCII ID.
- WORKER_CAPACITY: 4, range 1–64.
- WORKER_POLL_INTERVAL: 250ms, minimum 10ms.
- WORKER_LEASE_DURATION: 30s; WORKER_HEARTBEAT_INTERVAL: 10s.
- WORKER_ATTEMPT_TIMEOUT: 5m; WORKER_IO_TIMEOUT: 3s.
- WORKER_DRAIN_TIMEOUT: 20s; WORKER_HTTP_ADDR: 127.0.0.1:8082.

Durations must be whole microseconds in (0,24h]. Heartbeat+I/O must fit strictly
inside the lease, and I/O must be shorter than the attempt timeout. Health exposes
/health/live and /health/ready; /metrics reports configured capacity, active slots,
recorded outcomes (not just successes) and process errors without job labels.

```sh
# Follow README/CDC runbook first; use dedicated disposable dependencies.
export TEST_DATABASE_URL="$DATABASE_URL"
export TEST_KAFKA_BROKERS="$KAFKA_BROKERS"
export TEST_CONNECT_URL=http://127.0.0.1:18083
go run ./cmd/migrate
python3 scripts/smoke-worker.py
go test -tags=integration ./internal/storage/... ./internal/api/... ./internal/worker/... ./internal/dispatch/... -count=1 -timeout=4m
go test -race -tags=integration ./internal/worker/... ./internal/storage/... ./internal/api/... -count=1 -timeout=4m
go test -race -tags=integration ./internal/dispatch/... -count=1 -timeout=4m
```

The CLI smoke builds real API/dispatcher/worker binaries into a temporary directory,
applies migrations, submits a unique namespaced demo.echo job, polls SUCCEEDED/one
attempt, checks metrics and verifies SIGTERM exit 0 for all three. It uses loopback
ports 18084–18086 and preserves job/receipt/history rows and service volumes.
SQL integration uses isolated random schemas. Kafka tests append unique namespace
rows and groups in the disposable connector database and exercise the real CDC path.

## Go compared with Java Spring Boot

Fixed goroutines act like a fixed ExecutorService with no submitted-task backlog.
A slot owns the claim and handler call; channel buffers here carry only bounded
control signals/results, not work that could be mistaken for durable delivery.
WaitGroup tracks slot exit, comparable to joining futures or a CountDownLatch.
Contexts carry cancellation/deadlines explicitly; Java interruption and Future.cancel
also require cooperative tasks and cannot safely stop arbitrary provider effects.

Small interfaces, an immutable registry snapshot and explicit constructors replace
annotation-driven component scanning. database/sql pools correspond to JDBC
DataSources. Explicit BeginTx/Commit/deferred rollback keep transaction boundaries
visible rather than relying on @Transactional proxies around long handler calls.
PostgreSQL fences coordinate processes; Go mutexes or Java synchronized cannot.

## Actual verification and remaining limits

Executed 2026-10-29 with Go 1.26.4 on darwin/arm64. Real dependencies: existing
healthy Compose project job-engine-m1, PostgreSQL 17 on loopback 55432, Kafka
19092, Connect 18083. Go caches were GOMODCACHE=/private/tmp/job-engine-modcache
and GOCACHE=/private/tmp/job-engine-gocache. No suite silently skipped absent
dependencies; tests require explicit TEST_DATABASE_URL and Kafka/Connect variables.

Successful commands (exit 0):

```sh
gofmt -w .
gofmt -l .                        # empty
go build ./...
go vet ./...
go test ./... -count=1
go test -race ./... -count=1
go test -tags=integration ./internal/storage/... ./internal/api/... ./internal/worker/... ./internal/dispatch/... -count=1 -timeout=4m
go test -race -tags=integration ./internal/worker/... ./internal/storage/... ./internal/api/... -count=1 -timeout=4m
go test -race -tags=integration ./internal/worker/... -count=1 -v -timeout=3m
go test -race -tags=integration ./internal/dispatch/... -count=1 -timeout=4m
python3 scripts/smoke-worker.py
git diff --check
```

The combined real integration run passed storage (3.857s), API (0.417s), worker
(6.323s) and dispatch (8.853s). The final worker race/process suite passed (8.742s),
including child SIGKILL before claim, SIGKILL after claim, and SIGTERM during an
active handler. Storage/API race suites passed (5.818s/1.448s); real dispatch race
suite passed (9.758s), including the new handoff/rebalance check and existing CDC
restart/quarantine tests. Full build/vet/unit/race checks were rerun after final
readiness/registry validation changes and passed. No command package has separate
unit tests; the built composition roots are covered by the CLI smoke.

Full CLI pipeline smoke passed twice. Final job
`e6eb6305-7463-4429-ad7c-34084b9504a2` reached SUCCEEDED with attempts=1 through
POST -> jobs/outbox -> Debezium -> Kafka -> dispatcher receipt -> worker -> fenced
completion. API, dispatcher and worker each exited 0 on SIGTERM. Temporary binaries
were removed, smoke processes stopped, test data and existing service volumes
retained. The global disposable schema is now at migration 004.

Checked properties:

- Two pools of capacity two execute twelve jobs once each, with at most four
  active handlers total. Original real ownership competition tests also pass.
- Duplicate SQL receipts and Kafka notifications preserve one attempt. With the
  dispatcher stopped after receipt and offset commit, a new worker executes the
  READY job from SQL alone. Unsupported types and jobs lacking receipts stay READY.
- Real Kafka group rebalance redistributes all three partitions between two
  dispatcher instances while an active worker remains uncanceled. The produced
  duplicate's actual Kafka offset is observed committed before completion.
- Child-process SIGKILL before claim leaves READY plus its durable receipt for
  a replacement. SIGKILL after claim leaves RUNNING/history; after lease expiry a
  replacement refuses to steal it. This intentionally demonstrates the M5 gap.
- Success, permanent failure, unknown at exhausted budget, safe retryable failure,
  unsafe retry request and exhausted retryable failure persist correct state/history.
  RETRY_WAIT is not automatically promoted on a subsequent worker restart.
- A separate SQL connection obtains FOR UPDATE NOWAIT during handler execution,
  proving no claim transaction/row lock spans the handler. A handler outlives its
  initial lease and succeeds via heartbeats.
- Expiry and heartbeat database pool failure cancel handlers without completion.
  Unit tests also inject uncertain renewal and blocked I/O and confirm cancellation
  with no fenced completion call. Existing old-token, expiry-during-lock-wait,
  rollback, canceled-context and ambiguous-commit PostgreSQL tests pass unchanged.
- Graceful shutdown leaves queued jobs unclaimed and completes active work; actual
  child SIGTERM drains successfully. Forced drain returns within its configured
  deadline and leaves ownership RUNNING while an uncooperative handler continues;
  its late return cannot complete the job.
- Injected receipt-store database failure in a real Kafka dispatcher leaves offsets
  uncommitted. Heartbeat DB failure uses a closed real PostgreSQL pool; a full
  PostgreSQL server outage was not injected in this work.
- Migrations 001–003, submission/API/hash code, Kafka consumer/store and CDC/Compose
  files are byte-for-byte unchanged from prerequisite commit 55e26b8. Changed
  Markdown local links, formatting, whitespace and a basic credential-pattern scan
  passed. `.idea/` remains untouched/untracked.

Two early assertions required correction: the old Part 1 unit test treated the
newly supported RETRYABLE outcome as invalid, and the worker replay fixture
expected Received when its receipt already existed. They were updated to the new
contract/idempotent fixture behavior; final unit and real dependency runs above
passed. There are no unresolved test failures.

No M5 scheduler, expired-lease recovery, reconciliation workflow, external provider,
Redis or Kubernetes. Production HA, host/database clock jumps, failover/power loss,
retention cleanup and load/soak capacity are not established by these tests. Durable
SQL receipts and a single-node Kafka volume are not a production backup guarantee.
Uncooperative handler termination relies on process exit. Monitor READY age,
unsupported types, RETRY_WAIT/RECONCILE/expired RUNNING backlog, pool pressure,
CDC/WAL lag and receipt growth. Do not delete receipts before a reviewed policy.

Issue #12 remains open for review. No push, merge, deployment or M5 is authorized
by this implementation. The prior Part 1 commit is preserved, not rewritten.
