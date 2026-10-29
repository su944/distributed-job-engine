# Implementation plan and review gates

Phase 0 was documentation only. The user's subsequent explicit implementation
request authorizes the revised Milestone 1 below, tracked in
[issue #3](https://github.com/su944/distributed-job-engine/issues/3). It moves the
submission API/persistence from M2, PostgreSQL-only Compose from M3, and basic
HTTP health/shutdown from M6 into M1. Other architecture questions and all later
milestone gates remain open; this is not blanket release or architecture approval.

## Common workflow and verification

Each milestone needs a GitHub issue with the acceptance criteria below, a feature
branch, a PR linked to that issue, and recorded verification results. Push requires
approval. Keep issues open until acceptance criteria pass; no automatic closing
reference while checks are outstanding. Review the milestone before starting its
successor. README records material architecture decisions.

Once Go code exists, baseline checks from the module root are:

```sh
gofmt -l .
go build ./...
go vet ./...
go test ./...
go test -race ./...
git diff --check
```

Require empty formatting output and successful command exits. Integration checks
below work for M1; later suites are proposed contracts to create in their milestone.
They must run with real PostgreSQL and later real Kafka and
Debezium; no silently skipped dependency tests count as acceptance. Configure
`TEST_DATABASE_URL` externally without committing credentials. The exact service
startup command is now documented in README for PostgreSQL; Kafka/CDC remain M3.

## Phase 0 — target architecture review

Deliverables: architecture, seven Mermaid diagrams, database proposal, failure
analysis, REST examples, and this plan. Verify Markdown links, lifecycle agreement,
transaction guards, and `git diff --check`; render Mermaid in GitHub preview or an
approved renderer. Resolve safety-critical questions before implementation.
Gate: architecture reviewer approves scope, retry/reconciliation semantics,
ownership, provider contract, and staged infrastructure justification.

## Milestone 1 — Go foundation and durable submission API (revised scope)

Dependencies: explicit user authorization for this bounded scope and issue #3.
Scope: Go module, configuration, pgx/v5 via database/sql, jobs/outbox migrations,
domain and repository interface, validated idempotent POST/GET, health, slog,
HTTP deadlines/shutdown, PostgreSQL Compose, unit/httptest/real DB tests, README.
Acceptance: concurrent identical logical submissions create one job/event;
different requests sharing a key return 409; SQL enforces uniqueness; rollback
cannot leave a job without its event; health reflects DB/schema availability;
requests/DB pools are bounded and graceful shutdown drains within its deadline.
Verification: baseline plus `go test -tags=integration ./internal/storage/... -count=1`
and the same integration suite with `-race`; record actual results.
Deferred: handler execution/interfaces, retry jitter/ownership transitions, and
all Kafka/CDC/worker infrastructure. No automatic job processing in M1.
Review gate: review API, schema, DI, canonicalization, tests, and scope changes.
Do not start M2 or push/merge without approval.

## Milestone 2 — ledger and API hardening review

Current user-authorized work: [issue #5](https://github.com/su944/distributed-job-engine/issues/5),
based on merged M1 PR #4. Issues #1 and #3 remain open for acceptance tracking;
starting M2 does not close unresolved architecture or production-readiness reviews.
Dependencies: M1 accepted; approve database access/migration tooling, driver,
authentication boundary, retention assumptions, and real PostgreSQL test setup.
Scope: review and harden the M1 ledger/API; add attempts/audit only as required
by approved subsequent execution work. Jobs/outbox migrations and POST/GET have
already moved into M1 and must not be implemented a second time.
Acceptance: API examples match behavior; concurrent same-key requests create one
job; conflicting bodies return 409; rollback creates neither job nor event;
commit ambiguity resolves with the original key; checks/indexes enforce schema.
Also verify context-aware error mapping, bounded pool/SQL waits, hash compatibility,
and readiness when submission tables are absent. Keep existing migration 001,
indexes, and transaction implementation unless a demonstrated defect requires change.
Verification: baseline; `go test -tags=integration ./internal/storage/... ./internal/api/... -count=1`.
See [M2 review and executed checks](milestone-2-verification.md) for actual results
and each regression test's reliability guarantee.
Review gate: inspect migrations and real query plans, uniqueness races, and
transaction boundaries before CDC configuration.

## Milestone 3 — CDC and Kafka dispatch pipeline

Part 2 authorization (2026-10-29): reuse existing branch and CDC bootstrap;
verify Kafka/Connect, INSERT-only pgoutput and the Outbox Event Router through
real HTTP submission, rollback and connector restart. Preserve existing work;
do not extend Go consumers or begin Part 3 until review. Results and reproducible
commands: [Part 2 verification](milestone-3-part-2.md).

Current scope: [issue #9](https://github.com/su944/distributed-job-engine/issues/9).
M2 implementation PR #6 is merged and its real SQL/race revalidation passed.
The user's explicit M3 request clears bounded development; PR #8 remains open
and no prior acceptance issue is implicitly closed. No claims/execution in M3.
Review evidence and remaining limits are in `milestone-3-verification.md`.

Dependencies: M2 accepted; approve justified local PostgreSQL/Kafka/Connect/
Debezium configuration, supported versions, topic policy, Go client, quarantine,
and offset management. Kafka/CDC infrastructure first becomes in scope here;
PostgreSQL-only Compose is already included in M1.
Acceptance: committed outbox records reach `jobs.ready.v1` keyed by job ID;
rollback does not publish; connector restart/replay is tolerated; malformed events
are durably quarantined before acknowledgement; backlogs and WAL lag observable.
Verification: baseline; `go test -tags=integration ./internal/dispatch/... -count=1`.
Review gate: inspect restart/offset tests, replication privileges, and retention
runbook; dispatch tests must not mistake delivery for completion.

## Milestone 4 — bounded execution and fenced ownership

Full M4 authorization: the user's next request explicitly covers ownership
prerequisites plus bounded pool, handler registry, durable handoff, leases and
shutdown, while excluding M5. Inspection found Part 1 commit `55e26b8` already
present and pushed but not merged; origin/main remains accepted M3 `b5d2856`.
Branch `feat/milestone-4-worker-pool` preserves/reuses that prerequisite rather
than recreating it. [Issue #12](https://github.com/su944/distributed-job-engine/issues/12)
tracks the expanded scope. Chosen handoff: SQL receipt inbox drain with SKIP LOCKED,
not a Kafka execution queue or new consumer group. The following Part 1 record
is historical; the current request authorizes continuation through full M4 only.
See [current design and actual verification](milestone-4-worker-pool.md).

Part 1 authorization: the user requested atomic claiming/renewal/fenced completion
and confirmed M3 merged. Verified PR #10 merged into main `b5d2856` (contains M3
`6af1f0d`); reviewed existing dispatcher, ledger, SQL/tests and design/failure
documents. [Issue #11](https://github.com/su944/distributed-job-engine/issues/11)
tracks Part 1 on `feat/milestone-4-atomic-ownership-11`, branched from that merged
main. Scope: ownership repository operations and additive attempt history only.
No worker pool, external handler, Kafka offset change, scheduler or recovery daemon.
The provider-safe handler gate applies to later execution, which is not started.
Verification: baseline plus real PostgreSQL storage/API ownership suites and M3
CDC regression. [Part 1 record](milestone-4-part-1.md) documents actual results.
Stop after Part 1 review; no push, merge or deployment is authorized.

The following acceptance criteria describe the full M4 target, beyond Part 1.

Dependencies: M3 accepted and provider-safe test handler approved.
Acceptance: N slots cap execution; claims increment attempts/tokens atomically;
heartbeats cannot revive expired leases; stale completion fails; all execution
occurs outside DB transactions. Consumer commits only contiguous handled offsets;
rebalances and saturation preserve ownership and bound memory.
Verification: baseline; `go test -tags=integration ./internal/worker/... -count=1`.
Review gate: demonstrate competing workers, paused stale worker, DB lock waits,
ambiguous claim resolution, and bounded resource use before recovery logic.

## Milestone 5 — recovery, retries, and reconciliation

Dependencies: M4 accepted; approve scan cadence, retry limits, provider lookup
contracts, and privileged reconciliation audit path.
Acceptance: expired attempts recover once; retries obey budget/due time/jitter;
multiple schedulers cannot double-promote; READY repair restores notifications;
unsafe unknown outcomes enter RECONCILE; audited resolution respects evidence and
budget; terminal jobs remain immutable.
Verification: baseline; `go test -tags=integration ./internal/scheduler/... ./internal/reconciliation/... -count=1`.
Review gate: failure injection covers every scenario in `failure-scenarios.md`,
including exhausted unknown outcomes and retry storms, before hardening.

## Milestone 6 — observability and graceful operations

Dependencies: M5 accepted; approve metric cardinality, alert thresholds, and
deployment grace/handler deadline relationship.
Acceptance: readiness/liveness distinct; structured logs redact payloads/secrets;
metrics cover backlog, leases, retries, slots, CDC/WAL, and uncertainty. SIGTERM
stops intake, drains with renewals, cancels on deadline, and permits safe recovery.
Verification: baseline; `go test -tags=integration ./internal/telemetry/... ./internal/shutdown/... -count=1`.
Review gate: demonstrate shutdown/rebalance/DB outage traces, bounded exits, and
actionable alerts without high-cardinality metric labels.

## Milestone 7 — resilience validation and release readiness

Dependencies: M6 accepted; agree load profile, SLO, security/durability targets,
retention windows, provider guarantees, and restore procedures.
Acceptance: all failure scenarios demonstrated against real dependencies; load
stays within agreed memory/DB/latency budgets; replay and backup/restore drills
reconcile uncertain effects; runbooks explain connector loss and WAL pressure;
remaining limitations are explicit in README. No exactly-once external promise.
Verification: baseline; `go test -tags=integration ./... -count=1`; execute and
record reviewed load, outage, and restore drill commands added by this milestone.
Review gate: human release approval. Passing this gate does not authorize push,
merge, deployment, or new infrastructure without their required approvals.
