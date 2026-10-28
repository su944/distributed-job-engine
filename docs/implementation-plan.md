# Implementation plan and review gates

Phase 0 is documentation only and precedes the seven implementation milestones
below. No application code, migration, worker, or infrastructure is authorized
by this plan. Milestone 1 starts only after explicit architecture approval.

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
below are proposed test contracts to be created in their milestone, not commands
that currently work. They must run with real PostgreSQL and later real Kafka and
Debezium; no silently skipped dependency tests count as acceptance. Configure
`TEST_DATABASE_URL` externally without committing credentials. The exact service
startup command must be documented when infrastructure is approved in M3.

## Phase 0 — architecture review (current)

Deliverables: architecture, seven Mermaid diagrams, database proposal, failure
analysis, REST examples, and this plan. Verify Markdown links, lifecycle agreement,
transaction guards, and `git diff --check`; render Mermaid in GitHub preview or an
approved renderer. Resolve safety-critical questions before implementation.
Gate: architecture reviewer approves scope, retry/reconciliation semantics,
ownership, provider contract, and staged infrastructure justification.

## Milestone 1 — Go foundation and domain contracts

Dependencies: Phase 0 approval and linked issue.
Scope: Go module, domain types, validated policy/configuration, handler interfaces,
bounded context/deadline contracts, and structured logging. No API, migrations,
Kafka deployment, or worker execution yet.
Acceptance: lifecycle transitions, error classifications, request canonicalization,
overflow-safe jitter, and budgets are unit tested; no unbounded concurrency.
Verification: baseline plus `go test ./internal/jobs/... -count=1`.
Review gate: approve package boundaries, handler replay-safety contract, and config
limits before M2.

## Milestone 2 — PostgreSQL ledger and atomic submission API

Dependencies: M1 accepted; approve database access/migration tooling, driver,
authentication boundary, retention assumptions, and real PostgreSQL test setup.
Scope: migrations for jobs/attempts/outbox/audit, repositories, POST/GET endpoints.
Acceptance: API examples match behavior; concurrent same-key requests create one
job; conflicting bodies return 409; rollback creates neither job nor event;
commit ambiguity resolves with the original key; checks/indexes enforce schema.
Verification: baseline; `go test -tags=integration ./internal/storage/... ./internal/api/... -count=1`.
Review gate: inspect migrations and real query plans, uniqueness races, and
transaction boundaries before CDC configuration.

## Milestone 3 — CDC and Kafka dispatch pipeline

Dependencies: M2 accepted; approve justified local PostgreSQL/Kafka/Connect/
Debezium configuration, supported versions, topic policy, Go client, quarantine,
and offset management. Infrastructure first becomes in scope here.
Acceptance: committed outbox records reach `jobs.ready.v1` keyed by job ID;
rollback does not publish; connector restart/replay is tolerated; malformed events
are durably quarantined before acknowledgement; backlogs and WAL lag observable.
Verification: baseline; `go test -tags=integration ./internal/dispatch/... -count=1`.
Review gate: inspect restart/offset tests, replication privileges, and retention
runbook; dispatch tests must not mistake delivery for completion.

## Milestone 4 — bounded execution and fenced ownership

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
