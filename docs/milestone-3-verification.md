# Milestone 3 review and actual verification

Branch `feat/milestone-3-cdc-dispatch`, [issue #9](https://github.com/su944/distributed-job-engine/issues/9).
Base: fetched main `f0ff345`. Existing untracked .idea work was preserved. PR #8
remains open; its documentation-only changes were not overwritten or merged.

## M2 gate and scope authorization

Inspected latest main, all open PRs (only #8), AGENTS, architecture, database,
failure scenarios, implementation plan and M2 verification. PR #6 is merged;
the M2 revalidation recorded passing formatting/build/vet/unit/race and real
PostgreSQL checks on the same implementation. No new review comments or failed
acceptance evidence were present. The user's explicit M3 request authorizes this
bounded next phase. Issues #1/#3/#5/#7 stay open; none are implicitly accepted
or closed and PR #8 is not merged by this work.

## Changes and architecture decisions

- Optional pinned Compose Kafka 4.0.1 and Connect/Debezium 3.2.4.Final, retaining
  the existing PostgreSQL 17 image/volume and adding logical WAL settings.
  Image digests are pinned. The attempted 3.2.7.Final image was unavailable;
  registry verification selected an available 3.2 image supporting PG17/Kafka4.
- Explicit cdc-setup creates restricted replication credentials and an INSERT-only
  publication, topics and router config. Passwords use Connect's environment
  provider; snapshots include pre-existing committed outbox rows. Slot is retained.
- Additive migration 002 creates durable notification receipts and quarantine.
  Migration 001, existing POST/GET APIs, request hashing, repository SQL and
  idempotency logic are unchanged. Migration runner now verifies both versions.
- `cmd/dispatcher` uses strict v1 validation, sequential bounded polling, SQL
  receipt/quarantine before synchronous offsets, replay-safe event-ID hashing,
  assignment/DB/broker readiness, structured safe logs, and five basic metrics.
- A receipt is not a claim or completion. All jobs remain READY/attempts=0 in
  the tests. M4 must separately approve the receipt/worker handoff. No workers,
  claims, lease renewal, retry scheduling, Redis or Kubernetes are introduced.

The [runbook](cdc-runbook.md) defines the exact envelope, compatibility sources,
resource limits, topic/offset semantics, credential setup, restart, WAL monitoring,
outage handling, source epochs and explicit recovery/replay procedure. Architecture
diagrams now show the M3 durable receipt boundary and distinguish future workers.

## Executed checks — all passed

Executed on 2026-10-28 with Go 1.26.4. Go caches used /private/tmp. Real dependency
stack: Compose project job-engine-m1, PostgreSQL loopback 55432, Kafka 19092,
Connect 18083. Real SQL tests use random schemas for existing API tests; the CDC
suite appends its own UUID-tagged rows in the disposable connector database.
After verification the dispatcher smoke process exited and all three test
services were stopped; PostgreSQL and Kafka named volumes/state were retained.
All listed commands exited 0; formatting output was empty:

```sh
gofmt -l .
go build ./...
go vet ./...
go test ./... -count=1
go test -race ./... -count=1
go test -tags=integration ./internal/storage/... ./internal/api/... ./internal/dispatch/... -count=1 -timeout=4m
go test -race -tags=integration ./internal/dispatch/... -count=1 -timeout=4m
git diff --check
```

Final Compose configuration validation also passed without printing resolved secrets.

No requested checks were skipped. Unit and race runs were uncached. Entry-point
and migration packages have no separate unit files; setup/migration behavior was
exercised against the real stack and the built dispatcher was smoke tested.

## Guarantees checked

- Committed outbox publication: a real API repository transaction produces a
  correctly keyed v1 notification; the first durable receipt is unique.
- Duplicate delivery: an actual duplicate is produced to Kafka and recorded as
  Duplicate without another receipt. Conflicting event-ID reuse is quarantined
  idempotently rather than changing the original receipt.
- Rollback exclusion: an outbox insert is rolled back; a later committed event
  on the same key/partition provides a WAL/stream barrier, with no rollback event.
- Connector restart: REST pause, backlog insert, resume and connector/task restart
  resume publishing from retained state. Both connector and task report RUNNING.
- Quarantine before acknowledgement: injected persistence failure leaves a real
  malformed Kafka record uncommitted and absent from SQL quarantine. A replacement
  consumer in the same group persists quarantine before its committed offset
  advances past that record. Unit tests directly assert store-before-commit order.
- Shutdown/rebalance: a consumer holds the target notification during a real
  group rebalance, then is canceled before persistence. The second member receives
  it and durably handles/acknowledges it, without changing job state.
- Strict input and bounded processing: unit tests reject missing/unknown/duplicate/
  null fields, invalid types, unsupported versions, bad UUID/time/key, oversized,
  malformed and trailing JSON. Store errors never invoke offset commit; commit
  errors propagate for replay. Health/config/metrics routes are exercised.
- Built dispatcher smoke: live and ready return 200 with assigned partitions,
  metrics are present, and SIGTERM shuts down within the test deadline with exit 0.
- Existing M2 real SQL/API tests pass unchanged, including idempotency races,
  hash compatibility, atomic rollback, ambiguous commits, pool/cancellation,
  constraints/index plans and readiness.

## Resolved setup failures and remaining limits

The initial image pull failed because the documented artifact had no matching
container tag. The initial connector validation then rejected authentication:
the image requires CONNECT_CONFIG_PROVIDERS* variable names. Both were corrected,
the connector became RUNNING and the final real suites passed. No unresolved
test failures remain. Logs do not print SQL/record payloads or raw credentials.

Not tested: production HA, process/power-loss crashes, prolonged broker outages,
slot invalidation/WAL-loss recovery, Kafka retention-gap recovery, live credential
rotation or production load/capacity. Kafka and Connect have single-node local
replication factors; named volumes are not an HA/backup guarantee. Receipt and
quarantine row counts require storage monitoring/retention review. Oversized
quarantine values/keys retain bounded prefixes plus full hashes/counts, not full
raw payloads. Large transport/decompression failures stop without acknowledgement.
Mermaid was updated using GitHub-compatible syntax; no rendering check was run.

No exactly-once delivery or execution claim. No push, merge or deployment is
authorized by this implementation. Stop for M3 review before M4.
