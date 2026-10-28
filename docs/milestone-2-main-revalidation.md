# Milestone 2 acceptance revalidation on merged main

Follow-up [issue #7](https://github.com/su944/distributed-job-engine/issues/7),
branch `docs/milestone-2-main-revalidation`. Reviewed fetched main `f0ff345`
on 2026-10-28. Its tree matches M2 implementation commit `a27d8e8`; main includes
merged M1 PR #4 and M2 PR #6. This is a fresh verification run, not a duplicate
implementation of the already merged milestone.

## Inspection and findings

Inspected AGENTS.md, architecture, database design, failure scenarios, plan,
domain/canonicalizer, HTTP error handling, SQL repository, pool configuration,
migration runner/schema, and acceptance tests. PRs #4 and #6 are merged and have
no recorded comments/reviews. Acceptance issues #1, #3, and #5 remain open with
empty discussion threads at the time of review; they are not closed by this work.

No additional demonstrated production defect or missing test requiring a code
change was found within this review's scope. The prior M2 readiness and context
mapping fixes and the requested concurrency/commit coverage are already in main.
No migrations, dependencies, hashes, repository SQL, pool configuration, or tests
were changed. Rewriting those paths or introducing more execution tables/indexes
would expand the milestone without an observed need.

The documentation still described M2 work without stating that its PR had merged.
README and the plan now point to that merged implementation and this follow-up
record. Architecture decisions remain unchanged; M3 approval is still outstanding.

## Existing tests and reliability guarantees rechecked

- **Concurrent same-key submissions:** repository and 24-request HTTP races
  verify a single creator/job/outbox pair. Equivalent requests retain one ID and
  Location; different logical requests under the winning key receive 409.
- **Canonical hashes:** the persisted golden vector guards upgrade compatibility.
  Exact decimals, ordering, defaults, Unicode escapes, arrays, null/missing,
  adjacent large integers, type, and policy cases check equality and distinction.
- **Atomic jobs/outbox:** an induced outbox constraint failure rolls back the
  job; success checks verify consistent event/job identifiers and one pair.
- **Ambiguous COMMIT:** real PostgreSQL protocol faults prevent sending COMMIT
  or hide its successful response. The API reports uncertainty; retrying the same
  key resolves either durable outcome without duplicating a job/event. The failed
  pooled connection is discarded and the pool remains usable.
- **Context and pool management:** an observed unique-key lock wait is canceled
  without leaving a waiter submission. A sole held pool connection forces a
  request deadline; releasing it restores progress. Replays/conflicts release
  transactions, and error-code tests preserve uncertainty over cancellation.
- **Schema and indexes:** CHECK/FK SQLSTATE assertions reject invalid writes;
  EXPLAIN ANALYZE on 1,000 rows uses the existing unique and primary-key indexes.
  Missing tables/ledger and database closure fail readiness. No speculative index
  or schema migration is justified by the currently exercised query shapes.

The [original M2 verification](milestone-2-verification.md) names each test and
documents the two bugs fixed in the merged PR. Existing HTTP lifecycle, input,
log-redaction, namespace, and migration/checksum tests were also rerun.

## Actual checks

Go 1.26.4; PostgreSQL 17 from the existing Compose service `job-engine-m1` on
loopback port 55432. SQL tests used their own isolated schemas with
TEST_DATABASE_URL. Go caches were under /private/tmp. The local .idea directory
was preserved. All requested commands passed with exit 0:

```sh
gofmt -l .
go build ./...
go vet ./...
go test ./... -count=1
go test -race ./... -count=1
go test -tags=integration ./internal/storage/... ./internal/api/... -count=1
git diff --check
```

Formatting produced no output. Unit and race tests explicitly bypassed the test
cache; integration also used count=1. No requested checks were skipped. Packages
cmd/api, cmd/migrate, and migrations report no test files; migration behavior is
covered through real SQL integration tests. Real SQL-backed HTTP tests reside
in internal/storage, and the requested command also runs internal/api transport
tests. Markdown links and fences were checked after the documentation update.
The PostgreSQL verification container was stopped afterward; its volume is retained.

## Remaining acceptance limits

This run does not verify arbitrary schema drift, production capacity, PostgreSQL
failover, OS/process/power-loss crashes, or external side effects. Readiness only
checks the documented ledger/relations; representative index plans do not prove
load SLOs. COMMIT protocol injection is not a server crash or power-loss test.
Authentication/production roles, retention and disaster-recovery review remain
open architecture/production gates. Future execution columns already exist in
the proposed schema; they do not mean workers or leases are implemented.

No Kafka, Debezium, worker pools, lease renewal, scheduling, or M3 work was added.
Keep the follow-up and earlier acceptance issues open pending review; no merge
or next milestone is authorized by this revalidation.
