# Milestone 2 review and verification

Work: [issue #5](https://github.com/su944/distributed-job-engine/issues/5), branch
`feat/milestone-2-ledger-hardening`, based on fetched main
`d40aa83b3a524ec5afc062e6cac29dc9179069ff`. Reviewed AGENTS, architecture, database
design, failure scenarios, implementation plan, Go code, SQL, and existing tests.

## Milestone 1 and open acceptance review

[PR #4](https://github.com/su944/distributed-job-engine/pull/4) merged on
2026-10-28; GitHub reported no PR comments or reviews. Its recorded validation
explicitly excluded lost-COMMIT-ack fault injection. Open issues at review were
#1 (architecture/diagram acceptance) and #3 (Milestone 1 acceptance). Neither is
closed by this work; both issue discussion threads were empty. The user's explicit request authorizes M2's bounded review;
it does not authorize M3, production deployment, or resolving those review gates.

## Findings and necessary fixes

1. **Readiness false positive:** an intact migration ledger reported ready even
   after jobs or outbox_events disappeared. Regression tests failed before the
   fix. The readiness query now resolves both relations using constant-false
   subqueries: no job rows are scanned. This checks availability, not arbitrary
   catalog drift or every migration checksum/constraint on every request.
2. **Misclassified context errors:** SQL cancellation/deadlines were labeled
   database_unavailable and body cancellation returned 400 invalid_request.
   Regression tests reproduced both. They now return JSON 503 request_canceled
   or request_timeout. submission_outcome_unknown retains precedence over an
   underlying context error; cancellation cannot prove a COMMIT rolled back.
3. **Acceptance gaps:** the M1 transaction and uniqueness design worked, but
   competing HTTP request variants, real uncertain commits, active SQL waits,
   and exhausted pool recovery lacked tests. Added coverage without rewriting
   the submission repository or introducing production fault hooks.

No schema/index defect requiring a migration was found for current submission
queries. No changes to migration 001, canonical hashing/defaults, dependencies,
submission SQL, or connection-pool implementation. Future ownership columns are
pre-existing design groundwork; adding attempts/audit tables or scheduler indexes
now would be premature. The protocol fault harness is integration-test-only;
no production transaction framework or competing database pool was introduced.

## Tests and the guarantees they check

- `TestPersistedHashCompatibility`: a frozen SHA-256 vector prevents deployments
  from silently turning retries of existing submissions into conflicts.
- `TestLogicalRequestBoundaries`: exact decimal/negative-zero/Unicode escape and
  nested-object equivalence preserve hashes; ordered arrays, missing/null,
  strings/numbers, adjacent large integers, type, and policy stay distinct.
- `TestConcurrentHTTPLogicalRequests`: 24 simultaneous equivalent or conflicting
  requests yield one creator, stable replay IDs/Locations, 409 for losing logical
  requests, and exactly one consistent job/outbox envelope.
- `TestAmbiguousCommitRetryResolvesBothOutcomes`: real PostgreSQL either never
  receives COMMIT or commits while its success reply is withheld from pgx. Both
  return submission_outcome_unknown; retrying the same key creates the absent
  job or reads the committed ID. Exactly one pair remains and the failed pooled
  connection is replaced. Protocol wrapping occurs after TLS, without SQL mocks.
- `TestCancellationWhileWaitingForUniqueKey`: pg_blocking_pids confirms a real
  lock wait before cancellation. The waiter returns request_canceled, leaves no
  job/event, releases its transaction, and the same key can subsequently succeed.
- `TestPoolExhaustionDeadlineAndTransactionRelease`: a held sole connection makes
  submission time out without writing or exceeding pool capacity. Release restores
  progress; repeated replays/conflicts release transactions rather than exhausting
  the pool. Existing Open configuration and single-pool design remain unchanged.
- `TestReadinessRequiresSubmissionTables`: removing either relation while retaining
  the ledger fails storage readiness and HTTP readiness with 503 not_ready.
- `TestLedgerConstraintsRejectInvalidWrites`: expected PostgreSQL CHECK/FK
  SQLSTATEs reject malformed payload/hash, invalid attempt/state/ownership fields,
  invalid event type/object, orphan events, and deletion of referenced jobs.
- `TestSubmissionLookupIndexPlans`: real EXPLAIN ANALYZE over 1,000 synthetic rows
  uses jobs_namespace_key_unique and jobs_pkey for the current lookup shapes.
  This verifies query support, not production throughput or all possible planners.
- `TestContextErrorsHaveRequestCodes` / `TestBodyCancellationIsNotInvalidInput`:
  wrapped context failures have consistent JSON codes; commit uncertainty wins.
  A disconnected client may be unable to receive that JSON response.

Existing tests still verify atomic rollback after an outbox-write failure,
namespace isolation, migration repeat/checksum drift, JSON validation, health,
log redaction, and graceful/forced HTTP shutdown.

## Actual verification results

Executed on 2026-10-28 with Go 1.26.4 and real PostgreSQL 17 in the existing
Compose project job-engine-m1, loopback port 55432. TEST_DATABASE_URL points to
that disposable development instance; every SQL test uses its own random schema.
GOMODCACHE and GOCACHE were under /private/tmp. Socket/Docker access was approved.
The verification PostgreSQL container was stopped after checks; its volume is retained.

All commands below passed (exit 0); formatting output was empty:

```sh
gofmt -l .
go build ./...
go vet ./...
go test ./...
go test -race ./...
go test -tags=integration ./internal/storage/... ./internal/api/... -count=1
go test -race -tags=integration ./internal/storage/... ./internal/api/... -count=1
git diff --check
```

The targeted regression tests first failed on main's readiness/context behavior,
then passed after the fixes. No unresolved check failures and no requested checks
skipped. Unchanged packages may use Go's unit-test cache; both final integration
runs used count=1. Real SQL-backed HTTP tests reside in internal/storage; the
same command also runs internal/api's transport tests. Entry-point/migration
packages report no test files; migration behavior is exercised by SQL tests.

Not exercised: OS/process/power-loss crashes, PostgreSQL failover, production load,
arbitrary schema drift, external side effects, or future Kafka/worker components.
The COMMIT test injects client protocol transport faults, not a server crash.
No new infrastructure, dispatch, lease renewal, or scheduling is implemented.

Stop at M2 review. Keep the tracking issue open until acceptance review; do not
merge or begin M3 without approval.
