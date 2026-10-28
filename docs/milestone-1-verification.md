# Milestone 1 verification record

Executed locally on 2026-10-28 using Go 1.26.4, pgx/v5 v5.11.0, and real
PostgreSQL from the PostgreSQL 17 Compose service. Work is on
`feat/milestone-1-submission-api`, tracked by
[issue #3](https://github.com/su944/distributed-job-engine/issues/3).

## Passed

- `gofmt -w cmd internal migrations`, followed by empty `gofmt -l` output.
- `go build ./...` and `go vet ./...`.
- `go test ./...` and `go test -race ./...`.
- `go test -tags=integration ./internal/storage/... -count=1`.
- `go test -race -tags=integration ./internal/storage/... -count=1`.
- Final combined run: `go test -race -tags=integration ./... -count=1`.
- `git diff --check`, local Markdown links, balanced fences, and diagram count.

Go caches were placed under `/private/tmp`. Integration used TEST_DATABASE_URL
for the Compose project `job-engine-m1` on loopback port 55432, with a disposable
development password. No tests were skipped for absent PostgreSQL.

Real SQL tests cover 24 concurrent same-key submissions producing one job/event,
logical numeric/default equivalence, differing request conflicts, namespace
isolation, atomic rollback when the outbox insert fails, canceled I/O, SQL
constraints, migration replay/checksum drift, and readiness after ledger loss or
database closure. HTTP tests cover validation, JSON/status contracts, admission,
safe logs, context propagation, deadlines, and graceful/forced shutdown.

Built-command smoke verification applied migration 1, started the real API on
loopback port 18080, and verified health 200, POST 202, logically equivalent
replay 200 with the same ID, conflicting POST 409, and GET 200 with READY state.
SIGTERM drained the API and returned process exit code 0. That API process exited;
the test PostgreSQL container was stopped afterward, with its named volume retained.

## Limits and resolved execution failures

The first sandboxed HTTP tests could not bind localhost sockets. Running them
with approved socket access passed. Module proxy access and Docker also required
approved access outside the sandbox. No unresolved test failures remain.

No worker/CDC/Kafka behavior, external side effects, production failover, or
wire-level lost-COMMIT-ack fault injection was executed. Commit-ambiguity response
mapping is covered by a repository stub, not a network fault experiment. Mermaid
rendering is not part of this implementation's checks.

The user subsequently authorized pushing this milestone for PR review. Issue #3
remains open for acceptance review. Implementation stops here pending Milestone 1
review; no merge or subsequent implementation is authorized.
