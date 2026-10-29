# distributed-job-engine

A Go distributed job engine with PostgreSQL as the authoritative job ledger.
Milestone 1 implements durable submission and status reads, tracked in
[issue #3](https://github.com/su944/distributed-job-engine/issues/3).
Milestone 2 hardens this ledger/API in
[issue #5](https://github.com/su944/distributed-job-engine/issues/5).
Milestone 3 adds Debezium/Kafka CDC and durable Go notification dispatch under
[issue #9](https://github.com/su944/distributed-job-engine/issues/9).
Accepted jobs remain `READY`; no claiming, workers, Redis or Kubernetes exist.
See the [CDC setup and operations runbook](docs/cdc-runbook.md).

## Local setup

Requires Go 1.26+, Docker Compose v2, and curl. Run from the repository root.
The password below is a disposable development example. URL-encode reserved
characters if you choose another password for DATABASE_URL.

```sh
export POSTGRES_PASSWORD='local-dev-only-change-me'
export POSTGRES_PORT=5432
export DATABASE_URL="postgres://jobengine:${POSTGRES_PASSWORD}@127.0.0.1:${POSTGRES_PORT}/jobengine?sslmode=disable"
docker compose up -d --wait postgres
go mod download
go run ./cmd/migrate
go run ./cmd/api
```

The API listens on `127.0.0.1:8080`; PostgreSQL binds to loopback with a named
volume. Existing volumes retain their original password; environment changes do
not rotate credentials. `docker compose stop` preserves data. Avoid `down -v`
unless intentionally deleting local jobs. The PostgreSQL 17 development tag
is now digest-pinned to the existing image; patch upgrades require explicit review.

Migrations are explicit, transactional, advisory-locked, and checksum-verified.
The API never migrates on startup. Re-running the migration command is safe;
do not edit applied SQL. The down script is destructive and manual-only for
disposable databases. No retention cleanup is implemented.

## CDC and dispatcher (Milestone 3)

For the bounded Part 2 review (CDC only), follow
[the Part 2 verification procedure](docs/milestone-3-part-2.md). Existing
dispatcher work is preserved separately; running it is not required for Part 2.

```sh
export CDC_PASSWORD='local-cdc-only-change-me'
export KAFKA_BROKERS=127.0.0.1:19092
export KAFKA_SOURCE=local-v1
export CONNECT_URL=http://127.0.0.1:18083
docker compose --profile cdc up -d --wait
go run ./cmd/cdc-setup
go run ./cmd/dispatcher
```

Use the DATABASE_URL/POSTGRES_PASSWORD from local setup. Export CDC_PASSWORD
before creating Connect. cdc-setup explicitly applies migration 002, configures
a separate replication login/INSERT-only publication and registers the router.
The connector uses an environment secret reference. All infrastructure images
are pinned by digest; Kafka/Connect run only in the optional cdc profile.
Ports: Kafka 19092, Connect 18083, dispatcher health/metrics 8081 (all loopback).

The dispatcher commits a receipt or bounded quarantine record in PostgreSQL
before synchronously acknowledging Kafka. It does not claim, execute or complete
jobs. Replays deduplicate event IDs; failures leave offsets uncommitted and stop
the instance rather than accumulating retry buffers. Buffers, decompression and
per-record I/O are bounded. `/health/ready` checks assignment, broker and DB;
`/metrics` reports low-cardinality notification counters, not job completion.
M4's handoff from receipts or a separate worker consumer group requires review.
Versioned event semantics, compatibility pins, credentials, retention, restarts,
rebalances and recovery are in [docs/cdc-runbook.md](docs/cdc-runbook.md).

## Run the REST API

In another terminal:

```sh
curl -i http://127.0.0.1:8080/health/live
curl -i http://127.0.0.1:8080/health/ready
curl -i -X POST http://127.0.0.1:8080/v1/jobs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: invoice-001' \
  -d '{"type":"invoice.send","payload":{"invoice_id":"inv_001"},"max_attempts":5}'
```

First submission returns 202, a job ID, and `Location: /v1/jobs/{id}`. Repeat
with the same key and logical request: 200 and the original ID. Change payload,
type, or effective retry policy under that key: 409 with:

```json
{"error":{"code":"idempotency_conflict"}}
```

Use the returned ID for status:

```sh
curl -i http://127.0.0.1:8080/v1/jobs/REPLACE_WITH_RETURNED_UUID
```

Responses contain `id`, `status`, `attempts`, `max_attempts`, `created_at`, and
`updated_at`, excluding payload, key, hash, and lease details. Unknown UUIDs
return 404; malformed IDs return 400. Liveness is independent of PostgreSQL;
readiness checks its migration ledger and both submission tables without scanning
job data, and returns 503 when DB/schema is absent
or the service is draining.

Inputs require application/json, one object, an allowed type, object payload,
integer max_attempts 1–20 (default 5), and an opaque 1–128 printable ASCII key
without spaces. The entire body is limited to 256 KiB. Unknown/duplicate keys,
NUL characters, trailing JSON, nesting beyond 64, numbers longer than 1024 bytes, and exponents
outside [-1000,1000] are rejected. Object ordering, whitespace, equivalent payload
decimals, and default policy omission do not change idempotency; array order does.
Submission keys never expire in this milestone.

Handled errors use `{"error":{"code":"..."}}`: 413 body limit, 415 media type,
405 with Allow for methods, 429 with Retry-After for admission saturation, and
503 for database/deadline/shutdown failures. After `submission_outcome_unknown`,
retry with the **same key** because the transaction may already have committed.
Use that same key after any submission timeout or lost connection as well.
Cancellation and deadline errors use `request_canceled` and `request_timeout`
respectively, including body reads. Uncertain COMMIT outcomes retain
`submission_outcome_unknown` even when the underlying error is cancellation.
SIGINT/SIGTERM stops admission and drains active HTTP requests; at the shutdown
deadline remaining contexts are canceled and connections closed.

## Environment configuration

- `DATABASE_URL`: required, never logged.
- `HTTP_ADDR`: `127.0.0.1:8080`; `JOB_NAMESPACE`: `default`, trusted server scope.
- `JOB_TYPES`: comma-separated allow-list, default `invoice.send`; no handlers run.
- `LOG_LEVEL`: `INFO`; standard slog levels accepted.
- `DB_MAX_OPEN`: 10; `DB_MAX_IDLE`: 5; range 1–100 and idle <= open.
- `DB_TIMEOUT`: `3s`, for startup ping, connections, and readiness.
- `REQUEST_TIMEOUT`: `5s`, propagated to SQL operations.
- `HTTP_READ_HEADER_TIMEOUT`: `3s`; `HTTP_READ_TIMEOUT`: `10s`.
- `HTTP_WRITE_TIMEOUT`: `10s`; `HTTP_IDLE_TIMEOUT`: `60s`.
- `SHUTDOWN_TIMEOUT`: `20s`.

Durations must be positive and <=10m. Write timeout must exceed request timeout;
shutdown must cover it; DB timeout cannot exceed it; header timeout cannot exceed
read timeout. Database pool connections and lifetimes are bounded. Active job
requests are capped at 64; health endpoints do not consume admission slots.
Compose reads required `POSTGRES_PASSWORD` and optional `POSTGRES_PORT` (5432).
No .env loader is used; do not commit secrets or .env files. Loopback development
uses a privileged database role and plaintext transport. Public access, ingress
authentication/TLS, and production least-privilege roles require further review.

## Verification

```sh
gofmt -l .
go build ./...
go vet ./...
go test ./...
go test -race ./...
export TEST_DATABASE_URL="$DATABASE_URL"
go test -tags=integration ./internal/storage/... ./internal/api/... -count=1
go test -race -tags=integration ./internal/storage/... ./internal/api/... -count=1
git diff --check
```

Integration requires a real development/test PostgreSQL URL with schema creation
privileges. It fails instead of skipping when unavailable. Tests create random
isolated schemas, apply migrations, and drop only their own schemas. They cover
concurrent idempotency, logical equality/conflicts, namespace isolation, atomic
outbox rollback, SQL constraints, canceled I/O, migration replay/drift, and HTTP
against real SQL. Unit/httptest covers input, config, JSON responses, health,
backpressure, and safe logs. Lifecycle tests bind localhost sockets and exercise
request deadlines plus graceful and forced shutdown.

For real CDC integration, follow the dedicated-stack runbook and export
TEST_KAFKA_BROKERS/TEST_CONNECT_URL as well as TEST_DATABASE_URL:

```sh
go test -tags=integration ./internal/dispatch/... -count=1 -timeout=4m
go test -race -tags=integration ./internal/dispatch/... -count=1 -timeout=4m
```

These tests publish through real PostgreSQL/Debezium/Kafka, restart the local
connector, and exercise quarantine-before-ack and rebalance/shutdown replay.
They fail on absent infrastructure and must not target shared production services.

M2 adds concurrent HTTP races between equivalent and conflicting logical requests,
frozen persisted-hash vectors, observed PostgreSQL lock-wait cancellation, pool
exhaustion/recovery, SQLSTATE constraint assertions, and representative lookup
plans. A test-only protocol transport drops COMMIT before transmission or hides
the real server's successful COMMIT reply. Both cases must return an uncertain
submission and resolve via the original key to exactly one durable job/event pair.
This is local PostgreSQL transport fault injection, not a production failover test.
The existing hash algorithm, submission transaction, migrations, indexes, and
pool implementation are unchanged. Future scheduling indexes remain deferred.

## Structure and architectural decisions

- `cmd/api`: composition root; `cmd/migrate`: explicit migrations.
- `internal/jobs`: domain, validation/canonicalization, repository interface.
- `internal/config`: environment; `internal/storage`: pgx/v5 through database/sql.
- `internal/api`: HTTP; `internal/server`: deadlines/shutdown; `migrations`: SQL.
- `internal/dispatch`: strict envelope validation, SQL receipts/quarantine and Kafka.
- `cmd/dispatcher`: notification process; `cmd/cdc-setup`: explicit local admin setup.

SQL uniquely enforces `(namespace, idempotency_key)`. A short READ COMMITTED
transaction inserts job plus outbox together, with no check-then-insert race.
Debezium now relays committed outbox inserts. Kafka delivery does not imply completion;
ownership will use PostgreSQL leases/fencing. Side effects remain at least once
and require stable provider keys or explicit reconciliation.

The user explicitly moved API/persistence, local PostgreSQL, and basic HTTP
lifecycle from later phases into M1. Atomic outbox submission is retained;
attempt/audit tables, scheduler indexes, handler interfaces, and retry algorithms
are deferred. See:

- [Architecture and REST contract](docs/architecture.md)
- [Seven Mermaid diagrams](docs/architecture-diagram.md)
- [Database design](docs/database-design.md)
- [Failure scenarios](docs/failure-scenarios.md)
- [Revised implementation plan](docs/implementation-plan.md)
- [Go design choices compared with Spring Boot](docs/go-design-choices.md)
- [Executed Milestone 1 verification](docs/milestone-1-verification.md)
- [Milestone 2 review and verification](docs/milestone-2-verification.md)
- [CDC runbook and version 1 event contract](docs/cdc-runbook.md)
- [Milestone 3 verification](docs/milestone-3-verification.md)
- [Contributor instructions](AGENTS.md)

Use issue-linked feature branches and run relevant checks. A linked PR follows
push approval. Do not push, merge, or begin the next milestone without approval.
