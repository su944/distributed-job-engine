# Go choices compared with Java Spring Boot

## Module, packages, and injection

`go.mod` pins the import path and pgx/v5 like Maven coordinates and dependencies.
`cmd/api` is the composition root, analogous to the Boot entry point. `internal/`
is an enforced Go import boundary, not just a package naming convention.
Constructors receive repository, readiness function, logger, and settings rather
than using `@Autowired` or component scanning. Tests substitute dependencies
directly without booting an application context.

## Domain and repository

`jobs.Job` is a plain struct, not an annotated JPA entity. Typed constants define
statuses without implementing future execution transitions. A small
`jobs.Repository` interface is a persistence port, comparable to a narrow Spring
repository abstraction. Its implementation uses explicit SQL rather than Spring
Data generation. No speculative worker or handler interface is introduced yet.

## PostgreSQL and transactions

pgx/v5's stdlib adapter drives `database/sql`, like PostgreSQL JDBC beneath a
DataSource. `sql.DB` is a concurrent pool, not a connection; its bounds play the
role of HikariCP settings. A second pgxpool would duplicate pooling and is avoided.
Parameterized SQL replaces ORM query generation.

Explicit `BeginTx`/`Commit` with deferred `Rollback` replaces proxy-driven
`@Transactional`. SQL uniqueness and ON CONFLICT coordinate different processes;
an `exists` query or Java synchronized block cannot guarantee that. A separate
READ COMMITTED SELECT reads the committed winner after a conflicting insert.
Job and outbox share one short transaction; no network publishing happens inside.

The explicit migration command is a small Flyway-like version/checksum ledger
with an advisory lock. M3 extends it to ordered migrations 1 and 2 while retaining
the original checksum. Startup does not change schema. Manual down SQL is
for disposable development databases, not a production rollback policy.

## Configuration, context, and errors

Validated environment parsing replaces `@ConfigurationProperties` plus Bean
Validation. Injecting a lookup function avoids global environment mutation in
tests. Startup errors omit DSNs and credentials.

`context.Context` propagates deadlines/cancellation through HTTP and SQL calls.
Unlike relying on a Java thread interrupt, each I/O boundary explicitly accepts
the cancellation contract. Errors are returned, wrapped with `%w`, and classified
with `errors.Is/As` rather than unchecked exceptions and `@ControllerAdvice`.
Commit errors conservatively report an unknown submission: clients resolve them
with the original key instead of blindly creating another job.

## HTTP and exact JSON

Standard `net/http` replaces MVC and an embedded servlet container. `httptest`
exercises the handler with injected ports. A shared error envelope centralizes
response mapping. Validation explicitly bounds bodies/depth, rejects duplicate
keys, and disallows unknown fields rather than relying on Jackson defaults.

Canonical hashing sorts object keys and normalizes exact decimal payload values,
analogous to BigDecimal rather than double. It avoids float64 precision loss
for large integers. Effective default retry policy is included in the hash;
opaque submission keys remain untouched. SQL supplies the final uniqueness gate.

## Logging, health, and lifecycle

`slog` JSON logs resemble SLF4J with a JSON encoder. SQL parameters, keys, payloads,
and raw driver errors are excluded because they can contain secrets. Separate
liveness and database/schema readiness serve the role of distinct Actuator probes.

Server header/read/write/idle deadlines and TimeoutHandler bound request time.
A semaphore provides bounded admission like a servlet filter with a bulkhead.
SIGTERM initiates an independent shutdown deadline, comparable to Boot graceful
shutdown. Requests drain before their contexts are canceled; deadline expiry
forces closure. The SQL pool closes after HTTP drain, not at the first signal.

## Tests and infrastructure

Go `testing` and `httptest` replace JUnit/MockMvc here; small stubs avoid a mocking
framework. Integration build tags explicitly require real PostgreSQL with random
isolated schemas rather than an in-memory SQL substitute. `go test -race` detects
Go shared-memory races; separate SQL concurrency tests prove cross-connection
idempotency. M1 used only PostgreSQL; M3 adds an optional, pinned Kafka/Connect
profile and a separate notification dispatcher without introducing worker execution.

Primary references: [pgx stdlib](https://pkg.go.dev/github.com/jackc/pgx/v5/stdlib)
documents the adapter, and [PostgreSQL READ COMMITTED](https://www.postgresql.org/docs/current/transaction-iso.html#XACT-READ-COMMITTED)
explains the separate conflict follow-up statement.
