# Repository Guidelines

## Objective & Architecture
Build a reliable, observable distributed job processing engine using Go,
PostgreSQL, Kafka, and Debezium.

- Design for at-least-once execution; assume duplicate events and executions.
- Treat PostgreSQL as the authoritative store for job state.
- Use a transactional outbox and CDC for job-ready events.
- Use atomic claims, leases, and fencing for ownership.
- Make external side effects idempotent.
- Never hold database transactions during job execution.

## Project Structure
Milestone 4 adds atomic ownership and a bounded worker pool (issue #12), reusing
the Part 1 ownership commit (issue #11) on merged M3. The user explicitly
authorized this scope. Preserve API/hash/migration compatibility. Keep Kafka's
durable receipt/quarantine-before-offset boundary. No retry scheduler, expired-lease
recovery, external integrations, Redis or Kubernetes. Stop before Milestone 5.
Read `docs/architecture.md`, `docs/database-design.md`, and
`docs/implementation-plan.md` before coding. Record milestone approvals.
Keep implementation under `internal/` and tests in `*_test.go` files beside code.
Update README and architecture documentation when designs change.

## Go Style & Development Commands
Prefer standard library, small interfaces, and explicit injection. Use gofmt,
`MixedCaps` identifiers, and lowercase packages. Propagate `context.Context`
through I/O. Bound concurrency, timeouts, and retries. Use `slog` and wrapped
errors with `errors.Is/As`.

Run:

- `gofmt -w .` to format Go source.
- `go build ./...` to compile packages.
- `go vet ./...` for static checks.
- `go test ./...` for tests.
- `go test -race ./...` to check for data races.

Use `docker compose up -d --wait`, `go run ./cmd/migrate`, and `go run ./cmd/api`;
see README and `docs/cdc-runbook.md` for the CDC profile and dispatcher.
The API never runs migrations. Never acknowledge a Kafka record before durable
receipt/quarantine; database failure must leave the offset uncommitted.

## Testing Guidelines
Use `testing` and `TestXxx` names. Test critical transitions, replay and fencing.
Use real PostgreSQL for concurrency tests; document service configuration.
Run `go test -tags=integration ./internal/storage/... ./internal/api/... -count=1` with
`TEST_DATABASE_URL` targeting real PostgreSQL. Tests create isolated schemas and
fail if infrastructure is absent. Never claim a test passed unless executed.

## Issues, Commits & Pull Requests
Use an issue-linked feature branch for each implementation milestone.
Write imperative commit subjects, e.g. `feat: add CDC dispatch (refs #9)`.

Open an issue-linked PR describing behavior, design, checks and results.
Close an issue only after its acceptance
criteria pass. Do not implement future milestones prematurely.

## Approval & Security Boundaries
Inspect `git status` first and preserve existing work. Present design trade-offs
before major architecture changes. Justify new infrastructure. Do not push,
merge, or deploy without approval. Never commit secrets or `.env` files.
