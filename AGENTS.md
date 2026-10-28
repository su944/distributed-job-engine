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
Milestone 1 is the user-authorized submission API, PostgreSQL persistence, and
local PostgreSQL Compose setup (issue #3). No workers, Kafka, Debezium, Redis,
or Kubernetes yet. Stop for review before starting subsequent milestones.
Read `docs/architecture.md`, `docs/database-design.md`, and
`docs/implementation-plan.md` before coding. Record milestone approvals.
Keep implementation under `internal/` and tests in `*_test.go` files beside code.
Update README and architecture documentation when designs change.

## Go Style & Development Commands
Prefer standard library, small interfaces, and explicit injection. Use gofmt,
`MixedCaps` identifiers, and lowercase packages. Propagate `context.Context`
through I/O. Bound concurrency, timeouts, and retries. Use `slog` and wrapped
errors with `errors.Is/As`.

Once a Go module exists, run:

- `gofmt -w .` to format Go source.
- `go build ./...` to compile packages.
- `go vet ./...` for static checks.
- `go test ./...` for tests.
- `go test -race ./...` to check for data races.

Use `docker compose up -d --wait`, `go run ./cmd/migrate`, and `go run ./cmd/api`;
see README for required environment settings. The API never runs migrations.

## Testing Guidelines
Use Go's `testing` package and `TestXxx` function names. Test every critical state
transition, duplicate delivery, retries, leases, and fencing. Use real PostgreSQL
for concurrency integration tests. Document required services and configuration.
Run `go test -tags=integration ./internal/storage/... -count=1` with
`TEST_DATABASE_URL` targeting real PostgreSQL. Tests create isolated schemas and
fail if infrastructure is absent. Never claim a test passed unless executed.

## Issues, Commits & Pull Requests
Use an issue-linked feature branch for each implementation milestone.
Write imperative commit subjects, e.g. `feat: add submission API (refs #3)`.

Open a PR linked to its issue, describing behavior, architectural decisions,
validation commands, and results. Close an issue only after its acceptance
criteria pass. Do not implement future milestones prematurely.

## Approval & Security Boundaries
Inspect `git status` first and preserve existing work. Present design trade-offs
before major architecture changes. Justify new infrastructure. Do not push,
merge, or deploy without approval. Never commit secrets or `.env` files.
