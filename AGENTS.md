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
The repository is in Phase 0: architecture design only. Do not add application
code, migrations, workers, or infrastructure before architecture approval.
Read `docs/architecture.md`, `docs/database-design.md`, and
`docs/implementation-plan.md` before coding. Record approval at each milestone's
review gate. Keep implementation packages
under `internal/` and Go tests beside their packages in `*_test.go` files.
Follow the implementation plan. Record important architectural decisions in
`README.md` and update architecture documentation when designs change.

## Go Style & Development Commands
Prefer the standard library, small interfaces, and explicit dependency injection.
Use Go formatting with tab indentation, `MixedCaps` identifiers, and short,
lowercase package names. Propagate `context.Context` through I/O boundaries.
Bound goroutines, queues, timeouts, and retries. Use `slog`, `errors.Is/As`, and
meaningful error wrapping.

Once a Go module exists, run:

- `gofmt -w .` to format Go source.
- `go build ./...` to compile packages.
- `go vet ./...` for static checks.
- `go test ./...` for tests.
- `go test -race ./...` to check for data races.

No local service startup command is configured yet.

## Testing Guidelines
Use Go's `testing` package and `TestXxx` function names. Test every critical state
transition, duplicate delivery, retries, leases, and fencing. Use real PostgreSQL
for concurrency integration tests. Document required services and configuration.
No coverage threshold is configured. Never claim a test passed unless executed.

## Issues, Commits & Pull Requests
Reference a GitHub issue for every implementation task. Work on a feature branch
and one milestone at a time. History currently contains only `Initial commit`;
use concise, imperative commit subjects describing the change.

Open a PR linked to its issue, describing behavior, architectural decisions,
validation commands, and results. Close an issue only after its acceptance
criteria pass. Do not implement future milestones prematurely.

## Approval & Security Boundaries
Inspect `git status` first and preserve existing work. Present design trade-offs
before major architecture changes. Justify new infrastructure. Do not push,
merge, or deploy without approval. Never commit secrets or `.env` files.
