# distributed-job-engine
Distributed job processing engine in Go with PostgreSQL, Kafka, CDC, durable execution, retries, and observability

## Status

Phase 0: architecture proposal only. No API, migrations, workers, or infrastructure
have been implemented. Architecture review is required before Milestone 1.

## Design documents

- [Architecture and proposed REST contract](docs/architecture.md)
- [Seven Mermaid architecture and lifecycle diagrams](docs/architecture-diagram.md)
- [Database design and transaction boundaries](docs/database-design.md)
- [Failure scenarios and verification obligations](docs/failure-scenarios.md)
- [Seven implementation milestones and review gates](docs/implementation-plan.md)
- [Repository contributor instructions](AGENTS.md)

## Important architectural decisions

- PostgreSQL is authoritative for state and ownership; short transactions use
  atomic claims, leases, and monotonic fencing tokens.
- Submission writes a job and outbox event atomically. Debezium CDC sends
  wake-ups through Kafka; Kafka acknowledgement is not job completion.
- Execution is at least once. Stable provider idempotency keys and explicit
  reconciliation handle unknown side effects; exactly-once external effects are
  not promised. Database fencing cannot stop a stale external HTTP call.
- A bounded recovery scheduler handles expired owners, delayed retries, and
  overdue READY notifications even with CDC in place.
- Worker slots, event buffers, retries, scans, and admission are bounded.
  Handlers run outside database transactions.

These decisions and their polling/CDC alternatives remain subject to architecture
review. See the design documents for open questions and proposed limits.

## Development workflow

Reference a GitHub issue for implementation, use a feature branch, run relevant
tests, and open a linked PR. Close issues only after acceptance criteria pass.
Push, merge, and deployment require approval. There is no runnable Go module or
local startup command yet; future verification commands are specified in the plan.
