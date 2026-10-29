# Milestone 3 Part 2 — CDC review

Tracking: [issue #9](https://github.com/su944/distributed-job-engine/issues/9).
User authorization: 2026-10-29, bounded to Kafka, Debezium and Outbox Event Router;
stop before Part 3. Branch: `feat/milestone-3-cdc-dispatch`.

## Preserved work and decisions

Inspected git status, AGENTS, README, architecture, database design and plan.
`compose.cdc.yaml`, `scripts/setup-outbox-publication.sh` and
`docs/milestone-3-cdc-bootstrap.md` were absent. Their functionality already exists
in `compose.yaml` (optional cdc profile) and `cmd/cdc-setup`, so it was reused.
Existing uncommitted dispatcher, migration 002, dependency and documentation
changes, plus `.idea/`, were preserved. No Go consumer was added or extended.
The existing setup applies both existing migrations; no migration SQL or API,
hashing, submission transaction or job schema was changed in this Part 2 work.

Kafka and Connect are necessary to test the explicitly requested CDC path.
Retain existing tested digest pins: PostgreSQL 17, Kafka 4.0.1 single-node KRaft,
Debezium Connect 3.2.4.Final. Internal Kafka address is `kafka:9092`, external
address `127.0.0.1:19092` (override via KAFKA_PORT). This local topology favors
reproducibility over availability; replication factor one is not production HA.
The existing runbook documents topic retention, slots, LSNs, WAL limits and replay.

Part 2 adds explicit router field mappings and a source-topic predicate, disables
schema change output, adds standalone REST registration, and provides a Python
smoke script using Kafka's bundled console observer rather than Go consumers.

## Complete flow

1. POST `/v1/jobs` validates the existing request and idempotency key.
2. A short PostgreSQL transaction atomically commits one job and one outbox row.
   Reusing an equivalent submission key returns the original job without an insert.
3. PostgreSQL writes committed changes to WAL. An aborted insert is never emitted.
4. Debezium's dedicated `jobengine_cdc` LOGIN REPLICATION role reads the
   `jobs_outbox` INSERT-only publication via pgoutput and slot `jobengine_outbox`.
   SELECT on outbox supports the initial snapshot; the role cannot SELECT jobs.
5. The router applies only to `jobengine.public.outbox_events`. It routes to
   `jobs.ready.v1`, uses aggregateid as the string UUID key, expands the existing
   versioned JSON payload as the value, and retains `id` in a header. The envelope
   contains event/job IDs, kind, version and occurrence time, without job payloads.
6. Kafka stores records in three partitions with seven-day delete retention.
   Heartbeats have their own topic; they do not become job notifications.
7. Connect persists source offsets in Kafka. Restart uses retained offsets/slot;
   a crash around offset persistence may replay events with the same event ID.
   Initial snapshots may replay retained outbox rows. This is at-least-once delivery,
   not job execution or an exactly-once guarantee.

## Reproduce on a disposable local stack

Requires Python 3, Go, Docker Compose and the repository root. Tests append unique
jobs/outbox rows and pause/restart the connector, so use a dedicated local stack.
Do not run concurrently with another connector restart test. Keep volumes intact.

```sh
export POSTGRES_PASSWORD='local-test-only'
export POSTGRES_PORT=55432
export CDC_PASSWORD='local-cdc-test-only'
export DATABASE_URL="postgres://jobengine:${POSTGRES_PASSWORD}@127.0.0.1:${POSTGRES_PORT}/jobengine?sslmode=disable"
export KAFKA_BROKERS=127.0.0.1:19092
export CONNECT_URL=http://127.0.0.1:18083
export COMPOSE_PROJECT_NAME=job-engine-m1
docker compose --profile cdc up -d --wait
go run ./cmd/cdc-setup
python3 scripts/register-outbox-connector.py
python3 scripts/register-outbox-connector.py
# Separate terminal, same DATABASE_URL:
HTTP_ADDR=127.0.0.1:18080 go run ./cmd/api
# Original terminal:
API_URL=http://127.0.0.1:18080 python3 scripts/smoke-cdc.py
```

The observer reads retained records and checks only new test job keys, tolerating
historical poison messages from prior dispatcher tests. Each observation finishes
after ten idle seconds; absence alone is not the rollback proof. A later committed
outbox row with the same aggregate key provides the WAL/partition barrier. The
test requires one record for the fresh pre-restart insert in that observation
window, and at least one after restart; it does not assert global exactly-once.
For restart it pauses connector/tasks, submits through HTTP while paused, resumes
and restarts tasks, then verifies both the prior event and backlog event exist.
SQL rollback clones only the test row and changes its event ID; it deletes nothing.

## Actual verification

Executed 2026-10-29 with Go 1.26.4 and Python 3 against retained local Compose
project `job-engine-m1`: PostgreSQL 55432, Kafka 19092, Connect 18083, API 18080.
Go used `GOMODCACHE=/private/tmp/job-engine-modcache` and
`GOCACHE=/private/tmp/job-engine-gocache`. Actual successful checks:

- `docker compose -p job-engine-m1 --profile cdc up -d --wait`: all three healthy.
- `go run ./cmd/cdc-setup`: role/publication/topic/config ready, exit 0.
- `python3 scripts/register-outbox-connector.py`, twice: connector and task RUNNING.
- `API_URL=http://127.0.0.1:18080 python3 scripts/smoke-cdc.py`: exit 0.
  Fresh HTTP job `8ab13979-cece-44eb-98ea-1b8ef677c025`, event
  `635a1349-ae01-41df-bc9b-b287da5617a1`: exactly one observed record with matching
  key, job ID, event ID header and five-field v1 value. Rollback event absent;
  committed barrier `783b8d81-16ab-45cf-8660-fe887e61dddc` observed once.
  After restart the original event remained and paused-backlog event
  `70bc54aa-6e6d-43ed-853c-ebf5be6cad34` for HTTP job
  `96767200-4453-498a-8db1-bd85c0db17cb` appeared once. No duplicates occurred in
  this run; duplicates remain allowed by the delivery contract.
- SQL catalog checks: publication INSERT=true and UPDATE/DELETE/TRUNCATE=false;
  CDC role replication=true, superuser=false, SELECT outbox=true, SELECT jobs=false.
  Slot active, WAL status `reserved`, restart_lsn and confirmed_flush_lsn both
  `0/341DF18` at the observation instant.
- `gofmt -w .`, followed by `gofmt -l .`: exit 0, no remaining formatting output.
- `go build ./...`, `go vet ./...`, `go test ./...`, `go test -race ./...`: exit 0.
- `go test -tags=integration ./internal/storage/... ./internal/api/... -count=1`:
  exit 0; real PostgreSQL storage and API suites passed.
- `python3 -m py_compile scripts/register-outbox-connector.py scripts/smoke-cdc.py`:
  initial script syntax check passed; final scripts also executed successfully.
- `git diff --check`: exit 0.

Setup obstacles resolved: Docker Desktop was stopped, and sandbox network access
initially blocked a Go dependency download; starting Docker and permitted network
execution resolved both. One initial shell command failed before execution due
to an unquoted URL query character; the quoted command above succeeded.
No unresolved verification failures. The pre-existing dispatcher integration
suite was not run for this Part 2 review; the independent smoke exercises CDC
without running that consumer. Production HA, process-crash/power-loss recovery,
WAL loss/slot invalidation, prolonged outages and load remain untested here.

Operational details: [CDC runbook](cdc-runbook.md). Router behavior reference:
[Debezium 3.2 Outbox Event Router](https://debezium.io/documentation/reference/3.2/transformations/outbox-event-router.html).
No push, merge, production deployment, or Part 3 implementation is authorized.
