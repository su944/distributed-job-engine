# M3 CDC and dispatch runbook

Development stack only. Read [architecture](architecture.md) and
[database design](database-design.md). No claiming or job execution exists.
Issue: [#9](https://github.com/su944/distributed-job-engine/issues/9).

## Versions and local startup

Compose pins all three images by digest: the existing PostgreSQL 17 image is
retained unchanged; Apache Kafka 4.0.1 runs single-node KRaft; Debezium Connect
3.2.4.Final supplies the PostgreSQL connector/router. Debezium's 3.2 compatibility
matrix includes PostgreSQL 17 and Kafka Connect 3.1+ / Kafka 4.x. Registry lookup
confirmed the chosen image; the newer documented 3.2.7 artifact did not have a
matching image. These are tested compatibility pins, not a claim of latest
security patches. Production release requires version/CVE and HA review.
franz-go v1.22.2 and kadm v1.19.0 are pinned; Go 1.26 is already required.

From the repository root (example passwords are disposable development values):

```sh
export POSTGRES_PASSWORD='local-dev-only-change-me'
export CDC_PASSWORD='local-cdc-only-change-me'
export DATABASE_URL="postgres://jobengine:${POSTGRES_PASSWORD}@127.0.0.1:5432/jobengine?sslmode=disable"
export KAFKA_BROKERS=127.0.0.1:19092
export KAFKA_SOURCE=local-v1
export CONNECT_URL=http://127.0.0.1:18083
docker compose --profile cdc up -d --wait
go run ./cmd/cdc-setup
go run ./cmd/dispatcher
```

Run the existing API in another terminal, `go run ./cmd/api`, and submit using
the README POST example. Inspect `/health/live`, `/health/ready`, and `/metrics`
on `127.0.0.1:8081`. `DISPATCH_HTTP_ADDR` overrides that address; `DISPATCH_GROUP`
defaults to `job-engine-dispatch-v1`. The ordinary PostgreSQL-only Compose command
still works without the cdc profile. `POSTGRES_PORT`, `KAFKA_PORT`, `CONNECT_PORT`
override published ports; also update corresponding URLs/brokers explicitly.

`cdc-setup` is an explicit admin operation: applies checksummed migrations,
creates/updates `jobengine_cdc` with LOGIN REPLICATION, grants database CONNECT,
public schema USAGE, and SELECT only on outbox_events. It creates and verifies
INSERT-only publication `jobs_outbox`. The connector creates slot
`jobengine_outbox` using pgoutput and never drops it on stop. PostgreSQL uses
logical WAL, four slots/senders, and a 1 GiB retained-WAL slot limit. Existing
volumes must be restarted with these settings; no data-volume recreation is needed.

The captured database/schema/table are deliberately fixed to the Compose
`jobengine.public.outbox_events` layout. `cdc-setup` runs with the local admin
URL; the connector uses the separate replication login. Connect gets its password
via `${env:CDC_PASSWORD}` and a CONNECT_CONFIG_PROVIDERS env provider, not a
checked-in connector secret. Export CDC_PASSWORD before starting Connect; an
empty provider value fails connector validation. Rotation requires pausing the
connector, updating the role via setup and the Connect container environment,
then resuming and verifying RUNNING. No secrets or .env files belong in Git.
Loopback PLAINTEXT and an unauthenticated local Connect API are not production
security; use trusted ingress/networking, TLS/SASL and restricted roles there.

## Version 1 JobReady wire contract

For CDC-only verification without the dispatcher, use the
[Part 2 smoke procedure](milestone-3-part-2.md). After initial `cdc-setup`,
`python3 scripts/register-outbox-connector.py` idempotently PUTs the checked-in
configuration and waits up to 90 seconds for connector and task RUNNING. It does
not reset offsets, recreate topics, apply migrations or modify replication roles.
Connect must already have CDC_PASSWORD injected in its environment. Repeating
registration is safe; a failed existing task may need the explicit REST restart
below after its underlying fault is repaired.

The router predicate matches only `jobengine.public.outbox_events` before topic
routing. Heartbeats stay on `__debezium-heartbeat.jobengine`; schema change output
is disabled. The `id` column is explicitly the event ID, retained in Kafka's
`id` header as well as `payload.event_id`. Only outbox payload fields are emitted,
never `jobs.payload`. Kafka Connect persists configuration, offsets and status in
its broker topics (compacted internal topics, local replication factor one),
which survive Connect container recreation through Kafka's named data volume.

Topic `jobs.ready.v1` has three partitions, delete retention of seven days,
replication factor one for local development, and maximum batch/message bytes
1 MiB. Source key is the aggregateid UUID encoded with StringConverter. The
router expands payload JSON using JsonConverter without schemas:

```json
{"schema_version":1,"event_id":"78aad7c3-8c81-4a65-88b4-a4ba1e115f77","job_id":"0b4027ce-0686-4191-8e77-d3bc5a9d944d","kind":"JobReady","occurred_at":"2026-10-28T10:00:00Z"}
```

All five fields are required. Exactly one UTF-8 object, <=8192 bytes; duplicate,
unknown, null or wrongly typed fields fail validation. IDs must be UUID-shaped;
timestamps must parse as RFC3339 and be nonzero. Version must be 1 and kind
JobReady. IDs normalize to lowercase and timestamps UTC; the string Kafka key
must equal the canonical job UUID. Validation never trusts a notification for
ownership or completion. A well-formed but unknown database job is quarantined.
The envelope already emitted by M1 is unchanged. Incompatible future versions
require a reviewed decoder/topic change; they currently go to quarantine.

The PostgreSQL publication emits only INSERT changes. Initial snapshot reads
also export existing committed outbox rows, deliberately recovering submissions
accepted before CDC was configured. Updates/deletes/truncates are excluded;
outbox pruning/cleanup is not implemented. Unexpected router operations fail
the connector rather than silently discarding data. Connect errors.tolerance=none.

## Durable outcomes, offsets and rebalances

For a valid notification the dispatcher commits a dispatch_events receipt. An
identical event ID/envelope hash is a duplicate; conflicting ID reuse is
quarantined. Malformed/unsupported/unknown-job events commit quarantine with
source/group/topic/partition/offset, reason, hashes, byte counts and bounded
previews (1 KiB key, 8 KiB value). Preview bytes can contain sensitive material:
restrict SQL access. Prefix truncation is explicit, not a claim of full payload
preservation. No automatic quarantine/inbox deletion is implemented yet.

Only after that transaction commits does the consumer synchronously commit
offset+1 for the handled record. Auto-commit is disabled; sequential processing
makes each partition's acknowledgement contiguous. A later DB/offset error stops
the process and replays on restart; there is no in-memory retry queue. Duplicate
database commits and lost Kafka commit replies are safe. No acknowledgement
means completion or claiming. M4 must explicitly choose a durable-receipt handoff
or a separate worker Kafka group; current receipts and READY jobs remain intact.

BlockRebalanceOnPoll holds ownership around one bounded record, then releases it.
Store and commit each get 5s; rebalance budget is 30s. One concurrent fetch is
capped at 1 MiB, broker reads at 2 MiB, decompression at 1 MiB. Oversized transport
batches that cannot be decoded stop the partition/process; they are not silently
skipped. New groups start at the beginning. Expired/truncated committed offsets
stop with an out-of-range error, requiring operator recovery rather than reset.
Source epoch identifies a Kafka cluster/topic incarnation: change KAFKA_SOURCE
after replacement. Different bytes at an existing quarantine coordinate halt
processing instead of overwriting audit evidence. Event IDs remain globally
deduplicated across groups/sources in the same PostgreSQL ledger.

SIGTERM cancels polling/current I/O; pending offsets are left uncommitted. HTTP
drains and the client permits rebalance/leaves its group with a 3s leave deadline.
After a failed leave, session expiry may delay reassignment by up to 10s. Store
commit uncertainty is resolved by replay, never by changing job state.

## Health, monitoring and outages

Dispatcher readiness requires a running consumer with assigned partitions,
database migration 002/tables, and a broker ping. Standby instances with no
partitions are unready but live. An idle healthy consumer remains ready.
`/metrics` exposes process-local received/duplicate/quarantine/error counters and
assigned partition gauge, without job labels. Counters reset on restart and
are not completion/unique-execution counts. Quarantine growth, disk and retained
receipts require separate storage capacity monitoring; row counts are not capped.

Inspect the connector and consumer lag:

```sh
curl -fsS "$CONNECT_URL/connectors/job-engine-outbox/status"
docker compose --profile cdc exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 --describe --group job-engine-dispatch-v1
docker compose exec postgres psql -U jobengine -d jobengine -c \
  "SELECT slot_name, active, restart_lsn, confirmed_flush_lsn, wal_status, pg_wal_lsn_diff(pg_current_wal_lsn(),restart_lsn) AS retained_bytes FROM pg_replication_slots;"
```

Alert on FAILED connector/task, READY/outbox age, consumer lag approaching the
seven-day retention window, quarantine/error rates, slot inactivity/WAL bytes and
database/broker disk usage. Dispatcher readiness alone does not prove CDC is
caught up: inspect Connect state and WAL/consumer lag too. Capture-only traffic
heartbeats use a pre-created heartbeat topic; monitor confirmed_flush_lsn during
low outbox activity. This milestone does not provide a full monitoring backend.

- **Broker outage:** readiness fails; no successful offset commits. Bounded
  client fetches may wait for recovery, or fatal/commit errors stop the instance.
  Restart the dispatcher after repair. Connect retries and WAL/outbox accumulate.
- **DB outage/quarantine write failure:** stop with offset uncommitted. Restore DB
  and restart the same group/source. Do not skip malformed records to restore lag.
- **Connector restart:** preserve Kafka internal config/offset/status topics,
  PostgreSQL data and slot. `docker compose --profile cdc restart connect` or the
  REST restart below must eventually report connector and task RUNNING:

```sh
curl -fsS -X POST "$CONNECT_URL/connectors/job-engine-outbox/restart?includeTasks=true&onlyFailed=false"
```

Connect resumes from stored offsets; replay is permitted. Re-running cdc-setup
updates the same reviewed config, not offsets. Do not delete/recreate its internal
topics, reset offsets or drop a live slot during routine restart. Kafka and DB
named volumes are retained by `docker compose --profile cdc stop`; avoid down -v.

## Recovery after lost WAL, offsets, topics or slots

The 1 GiB slot limit is enforced around checkpoints, not a hard disk quota; it
can invalidate a lagged slot. Kafka retention
can likewise expire unread notifications. No current scheduler repairs either.
Never claim continuity merely because a new connector starts RUNNING.

1. Stop dispatch and pause the old connector. Back up configuration/offsets and
   the database. Confirm all needed outbox rows still exist and jobs are intact.
2. Review a recovery connector config based on deploy/debezium-outbox.json with
   a new connector name, unique slot and topic.prefix, snapshot.mode=initial,
   the same INSERT-only publication and jobs.ready.v1 routing. Pre-create its
   heartbeat topic. Do not reuse/delete the old slot or offsets as a shortcut.
3. Start the replacement and verify snapshot plus streaming progress against
   retained event IDs. Start a deliberately selected group/source epoch from the
   beginning. Durable receipts deduplicate replay; quarantine is not execution.
4. Retire the old connector/slot only after operator acceptance and backup. If
   required outbox rows are missing, recovery needs an audited job reconciliation
   plan; this milestone cannot guarantee reconstruction. Do not truncate tables.

Quarantine replay is similarly deliberate: inspect safely, repair the cause, then
republish a corrected envelope with its original logical event ID when valid.
Conflicting ID reuse requires reconciliation, not overwriting the original
receipt. Resetting offsets or rotating source epochs requires an operator record.

## Real integration tests

Use a dedicated disposable stack: tests pause/resume/restart job-engine-outbox
and append jobs/outbox/receipts/quarantine. They do not clear existing data.
After startup/setup above:

```sh
export TEST_DATABASE_URL="$DATABASE_URL"
export TEST_KAFKA_BROKERS="$KAFKA_BROKERS"
export TEST_CONNECT_URL="$CONNECT_URL"
go test -tags=integration ./internal/dispatch/... -count=1 -timeout=4m
go test -race -tags=integration ./internal/dispatch/... -count=1 -timeout=4m
```

Missing dependencies/configuration fail rather than skip. Tests cover committed
vs rolled-back inserts, real duplicate production, connector pause/restart,
quarantine persistence failure before broker acknowledgement, and a real group
rebalance recovering interrupted work. They assert jobs stay READY/attempts=0.
These tests do not establish production failover, crash/power-loss durability,
long outage capacity, slot invalidation repair or exactly-once side effects.

Primary references: [Debezium 3.2 compatibility](https://debezium.io/releases/3.2/),
[outbox router](https://debezium.io/documentation/reference/3.2/transformations/outbox-event-router.html),
[Kafka Docker](https://kafka.apache.org/40/getting-started/docker/),
[franz-go group/fetch options](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo),
[PostgreSQL logical replication security](https://www.postgresql.org/docs/17/logical-replication-security.html).
