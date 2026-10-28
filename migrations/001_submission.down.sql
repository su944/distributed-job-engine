-- Destructive, manual development rollback only. Never run against retained jobs.
DROP TABLE outbox_events;
DROP TABLE jobs;
DELETE FROM schema_migrations WHERE version = 1;
