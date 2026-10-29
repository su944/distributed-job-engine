-- Destructive, manual-only rollback for disposable databases.
DROP INDEX jobs_ready_inbox;
DROP INDEX dispatch_events_job_id;
DELETE FROM schema_migrations WHERE version=4;
