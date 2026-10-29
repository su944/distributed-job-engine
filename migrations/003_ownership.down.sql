-- Destructive, manual-only rollback for disposable databases.
DROP TABLE job_attempts;
DELETE FROM schema_migrations WHERE version = 3;
