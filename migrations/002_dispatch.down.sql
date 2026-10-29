-- Manual destructive rollback for disposable development databases only.
DROP TABLE dispatch_quarantine;
DROP TABLE dispatch_events;
DELETE FROM schema_migrations WHERE version = 2;
