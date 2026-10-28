CREATE TABLE jobs (
    id uuid PRIMARY KEY,
    namespace text NOT NULL CHECK (octet_length(namespace) BETWEEN 1 AND 128),
    idempotency_key text NOT NULL CHECK (octet_length(idempotency_key) BETWEEN 1 AND 128),
    request_hash bytea NOT NULL CHECK (octet_length(request_hash) = 32),
    type text NOT NULL CHECK (type ~ '^[a-z][a-z0-9_.-]{0,127}$'),
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    status text NOT NULL DEFAULT 'READY' CHECK (status IN ('READY', 'RUNNING', 'RETRY_WAIT', 'SUCCEEDED', 'FAILED', 'RECONCILE')),
    attempt_count integer NOT NULL DEFAULT 0,
    max_attempts integer NOT NULL CHECK (max_attempts BETWEEN 1 AND 20),
    replay_safe boolean NOT NULL DEFAULT false,
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_dispatched_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    lease_token bigint NOT NULL DEFAULT 0 CHECK (lease_token >= 0),
    lease_owner text,
    lease_expires_at timestamptz,
    attempt_deadline timestamptz,
    result jsonb,
    last_error_code text,
    reconciliation_note text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at timestamptz,
    CONSTRAINT jobs_namespace_key_unique UNIQUE (namespace, idempotency_key),
    CHECK (attempt_count BETWEEN 0 AND max_attempts),
    CHECK (status NOT IN ('READY', 'RETRY_WAIT') OR attempt_count < max_attempts),
    CHECK ((status = 'RUNNING' AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL
            AND attempt_deadline IS NOT NULL AND attempt_count > 0 AND lease_token > 0
            AND lease_expires_at <= attempt_deadline)
        OR (status <> 'RUNNING' AND lease_owner IS NULL AND lease_expires_at IS NULL AND attempt_deadline IS NULL)),
    CHECK ((status IN ('SUCCEEDED', 'FAILED')) = (finished_at IS NOT NULL))
);

CREATE TABLE outbox_events (
    id uuid PRIMARY KEY,
    aggregatetype text NOT NULL CHECK (aggregatetype = 'jobs'),
    aggregateid uuid NOT NULL REFERENCES jobs(id) ON DELETE RESTRICT,
    type text NOT NULL CHECK (type = 'JobReady'),
    timestamp timestamptz NOT NULL DEFAULT clock_timestamp(),
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object')
);
CREATE INDEX outbox_events_timestamp_id ON outbox_events (timestamp, id);
