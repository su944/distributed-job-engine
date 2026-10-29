-- Notification receipts are not job claims or execution results.
CREATE TABLE dispatch_events (
    event_id uuid PRIMARY KEY,
    job_id uuid NOT NULL REFERENCES jobs(id) ON DELETE RESTRICT,
    schema_version integer NOT NULL CHECK (schema_version = 1),
    occurred_at timestamptz NOT NULL,
    envelope_hash bytea NOT NULL CHECK (octet_length(envelope_hash) = 32),
    received_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE dispatch_quarantine (
    source text NOT NULL CHECK (octet_length(source) BETWEEN 1 AND 128),
    consumer_group text NOT NULL CHECK (octet_length(consumer_group) BETWEEN 1 AND 128),
    topic text NOT NULL CHECK (octet_length(topic) BETWEEN 1 AND 249),
    partition integer NOT NULL CHECK (partition >= 0),
    record_offset bigint NOT NULL CHECK (record_offset >= 0),
    reason text NOT NULL CHECK (reason IN ('invalid_json','invalid_envelope','unsupported_version','key_mismatch','unknown_job','event_id_conflict')),
    key_prefix bytea NOT NULL CHECK (octet_length(key_prefix) <= 1024),
    value_prefix bytea NOT NULL CHECK (octet_length(value_prefix) <= 8192),
    key_hash bytea NOT NULL CHECK (octet_length(key_hash) = 32),
    value_hash bytea NOT NULL CHECK (octet_length(value_hash) = 32),
    key_bytes bigint NOT NULL CHECK (key_bytes >= 0),
    value_bytes bigint NOT NULL CHECK (value_bytes >= 0),
    received_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (source,consumer_group,topic,partition,record_offset)
);
