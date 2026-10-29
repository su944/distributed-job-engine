CREATE TABLE job_attempts (
    job_id uuid NOT NULL REFERENCES jobs(id) ON DELETE RESTRICT,
    attempt_no integer NOT NULL CHECK (attempt_no BETWEEN 1 AND 20),
    lease_token bigint NOT NULL CHECK (lease_token > 0),
    worker_id text NOT NULL CHECK (octet_length(worker_id) BETWEEN 1 AND 128),
    started_at timestamptz NOT NULL,
    deadline_at timestamptz NOT NULL CHECK (deadline_at > started_at),
    ended_at timestamptz,
    outcome text CHECK (outcome IN ('SUCCEEDED','PERMANENT','UNKNOWN','LEASE_EXPIRED','RETRYABLE')),
    error_code text CHECK (error_code ~ '^[a-z][a-z0-9_.-]{0,127}$'),
    PRIMARY KEY (job_id, attempt_no),
    UNIQUE (job_id, lease_token),
    CHECK ((ended_at IS NULL) = (outcome IS NULL)),
    CHECK (ended_at IS NULL OR ended_at >= started_at)
);
CREATE UNIQUE INDEX job_attempts_one_active ON job_attempts(job_id) WHERE ended_at IS NULL;
