-- Create queue and job tables used by the repositories in:
-- - internal/repository/postgresql/queue.go
-- - internal/repository/postgresql/job.go

CREATE TABLE IF NOT EXISTS queue (
    id             BIGSERIAL PRIMARY KEY,
    name           TEXT NOT NULL UNIQUE,
    max_attempts   INTEGER NOT NULL,
    -- Stored as an integer duration (nanoseconds) to map cleanly to Go's time.Duration.
    lease_duration BIGINT NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS job (
    id                  BIGSERIAL PRIMARY KEY,
    idempotency_key     TEXT NOT NULL UNIQUE,
    payload             TEXT NOT NULL,
    status              TEXT NOT NULL,
    group_id            TEXT NOT NULL,
    queue_id            BIGINT NOT NULL,
    attempts            INTEGER NOT NULL DEFAULT 0,
    -- Stored as JSON for flexibility (note: scanning into a Go struct requires custom decoding).
    metadata            JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at        TIMESTAMPTZ NULL,
    lease_token         BIGINT NULL,
    lease_by            TEXT NULL,
    lease_expires_at    TIMESTAMPTZ NULL,
    CONSTRAINT fk_job_queue FOREIGN KEY (queue_id) REFERENCES queue(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_job_status_created_at ON job (status, created_at);
CREATE INDEX IF NOT EXISTS idx_job_group_id_created_at ON job (group_id, created_at);
CREATE INDEX IF NOT EXISTS idx_job_queue_id ON job (queue_id);

CREATE TABLE IF NOT EXISTS queue_permits (
    queue_id        BIGINT NOT NULL,
    slot            INT NOT NULL,
    lease_token     UUID,
    leased_by       TEXT,
    lease_expires_at TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (queue_id, slot),
    CONSTRAINT fk_queue_permits_queue FOREIGN KEY (queue_id) REFERENCES queue(id) ON DELETE CASCADE
);




