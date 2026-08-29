-- Create queues and jobs tables used by the repositories in:
-- - src/repository/postgresql/queue.repository.ts
-- - src/repository/postgresql/job.repository.ts

CREATE TABLE IF NOT EXISTS queues (
    id                      BIGSERIAL PRIMARY KEY,
    name                    TEXT NOT NULL UNIQUE,
    max_attempts            INTEGER NOT NULL,
    concurrency             INTEGER NOT NULL,
    requires_group_id       BOOLEAN NOT NULL DEFAULT false,
    -- Stored as an integer duration (nanoseconds) to map cleanly to Go's time.Duration.
    lease_duration          BIGINT NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Add comment for documentation
COMMENT ON COLUMN queues.requires_group_id IS 
'Determines if queue requires group coordination: false = can use fast path (if concurrency also 0), true = requires group coordination';

CREATE TABLE IF NOT EXISTS jobs (
    id                  BIGSERIAL PRIMARY KEY,
    idempotency_key     TEXT NOT NULL UNIQUE,
    payload             TEXT NOT NULL,
    status              TEXT NOT NULL,
    group_id            TEXT NULL,
    queue_id            BIGINT NOT NULL,
    queue_shard_no      INTEGER NULL,
    attempts            INTEGER NOT NULL DEFAULT 0,
    metadata            JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at        TIMESTAMPTZ NULL,
    lease_seq           BIGINT NULL,
    lease_expires_at    TIMESTAMPTZ NULL,
    CONSTRAINT fk_job_queue FOREIGN KEY (queue_id) REFERENCES queues(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_job_queue_id_status ON jobs (queue_id, status, created_at);
CREATE INDEX IF NOT EXISTS idx_job_group_id_created_at ON jobs (group_id, created_at);
CREATE INDEX IF NOT EXISTS idx_job_queue_id ON jobs (queue_id);

CREATE TABLE IF NOT EXISTS group_queue_limits (
    queue_id        BIGINT NOT NULL,
    group_id        TEXT NOT NULL,
    max_running     INTEGER NOT NULL,
    running         INTEGER NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (queue_id, group_id),
    CONSTRAINT fk_group_queue_limits_queue FOREIGN KEY (queue_id) REFERENCES queues(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS queue_shards (
    queue_id        BIGINT NOT NULL,
    shard_no        INTEGER NOT NULL,
    max_running     INTEGER NOT NULL,
    running         INTEGER NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (queue_id, shard_no),
    CONSTRAINT fk_queue_shards_queue FOREIGN KEY (queue_id) REFERENCES queues(id) ON DELETE CASCADE
);
