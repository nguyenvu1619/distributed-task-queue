-- Schema for the Postgres-backed task queue, as the repositories in
-- src/repository/postgresql are written against it.
--
-- Squashed from the incremental migrations this schema was developed through.
-- Nothing has been published, so the intermediate states are not worth keeping
-- and the comments below explain the final shape rather than the path to it.

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

COMMENT ON COLUMN queues.requires_group_id IS
'Determines if queue requires group coordination: false = can use fast path (if concurrency also 0), true = requires group coordination';

CREATE TABLE IF NOT EXISTS jobs (
    id                  BIGSERIAL PRIMARY KEY,
    -- Unique per queue, not table-wide: see jobs_queue_id_idempotency_key_uniq.
    idempotency_key     TEXT NOT NULL,
    payload             TEXT NOT NULL,
    status              TEXT NOT NULL,
    group_id            TEXT NULL,
    queue_id            BIGINT NOT NULL,
    queue_shard_no      INTEGER NULL,
    attempts            INTEGER NOT NULL DEFAULT 0,
    -- Counts publishes that resolved to this row after the first. The publish
    -- statement needs a value that separates an insert from a deduplicated
    -- conflict, because ON CONFLICT ... DO UPDATE ... RETURNING returns a row
    -- either way and says nothing about which path produced it. A freshly
    -- inserted row is revision 0; the conflict path increments, so anything
    -- above 0 came back as a duplicate.
    revision            INTEGER NOT NULL DEFAULT 0,
    metadata            JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at        TIMESTAMPTZ NULL,
    lease_seq           BIGINT NULL,
    lease_expires_at    TIMESTAMPTZ NULL,
    CONSTRAINT fk_job_queue FOREIGN KEY (queue_id) REFERENCES queues(id) ON DELETE CASCADE
);

-- Idempotency is scoped to the queue. A table-wide unique key would reserve one
-- business id for a single queue, so `order-123` could back a job on the email
-- queue or the invoicing queue but never both — and once publish deduplicates
-- instead of raising, the second publish would return the first queue's job and
-- the second queue would silently never receive anything.
--
-- The publish path's ON CONFLICT infers its arbiter from this index, so the
-- column set here is what decides what "already published" means.
--
-- Uniqueness only ever covers live jobs: completed and failed jobs are deleted
-- rather than kept, so settling a job frees its key for reuse. That is the
-- dedup window, and it is a property of this schema, not of the publish code.
CREATE UNIQUE INDEX IF NOT EXISTS jobs_queue_id_idempotency_key_uniq
  ON jobs (queue_id, idempotency_key);

-- Partial indexes scoped to the status each hot path actually reads. Smaller
-- than the equivalent full indexes, and they keep the scan off the rows in
-- states the query can never return.

-- Pull queries: WHERE queue_id=$1 AND status='PENDING' ORDER BY created_at
CREATE INDEX IF NOT EXISTS idx_job_pending_queue ON jobs (queue_id, created_at)
  WHERE status = 'PENDING';

-- Reaper queries: WHERE status='PROCESSING' AND lease_expires_at <= now()
CREATE INDEX IF NOT EXISTS idx_job_processing_lease ON jobs (lease_expires_at)
  WHERE status = 'PROCESSING';

-- Group-based pull queries: WHERE group_id=$1 AND status='PENDING' ORDER BY created_at
CREATE INDEX IF NOT EXISTS idx_job_pending_group ON jobs (group_id, created_at)
  WHERE status = 'PENDING';

-- The two counter tables below carry a CHECK that makes the admission cap an
-- invariant of the schema rather than a property the pull query is trusted to
-- maintain.
--
-- `running` is incremented by the admission gate in pullJobWithCoordination and
-- decremented by settle and by the reaper. Every one of those is a conditional
-- UPDATE, so the caps already hold — but nothing *proved* it: an over-admit
-- would raise no error and simply run more jobs than configured. Client-side
-- counters cannot close that gap either, because a worker still holds a job for
-- a round trip after the database has released its slot, so a peak measured in
-- the client legitimately exceeds the cap without the cap ever being broken.
--
-- The CHECK is evaluated inside the statement that writes the counter, so an
-- over-admit fails the pull outright instead of passing unnoticed.
--
-- NOTE: this fixes `running` at or below `max_running` at every commit. A future
-- feature that lowers a cap while jobs are in flight would violate it and needs
-- to drain, or to relax this constraint deliberately.

CREATE TABLE IF NOT EXISTS group_queue_limits (
    queue_id        BIGINT NOT NULL,
    group_id        TEXT NOT NULL,
    max_running     INTEGER NOT NULL,
    running         INTEGER NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (queue_id, group_id),
    CONSTRAINT fk_group_queue_limits_queue FOREIGN KEY (queue_id) REFERENCES queues(id) ON DELETE CASCADE,
    CONSTRAINT group_queue_limits_running_within_cap CHECK (running >= 0 AND running <= max_running)
);

CREATE TABLE IF NOT EXISTS queue_shards (
    queue_id        BIGINT NOT NULL,
    shard_no        INTEGER NOT NULL,
    max_running     INTEGER NOT NULL,
    running         INTEGER NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (queue_id, shard_no),
    CONSTRAINT fk_queue_shards_queue FOREIGN KEY (queue_id) REFERENCES queues(id) ON DELETE CASCADE,
    CONSTRAINT queue_shards_running_within_cap CHECK (running >= 0 AND running <= max_running)
);
