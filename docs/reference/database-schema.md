# Database schema

Created by migration `000001_initial_schema`. Table names are unqualified, so
the tables live in the first schema on the connection's `search_path`, usually
`public`.

Reading these tables is always safe. Writing to them by hand while workers are
running can break the concurrency caps: the counters in `queue_shards` and
`group_queue_limits` must match the jobs that hold slots.

## queues

One row per queue.

| Column              | Type        | Notes |
| ------------------- | ----------- | ----- |
| `id`                | BIGSERIAL   | Primary key. |
| `name`              | TEXT        | Unique. |
| `max_attempts`      | INTEGER     | |
| `concurrency`       | INTEGER     | `0` means no cap. |
| `requires_group_id` | BOOLEAN     | Default `false`. |
| `lease_duration`    | BIGINT      | **Nanoseconds.** The API takes and returns milliseconds. |
| `created_at`        | TIMESTAMPTZ | |
| `updated_at`        | TIMESTAMPTZ | |

## jobs

One row per live job. Completed and failed jobs are deleted.

| Column             | Type        | Notes |
| ------------------ | ----------- | ----- |
| `id`               | BIGSERIAL   | Primary key. |
| `queue_id`         | BIGINT      | References `queues(id)`, `ON DELETE CASCADE`. |
| `idempotency_key`  | TEXT        | Unique together with `queue_id`. |
| `payload`          | TEXT        | |
| `status`           | TEXT        | `'PENDING'` or `'PROCESSING'`. |
| `group_id`         | TEXT        | Nullable. |
| `queue_shard_no`   | INTEGER     | Shard slot held while `PROCESSING` on a capped queue. Nullable. |
| `attempts`         | INTEGER     | Leases taken so far. Default `0`. |
| `available_at`     | TIMESTAMPTZ | Earliest time a pull may take the job; also its place in line. |
| `revision`         | INTEGER     | How many duplicate publishes resolved to this row. Default `0`. |
| `metadata`         | JSONB       | Default `'{}'`. |
| `lease_seq`        | BIGINT      | Fence token, exposed as `Job.lockSeq`. Default `0`, meaning never leased. |
| `lease_expires_at` | TIMESTAMPTZ | Set while `PROCESSING`. Nullable. |
| `created_at`       | TIMESTAMPTZ | |
| `updated_at`       | TIMESTAMPTZ | |
| `completed_at`     | TIMESTAMPTZ | Unused, since settled rows are deleted. |

Indexes:

| Index                                | Definition                                              | Serves |
| ------------------------------------ | ------------------------------------------------------- | ------ |
| `jobs_queue_id_idempotency_key_uniq` | `UNIQUE (queue_id, idempotency_key)`                    | Deduplication |
| `idx_job_pending_queue`              | `(queue_id, available_at) WHERE status = 'PENDING'`     | Pulls |
| `idx_job_processing_lease`           | `(lease_expires_at) WHERE status = 'PROCESSING'`        | The reaper |
| `idx_job_pending_group`              | `(group_id, created_at) WHERE status = 'PENDING'`       | Not used by the current queries |

## queue_shards

Slot counters for a queue's `concurrency` cap. A queue with `concurrency: N`
gets `min(N, 32)` rows whose `max_running` values add up to N. A queue with
`concurrency: 0` has none.

| Column        | Type        | Notes |
| ------------- | ----------- | ----- |
| `queue_id`    | BIGINT      | References `queues(id)`, `ON DELETE CASCADE`. |
| `shard_no`    | INTEGER     | Primary key together with `queue_id`. |
| `max_running` | INTEGER     | Slots in this shard. |
| `running`     | INTEGER     | Slots in use. `CHECK (running >= 0 AND running <= max_running)`. |
| `created_at`  | TIMESTAMPTZ | |
| `updated_at`  | TIMESTAMPTZ | |

## group_queue_limits

Slot counters for group caps. A row is created by the first publish that names
a group in a queue with a positive `concurrency`, and is never deleted.

| Column        | Type        | Notes |
| ------------- | ----------- | ----- |
| `queue_id`    | BIGINT      | References `queues(id)`, `ON DELETE CASCADE`. |
| `group_id`    | TEXT        | Primary key together with `queue_id`. |
| `max_running` | INTEGER     | The group's cap. |
| `running`     | INTEGER     | Slots in use. `CHECK (running >= 0 AND running <= max_running)`. |
| `created_at`  | TIMESTAMPTZ | |
| `updated_at`  | TIMESTAMPTZ | |

## schema_migrations

Created by the bundled migration runner. One row per applied migration.

| Column    | Type    | Notes |
| --------- | ------- | ----- |
| `version` | BIGINT  | Primary key. |
| `dirty`   | BOOLEAN | `false` once applied. Each migration runs in one transaction, so a failed run leaves the table as it was. |
