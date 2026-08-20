# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Start PostgreSQL
docker compose up -d

# Apply/rollback migrations
npm run migrate:up
npm run migrate:down

# Build (TypeScript compilation)
npm run build

# Watch mode during development
npm run dev

# Performance tests
npm run perf:test
npm run perf:mp        # multiprocess variant
npm run perf:all       # run all perf test variants
```

No unit test suite exists — correctness is verified through performance/integration tests in `examples/`.

## Architecture

This is a **distributed task queue library** backed by PostgreSQL. Consumers publish jobs; workers poll and process them; a reaper recovers expired leases.

### Layers

- **`src/domain/`** — plain interfaces and enums (`Job`, `Queue`, `Group`, `JobStatus`, custom errors)
- **`src/services/`** — business logic (`JobService`, `WorkerService`, `QueueService`, `ReaperService`)
- **`src/repository/postgresql/`** — data access; maps DB rows to domain models
- **`src/migration/`** — version-tracked SQL migration runner
- **`src/index.ts`** — public API surface

### Dual-path job pulling

`JobRepository` has two execution paths chosen at runtime based on queue config:

- **Fast path** (`pullJobFast`): a single `UPDATE … RETURNING` — used when `concurrency = 0` and no group limits. No lock contention.
- **Coordination path** (`pullJobWithCoordination`): a transaction that also updates `queue_shards` and `group_queue_limits` counters — used when concurrency > 0 or `requiresGroupId = true`.

The same split applies to `completeJob` and `failJob`.

### Job lifecycle

```
PENDING → PROCESSING → COMPLETED (deleted from jobs, inserted into job_status)
                      → FAILED   (same)
```

On completion/failure the row is moved from the hot `jobs` table into the append-only `job_status` archive to avoid MVCC bloat. The reaper periodically resets expired PROCESSING jobs back to PENDING.

### Lease-based locking

Each pulled job gets `lease_expires_at` + `lease_seq`. Workers must supply the correct `lease_seq` when completing/failing, preventing stale writes. The reaper locks and reclaims jobs whose leases have expired.

### Queue sharding

When `concurrency > 0`, the queue is split into 32 shards (`NUMBER_OF_SHARD = 32`). Each shard gets `⌊concurrency / 32⌋` slots. Workers pick a shard with available capacity to spread load.

## Database setup

PostgreSQL 16 via Docker Compose:

```
host: localhost:5432
user: user / password: password
database: queue
```

Environment variables: `DATABASE_HOST`, `DATABASE_PORT`, `DATABASE_USER`, `DATABASE_PASS`, `DATABASE_NAME`.

## Schema overview

- **`jobs`** — active (PENDING/PROCESSING) jobs with lease columns and optional `queue_shard_no` / `group_id`
- **`job_status`** — append-only archive for COMPLETED/FAILED jobs
- **`queues`** — queue config (concurrency, lease_duration in nanoseconds, max_attempts, requiresGroupId)
- **`queue_shards`** — per-shard running counters (created automatically when a queue has concurrency > 0)
- **`group_queue_limits`** — per-group-per-queue running counters for group concurrency enforcement
