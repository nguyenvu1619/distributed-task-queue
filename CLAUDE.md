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

# Typecheck src + tests + examples
npm run typecheck

# Integration tests (real Postgres via testcontainers)
npm test

# Examples
npm run example:producer
npm run example:worker
npm run example:tx

# Performance tests
npm run perf:test
npm run perf:mp        # multiprocess variant
npm run perf:all       # run all perf test variants
```

Correctness is covered by integration tests in `tests/` that run against a real
PostgreSQL 16 container. `examples/` holds performance harnesses plus the three
usage examples above.

## Architecture

This is a **distributed task queue library** backed by PostgreSQL. Consumers publish jobs; workers poll and process them; a reaper recovers expired leases.

### Layers

- **`src/domain/`** — plain interfaces and enums (`Job`, `Queue`, `Group`, `JobStatus`, `Executor`, `Logger`, custom errors)
- **`src/services/`** — business logic (`JobService`, `WorkerService`, `QueueService`, `ReaperService`)
- **`src/repository/postgresql/`** — data access; maps DB rows to domain models
- **`src/client/`** — the public facade (`TaskQueue`, `QueueHandle`) that users actually touch
- **`src/migration/`** — version-tracked SQL migration runner
- **`src/index.ts`** — public API surface

`src/index.ts` exports the `src/client/` facade and the domain types only.
Repositories and services are internals: reachable by deep import (tests and
benchmarks do this deliberately) but not covered by the package version.

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

On completion/failure the row is **deleted** from `jobs`. The `job_status` archive
below is planned but not yet created by any migration, so terminal jobs leave no
trace. The reaper periodically resets expired PROCESSING jobs back to PENDING, and
discards those that have spent their attempt budget.

### Transactional publish

`JobRepository.publishJob(input, executor?)` takes an optional `Executor` — any
`{ query(text, values) }`. With one supplied it emits no `BEGIN`/`COMMIT` of its
own and joins the caller's transaction; without one it manages its own. This is
what lets `TaskQueue.transaction()` commit a job alongside the caller's business
writes with no outbox table. The insert uses `ON CONFLICT (idempotency_key) DO
NOTHING` and reads the existing row back, because a raw `23505` would abort the
caller's entire transaction.

### Worker wake-ups

Workers do not wait out `pollInterval` to notice a job. Every statement that makes
a row pullable — the publish INSERT, a fail that puts a job back to PENDING, the
reaper's recovery UPDATE — carries `pg_notify('tq_job_' || queue_id, '')` in its
RETURNING list, so the announcement costs no extra round trip and, because NOTIFY
is transactional, fires exactly when the row becomes visible (after the caller's
COMMIT for a transactional publish, never at all on rollback).

`PgNotifier` (`src/repository/postgresql/notifier.ts`) is the other half: one
LISTEN connection per `TaskQueue`, held for as long as any worker is subscribed,
reconnecting with backoff and waking every watcher once after a reconnect since
notifications sent while it was down are gone. `TaskQueue.close()` must close it
before ending the pool — a checked-out client would otherwise make `pool.end()`
wait for ever.

Each slot captures `wakeup.next()` *before* it pulls, so a job published during
the pull resolves the promise the slot is about to await instead of falling into
the gap between the two. The poll interval is still the floor: a dropped
connection, or `notify: false` (needed behind PgBouncer in transaction mode),
costs latency and never delivery.

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
- **`job_status`** — *planned* append-only archive for COMPLETED/FAILED jobs; no migration creates it yet
- **`queues`** — queue config (concurrency, lease_duration in nanoseconds, max_attempts, requiresGroupId)
- **`queue_shards`** — per-shard running counters (created automatically when a queue has concurrency > 0)
- **`group_queue_limits`** — per-group-per-queue running counters for group concurrency enforcement
