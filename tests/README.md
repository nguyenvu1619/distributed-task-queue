# Test suite

Integration tests against a **real PostgreSQL 16**. Nothing is mocked: every
assertion is made against the same SQL the library runs in production.

```bash
npm test                 # everything (starts a throwaway container)
npm run test:watch
npm run typecheck        # type-checks src/, tests/ and examples/ together

# Public API (TaskQueue facade)
npm run test:client      # client construction, defineQueue, close
npm run test:producer    # publishing
npm run test:consumer    # workers and the reaper
npm run test:transaction # transactional publish

# Storage layer (repositories driven directly)
npm run test:lifecycle
npm run test:concurrency
npm run test:crash
```

Each `vitest run` invocation starts its **own** container, so running two suites
in separate terminals is safe.

## How the database is provisioned

`tests/global-setup.ts` runs once per `vitest` invocation:

1. starts `postgres:16` with [testcontainers](https://testcontainers.com) on a
   random port (`max_connections=200`, mirroring `compose.yaml`),
2. applies `migrations/` through the project's own migration runner — the tests
   exercise the real schema, not a hand-written fixture,
3. publishes the connection details via vitest's `provide`/`inject`, and mirrors
   them onto `process.env` so spawned child processes inherit them,
4. stops the container when the run ends.

To run against a Postgres you already have (`docker compose up -d`), skip the
container entirely:

```bash
TEST_PG_EXTERNAL=1 npm test         # or: npm run test:external
```

`TEST_PG_IMAGE` overrides the image tag.

### Isolation

All suites share one database, so `vitest.config.ts` sets `fileParallelism:
false` and a single fork. Each test starts from `TRUNCATE ... RESTART IDENTITY`
and builds a **fresh harness** — `QueueRepository` memoises queues in-process, so
a reused instance would keep serving rows that `TRUNCATE` already removed.

## Layout

| Path | What it holds |
| --- | --- |
| `global-setup.ts` | container lifecycle + migrations |
| `support/harness.ts` | pool/repo/service wiring, reset, fixture builders, `waitFor` |
| `support/task-queue.ts` | `TaskQueue` facade harness, scratch business tables, `collect()` |
| `support/invariants.ts` | counter readers, `ProcessingSampler`, concurrency trackers |
| `support/racers.ts` | `runRace()` — N concurrent pull→hold→settle loops |
| `support/crash-process.ts` | spawns a real OS process to SIGKILL |
| `fixtures/crash-worker.ts` | child that leases a job then hangs forever |
| `client.test.ts` | facade lifecycle: create, `defineQueue`, `migrate`, `close` |
| `producer.test.ts` | publishing: payloads, idempotency, groups, batches |
| `consumer.test.ts` | `work()`: dispatch, retries, poison, shutdown, reaper |
| `transaction.test.ts` | publishing inside the caller's transaction |
| `lifecycle.test.ts` | queue/job state machine, both execution paths |
| `concurrency.property.test.ts` | caps and mutual exclusion under contention |
| `crash-fencing.test.ts` | SIGKILL, reaper reclaim, stale-lease rejection |

Two layers, deliberately: the first four files drive the **public API** a user
touches, and would keep passing through any internal refactor. The last three
drive the **repositories directly**, below the facade, because they assert on SQL
behaviour and contention windows that the worker's poll-sleep would blur.

## The facade suites

### Producer

Everything `publish` / `publishMany` guarantees: exactly one row per publish,
payload round-trip fidelity through the serializer, `idempotencyKey`
deduplication (including the sharp edge that the key is **globally** unique and
is freed when a terminal job is deleted), group-limit registration, batch
ordering, and publishing through a caller-supplied client from a foreign pool.

### Consumer

`work()` end to end: dispatch and `ctx`, FIFO order, slot concurrency versus the
queue's own cap, the retry ladder up to `maxAttempts`, poison payloads discarded
on first sight rather than retried, a completion failure **not** being converted
into a failure, interruptible shutdown with `stop({ timeout })`, and the reaper
reclaiming expired leases without resetting `lease_seq`.

### Transaction

The headline feature. Every test commits or rolls back a **real business write**
alongside the job — via `createScratchTable` — because a test that only touches
the `jobs` table does not prove the claim. Covers commit, rollback, isolation
from other sessions, savepoints, structural `Executor` adapters of the kind an
ORM user would write, and the guarantee that a duplicate `idempotencyKey` does
not abort the caller's transaction.

## The storage-layer suites

### Lifecycle

Queue creation (shard provisioning, nanosecond lease encoding), publish → pull →
complete/fail, idempotency-key conflicts, stale-`lease_seq` rejection, FIFO
order, group-limit registration, and the `snake_case → camelCase` mapping.

### Concurrency properties

`runRace()` drives N independent pull loops straight at `JobRepository` — not at
`WorkerService`, whose poll-sleep would blur the contention window. Two
independent witnesses check every cap:

- **`ConcurrencyTracker`** — client-side high-water mark of jobs held at once.
  Cannot miss a window, but only sees this process.
- **`ProcessingSampler`** — polls `count(*) WHERE status='PROCESSING'` on its own
  connection pool. Ground truth, independent of client bookkeeping.

A cap is only considered upheld when *both* stay at or below the configured
limit. Races always run with more workers than slots, and with leases long
enough that a re-delivery can only mean a genuine double-delivery.

### Crash / fencing

`spawnCrashWorker()` starts a real `node` process that leases one job, announces
it on stdout, and then hangs. The parent `SIGKILL`s it — leaving exactly the
state a crashed worker leaves behind: a `PROCESSING` row with a live lease and
nobody left to settle it. From there the tests assert the lease is honoured
until it expires, the reaper returns the job to `PENDING`, a healthy worker
picks it up, and the **zombie's settle carrying the dead lease is rejected**.

A parallel `describe` block re-checks the same fencing rules deterministically by
forcing `lease_expires_at` into the past, so a fencing regression does not depend
on process-spawn timing.

## Mutation checks

The suites are verified by breaking the library on purpose. Nine one-line mutations, each
deleting a guarantee the facade suites exist to protect, were applied to `src/` in turn; every
one of them turned the relevant suite red:

| Mutation | Guarantee | Caught by |
| --- | --- | --- |
| `withExecutor` ignores the caller's executor | publish joins the caller's transaction | `transaction` (29 tests) |
| group-limit insert goes to `this.pool` | that insert rides the caller's transaction | `transaction` (3) |
| `deduplicated` hardcoded false | a repeated key is reported as deduplicated | `producer` (5) |
| `Number(row.id)` reverted | BIGINT coerced at the boundary | `producer` (1) |
| a failed complete also calls `failJob` | success is not recorded as failure | `consumer` (1) |
| poison routed to `fail` not `discard` | undecodable payloads are discarded, not retried | `consumer` (5) |
| poll sleep drops its abort listener | the sleep wakes immediately on stop | `consumer` (1) |
| `stop()` stops awaiting its slots | stop waits for handlers up to its deadline | `consumer` (8) |
| the empty-queue poll sleep removed | an idle worker throttles its polling | `consumer` (1) |

A test that stays green under its own mutation is not a test. Re-run the sweep after changing
either the worker loop or the publish path.

## Reading a failure

These are property and characterisation tests, so a failure is a claim about the
library, not about the test. Every assertion carries a message naming the
invariant it stands for, e.g.

```
client-observed peak in-flight jobs exceeded the cap of 64: expected 82 to be less than or equal to 64
```

Suites are deliberately not tuned to pass, and several deliberately pin behaviour that is
**wrong** — a grouped publish to a fast-path queue whose cap is never enforced, a NUL in
`metadata` that aborts the caller's transaction, a first-use publish that starves itself of
connections. Each of those carries a comment saying so and is tracked in `V1_CHECKLIST.md` §1b;
fixing one is expected to flag the test that describes it.

See `TEST_REPORT.md` in the repo root for the current scoreboard.
