# Test suite

Integration tests against a **real PostgreSQL 16**. Nothing is mocked: every
assertion is made against the same SQL the library runs in production.

```bash
npm test                 # everything (starts a throwaway container)
npm run test:lifecycle
npm run test:concurrency
npm run test:crash
npm run test:watch
npm run typecheck        # type-checks src/ and tests/ together
```

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
| `support/invariants.ts` | counter readers, `ProcessingSampler`, concurrency trackers |
| `support/racers.ts` | `runRace()` — N concurrent pull→hold→settle loops |
| `support/crash-process.ts` | spawns a real OS process to SIGKILL |
| `fixtures/crash-worker.ts` | child that leases a job then hangs forever |
| `lifecycle.test.ts` | queue/job state machine, both execution paths |
| `concurrency.property.test.ts` | caps and mutual exclusion under contention |
| `crash-fencing.test.ts` | SIGKILL, reaper reclaim, stale-lease rejection |

## The three suites

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

## Reading a failure

These are property and characterisation tests, so a failure is a claim about the
library, not about the test. Every assertion carries a message naming the
invariant it stands for, e.g.

```
client-observed peak in-flight jobs exceeded the cap of 64: expected 82 to be less than or equal to 64
```

Suites are deliberately not tuned to pass. See `TEST_REPORT.md` in the repo root
for the current scoreboard and the defects the suite surfaces.
