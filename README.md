# distributed-task-queue

A task queue for Node.js backed by the PostgreSQL database you already run.

Jobs are rows in your database, which means **you can enqueue inside your own
transaction** — the job and the business write commit together, or neither does.
No outbox table, no relay process, no dual-write to reconcile, no second piece of
infrastructure to operate.

```ts
import { TaskQueue } from 'distributed-task-queue';

const tq = TaskQueue.create({ connectionString: process.env.DATABASE_URL });
const emails = tq.defineQueue<Email>('emails', { concurrency: 10 });

await emails.publish({ to: 'ada@example.com', subject: 'Hi' });
await emails.work(async (email) => sendEmail(email), { concurrency: 4 });
```

## Contents

- [Is this for you?](#is-this-for-you)
- [Performance](#performance)
- [Requirements](#requirements)
- [Install](#install)
- [Quick start](#quick-start)
- [Connecting](#connecting)
- [Producing](#producing)
- [Transactional publish](#transactional-publish)
- [Consuming](#consuming)
- [The reaper](#the-reaper)
- [Concurrency and groups](#concurrency-and-groups)
- [Shutdown](#shutdown)
- [Serializers](#serializers)
- [Logging](#logging)
- [Errors](#errors)
- [Guarantees and semantics](#guarantees-and-semantics)
- [What it puts in your database](#what-it-puts-in-your-database)
- [API reference](#api-reference)
- [Known limitations](#known-limitations)
- [Development](#development)

## Is this for you?

**Good fit.** You already run PostgreSQL, and at least some of your jobs must be
enqueued atomically with a database write — "send the confirmation email if and
only if the order committed". You would rather have one datastore to back up,
monitor, and restore than two. Throughput is not the constraint for most such
systems: see [Performance](#performance) for measured numbers.

**Poor fit.** You need sub-millisecond dispatch latency, fan-out pub/sub, or the
scheduling features listed under [Known limitations](#known-limitations) —
delayed jobs, cron, retry backoff, priorities. A broker built for the job
(Redis-backed queues, SQS, NATS, Kafka) will serve you better.

Workers are woken by `LISTEN/NOTIFY` the moment a job is published, and poll on
`pollInterval` as the fallback — so pickup is prompt without a short poll, and
still correct if the notification never arrives.

## Performance

Measured through the public API with a no-op handler, so these are the queue's
own overhead — the ceiling you are working under, not what your service will do.
One Node process, 16 polling slots, batched publishes, on a laptop with
PostgreSQL 16 in Docker. Treat them as a floor: a tuned server with local
Postgres does considerably better.

| Queue configuration | Pull path | Consume | Publish (batched) |
| --- | --- | --- | --- |
| `concurrency: 0`, no groups | fast | **~6,000 jobs/s** | ~40,000 jobs/s |
| `concurrency: 512` | coordination | **~1,800 jobs/s** | ~45,000 jobs/s |
| `concurrency: 512` + groups | coordination | **~700 jobs/s** | ~25,000 jobs/s |

The headline is the **cost of the concurrency cap**, and it is the one number
worth knowing before you design around this library:

- The **fast path** is a single `UPDATE … RETURNING` per job. Nothing locks,
  nothing coordinates.
- The **coordination path** — taken whenever `concurrency > 0` or
  `requiresGroupId` is set — is still a single statement and a single round
  trip, but one chaining five data-modifying CTEs per job: claim a shard,
  `SELECT … FOR UPDATE SKIP LOCKED` the next job, bump the group counter, bump
  the shard counter, write the lease. The counter locking is what costs:
  ~3x slower, and ~9x with groups on top. (Numbers above were measured on the
  earlier multi-statement build of this path; the fold to one statement removes
  six round trips per pull, so treat them as a floor.)

So do not set `concurrency` unless you actually need the cap. It is a
rate-limiting feature that costs throughput, not a tuning knob that adds it.

In practice the queue is rarely the bottleneck: a handler doing 50ms of real work
across 16 slots tops out near 320 jobs/s on handler time alone, well under even
the group-limited path. Size against your handler first.

Reproduce with the harnesses in [examples/](examples/) — see
[examples/PERFORMANCE_TESTING.md](examples/PERFORMANCE_TESTING.md). Note that the
"11,851 jobs/sec" figure quoted in those docs is **Graphile Worker's published
reference number on a 12-core Ryzen**, kept there for comparison; it is not a
measurement of this library.

## Requirements

- **Node.js 18+** — the worker uses `AbortSignal` and `AbortController`
- **PostgreSQL 12+** — partial indexes and `ON CONFLICT` are the newest features used
- TypeScript is optional. The package ships CommonJS (`require`) plus `.d.ts`
  declarations, and works from plain JavaScript.

## Install

> **Not on npm yet.** The package is unpublished, and `dist/` is not committed, so
> `npm install <git-url>` yields a package whose entry point does not exist.
> Until it is published, install from source:

```bash
git clone https://github.com/nguyenvu1619/distributed-task-queue.git
cd distributed-task-queue
npm install
npm run build          # produces dist/

# then, from your own project:
npm install /path/to/distributed-task-queue
```

Once published, this becomes `npm install distributed-task-queue`.

## Quick start

### 1. Get a Postgres

Any Postgres will do. For local development this repo ships one:

```bash
docker compose up -d          # postgres:16 on localhost:5432, database "queue"
```

### 2. Apply the schema

Once per database. `migrate()` is safe to call on every boot — it applies only
what is missing and records what it applied in `schema_migrations`.

```ts
const tq = TaskQueue.create({ connectionString: process.env.DATABASE_URL });
await tq.migrate();
```

If you would rather run migrations from your own tooling, the SQL files live in
[migrations/](migrations/) and are plain `.up.sql` / `.down.sql` pairs.

### 3. Publish

```ts
import { TaskQueue } from 'distributed-task-queue';

interface Email {
  to: string;
  subject: string;
}

const tq = TaskQueue.create({ connectionString: process.env.DATABASE_URL });
const emails = tq.defineQueue<Email>('emails', {
  concurrency: 10,
  maxAttempts: 3,
  leaseDuration: '30s',
});

await emails.publish({ to: 'ada@example.com', subject: 'Hello' });
```

### 4. Consume

```ts
await emails.work(
  async (email, ctx) => {
    ctx.log.info(`sending to ${email.to} (attempt ${ctx.attempt}/${ctx.maxAttempts})`);
    await sendEmail(email);
  },
  { concurrency: 4, pollInterval: '250ms' }
);

// Recovers jobs stranded by a worker that died. Run at least one per deployment.
tq.startReaper({ interval: '30s' });
```

Runnable versions of all three are in [examples/](examples/):

```bash
npm run example:producer   # publish
npm run example:worker     # consume; Ctrl-C drains and exits
npm run example:tx         # transactional publish, committed and rolled back
```

## Connecting

`TaskQueue.create()` opens no connection. The first publish or worker resolves its
queue and, with it, the pool.

```ts
// A connection string...
TaskQueue.create({ connectionString: process.env.DATABASE_URL });

// ...or discrete fields...
TaskQueue.create({
  host: 'localhost',
  port: 5432,
  user: 'user',
  password: 'password',
  database: 'queue',
  ssl: { rejectUnauthorized: false },
  applicationName: 'my-service-worker',   // shows up in pg_stat_activity
  max: 20,                                // pool size; default 20
});

// ...or a pool you already own.
TaskQueue.create({ pool: myExistingPool });
```

A **borrowed pool is never ended** by `close()` — whoever created it owns its
lifetime. A pool the library created is ended by `close()`.

There is no automatic reading of `DATABASE_*` environment variables; wire them up
yourself if you want them (the examples show one way).

### Pool sizing

Each worker slot holds a connection only for the duration of a pull or a settle,
not for the length of your handler — so `concurrency: 50` does not need 50
connections. A process that runs workers does hold **one** connection for the
whole of its lifetime — the `LISTEN` session that wakes them — so budget `max`
for it, or set `notify: false` to give it up. But note that the **first** publish
to a given queue inside
`tq.transaction()` checks out a *second* connection to create the queue row while
your transaction holds the first. On a very small pool (`max: 1`, some serverless
and PgBouncer setups) that deadlocks. Pre-resolve the queue once at boot to avoid
it:

```ts
await emails.resolve();   // or: await emails.id()
```

## Producing

```ts
const emails = tq.defineQueue<Email>('emails', {
  concurrency: 10,        // max running at once across every worker. 0 = unlimited
  maxAttempts: 3,         // total tries per job, including the first
  leaseDuration: '30s',   // how long a pulled job is held before it can be reclaimed
  requiresGroupId: false, // reject publishes that carry no group
});
```

`defineQueue` creates the queue on first use if it does not exist, and returns the
same handle on repeated calls. **Define each queue once and share the handle** —
calling it again with a *different* config throws `BadParamInputError`, and a
queue that already exists in the database with different settings keeps its stored
settings and logs a warning.

```ts
const job = await emails.publish({ to: 'ada@example.com', subject: 'Hi' });
job.id;             // number
job.deduplicated;   // see below

// One round trip for the whole batch. All-or-nothing within the batch.
await emails.publishMany([
  { to: 'a@x.com', subject: 'Hi' },
  { to: 'b@x.com', subject: 'Hi' },
]);

// Live backlog. Terminal jobs are deleted, so this is what is left to do.
const { pending, processing } = await emails.stats();
```

There is also a shorthand that defines the queue with defaults and publishes in
one call — handy for scripts, less so for a service:

```ts
await tq.publish('emails', { to: 'ada@example.com', subject: 'Hi' });
```

### Idempotency

Give a publish an `idempotencyKey` and a repeat returns the first job instead of
enqueueing a second:

```ts
const job = await emails.publish(payload, { idempotencyKey: `welcome-${userId}` });
job.deduplicated;   // true if this key was already in flight
```

Two caveats worth knowing before you rely on it:

- **The key is held only while the job is alive.** Terminal jobs are deleted,
  which frees the key. This protects against a double *publish*, not against
  re-running work that has already completed.
- **Keys are global, not per queue.** The `idempotency_key` column is `UNIQUE`
  across the whole table, so the same key on two different queues collides.
  Namespace your keys (`emails:welcome-42`).

Without an explicit key, each publish gets a random UUID — i.e. no deduplication.

### Metadata

Arbitrary JSON stored alongside the job and handed back to the handler as
`ctx.job.metadata`:

```ts
await emails.publish(payload, { metadata: { requestId, tenant: 'acme' } });
```

It lands in a `jsonb` column, so it must be JSON-serializable and must not contain
NUL characters (\u0000) — `jsonb` rejects those. The `payload` itself is stored
as `TEXT` and has no such restriction.

## Transactional publish

This is the whole point of putting the queue in your database.

```ts
await tq.transaction(async (tx) => {
  await tx.query('INSERT INTO orders (id, total) VALUES ($1, $2)', [id, total]);
  await emails.publish({ orderId: id }, { tx, idempotencyKey: `confirm-${id}` });
});
```

Both statements commit together. If the transaction rolls back, the job was never
enqueued — there is no window in which a confirmation email exists for an order
that does not, and none in which an order exists with no confirmation queued.

Already running a transaction of your own? Pass the client:

```ts
const client = await pool.connect();
try {
  await client.query('BEGIN');
  await client.query('INSERT INTO orders ...');
  await emails.publish(payload, { tx: client });
  await client.query('COMMIT');
} finally {
  client.release();
}
```

`tx` is **structural** — anything with a `query(text, values)` method resolving to
`{ rows, rowCount }`. A pg `PoolClient` satisfies it as-is; an ORM needs a few
lines of adapter, and only `rows` has to be right (`rowCount` is part of the type
but nothing in the library reads it).

A duplicate `idempotencyKey` will **not** abort your transaction. The insert uses
`ON CONFLICT DO NOTHING` and reads the existing job back, so a raw `23505` never
reaches your connection and the rest of your transaction proceeds normally.

### Using an ORM

The one rule that matters: **your adapter must run on the connection the ORM's
`BEGIN` is holding.** An adapter that quietly opens its own connection still
looks correct on commit — the job is there — and only fails on rollback, by
leaving an orphaned job for a business row that no longer exists. That is exactly
the dual-write bug this feature exists to prevent, so it is worth testing the
rollback path in your own suite.

Adapters below are verified against PostgreSQL 16 — commit and rollback, plus a
`BIGINT` job id arriving as a `number` when the ORM brings its own copy of `pg`.

**Sequelize 6.** Use `bind` (the library emits `$1`-style placeholders, so
`replacements` is wrong) and pass `transaction: t`:

```ts
import { QueryTypes } from 'sequelize';
import type { Executor } from 'distributed-task-queue';

const asExecutor = (t: Transaction): Executor => ({
  async query(text, values) {
    const rows = await sequelize.query(text, {
      bind: values ?? [],
      transaction: t,            // <- without this it runs on another connection
      type: QueryTypes.SELECT,   // <- returns the rows array
    });
    return { rows: rows as any[], rowCount: (rows as any[]).length };
  },
});

const t = await sequelize.transaction();
try {
  await sequelize.query('INSERT INTO orders (id) VALUES ($1)', {
    bind: [id], transaction: t, type: QueryTypes.INSERT,
  });
  await emails.publish({ orderId: id }, { tx: asExecutor(t), idempotencyKey: `confirm-${id}` });
  await t.commit();
} catch (error) {
  await t.rollback();
  throw error;
}
```

**TypeORM 0.3.** The `EntityManager` handed to `dataSource.transaction()` is
already bound to the transaction's query runner, so passing it through is enough:

```ts
import type { Executor } from 'distributed-task-queue';

const asExecutor = (manager: EntityManager): Executor => ({
  async query(text, values) {
    const rows = await manager.query(text, values ?? []);
    return { rows: rows ?? [], rowCount: rows?.length ?? 0 };
  },
});

await dataSource.transaction(async (manager) => {
  await manager.query('INSERT INTO orders (id) VALUES ($1)', [id]);
  await emails.publish({ orderId: id }, { tx: asExecutor(manager), idempotencyKey: `confirm-${id}` });
});
```

TypeORM's postgres driver also exposes its underlying pg `Pool`, so the library
can share it instead of opening a second one:

```ts
const tq = TaskQueue.create({ pool: (dataSource.driver as any).master });
```

**Knex** follows the same shape (untested here — verify the rollback path
yourself):

```ts
const asExecutor = (trx: Knex.Transaction): Executor => ({
  async query(text, values) {
    const result = await trx.raw(text, values ?? []);
    return { rows: result.rows, rowCount: result.rowCount ?? null };
  },
});
```

**Prisma** is the awkward one: `$executeRawUnsafe` returns a row count rather than
rows, and the library needs the `RETURNING` rows back. `$queryRawUnsafe` inside an
interactive `$transaction` is the shape to try. Not verified here.

#### Resolve the queue before the transaction

`publish()` resolves the queue on the **library's own pool**, not on your `tx`, so
the first publish to a not-yet-created queue checks out a second connection while
your ORM transaction holds the first. On a normal pool that is invisible. On a
pool of one — `max: 1`, some serverless and PgBouncer setups, or a shared pool
sized down — **it deadlocks**, verified. Resolve once at boot and it never
happens:

```ts
await emails.resolve();   // at startup, outside any transaction
```

`tq.transaction()` does not nest — calling it inside itself silently opens an
independent transaction on a second connection.

## Consuming

```ts
const worker = await emails.work(
  async (email, ctx) => {
    if (ctx.signal.aborted) return;      // the worker is shutting down
    await sendEmail(email);
  },
  {
    concurrency: 4,            // polling slots in this process. Default 1
    pollInterval: '250ms',     // wait between polls when the queue is empty. Default 1s
    name: 'email-worker',      // label in log lines
    onError: (event) => Sentry.captureException(event.error, { extra: event }),
    autoStart: true,           // set false to construct without starting
  }
);
```

The handler contract:

- **Returning** marks the job **complete**. The row is deleted.
- **Throwing** marks it **failed**. It goes back to `PENDING` and is retried until
  `maxAttempts` is spent, then discarded.
- A payload the serializer cannot decode is treated as **poison** and discarded
  immediately, rather than burning the whole attempt budget on the same failure
  one run at a time. You get `onError({ phase: 'deserialize' })`.

### The handler context

The second argument carries everything about the run:

```ts
async (payload, ctx) => {
  ctx.id;           // job id
  ctx.attempt;      // 1-based, already counting this run
  ctx.maxAttempts;
  ctx.queue;        // queue name
  ctx.groupId;      // string | null
  ctx.signal;       // AbortSignal, aborted when the worker is stopping
  ctx.log;          // logger, prefixed with [worker:<name>]
  ctx.job;          // the raw Job row — escape hatch for anything not surfaced here
}
```

Pass `ctx.signal` into anything that accepts one (`fetch`, your HTTP client) so a
shutdown cancels in-flight I/O instead of waiting it out.

### Observing failures

`onError` fires for every error the worker absorbs, tagged with where it happened:

```ts
type WorkerErrorPhase = 'pull' | 'deserialize' | 'handler' | 'complete' | 'fail' | 'discard';

interface WorkerErrorEvent {
  phase: WorkerErrorPhase;
  error: unknown;
  slot: number;      // which polling slot
  job?: Job;
}
```

A `complete` error is the interesting one: your handler *succeeded* but the library
could not record it. The job is deliberately left to have its lease expire and be
re-delivered, rather than being force-failed — which is why handlers must be
idempotent.

### Controlling a worker directly

```ts
const worker = await emails.work(handler, { autoStart: false });
await worker.start();
worker.isRunning();
const { drained } = await worker.stop({ timeout: '10s' });
```

Stopping a worker never ends the pool — that is `tq.close()`.

## The reaper

Workers die: a process is killed, a container is evicted, a node loses power. The
jobs those workers had leased sit in `PROCESSING` and nothing else can pick them
up. The reaper is what notices.

```ts
const reaper = tq.startReaper({
  interval: '30s',   // time between passes. Default 30s
  batchSize: 100,    // jobs reclaimed per pass. Default 100
});
```

Each pass finds jobs whose lease has expired, returns the shard and group
concurrency slots they were holding, then either resets them to `PENDING` or — if
their attempt budget is spent — discards them.

**Run at least one reaper per deployment.** Without it, a single crash strands
those jobs permanently. Running several is fine; they lock disjoint batches
(`FOR UPDATE SKIP LOCKED`). The reaper is global, not per queue: one covers every
queue in the database.

```ts
await reaper.runOnce();   // one pass right now; returns the reclaimed job ids
reaper.isRunning();
await reaper.stop();
```

## Concurrency and groups

`concurrency` on a queue caps how many of its jobs run at once **across every
worker and every process**. It is enforced in the database, not in memory, by
splitting the queue into 32 shards and giving each shard `⌊concurrency / 32⌋`
slots. Workers claim a shard that still has capacity.

```ts
const reports = tq.defineQueue('reports', { concurrency: 20 });
```

Because the cap is divided across 32 shards, small values are lumpy — a
`concurrency` below 32 leaves some shards with zero slots. Prefer values
comfortably above 32, or a multiple of it, when the exact ceiling matters.

Setting `concurrency` above 0 switches the queue onto the coordination pull path
and **costs roughly 3x throughput** (9x once groups are involved) — see
[Performance](#performance). Leave it at 0 unless you need the cap.

**Groups** cap concurrency per tenant, customer, or any other key:

```ts
const jobs = tq.defineQueue('tenant-work', { requiresGroupId: true });

await jobs.publish(payload, { group: { id: `tenant-${id}`, concurrency: 2 } });
```

> **The queue must be group-aware for group caps to be enforced.** Set
> `requiresGroupId: true`, or a queue `concurrency` above 0. On a queue with
> neither, jobs take a fast path that never consults the group counters: the limit
> rows are written and look live in the database, but the group runs with no cap
> at all. Pick one of the two settings.

`group.concurrency` is required, not optional, and must be a positive integer. A
group published with `0`, a negative number, or a non-integer produces a job that
can never be delivered.

Further sharp edges in the current implementation:

- **The first publisher's cap wins, permanently.** The limit row is inserted with
  `ON CONFLICT DO NOTHING`, so publishing the same group id later with a different
  `concurrency` silently keeps the original.
- **No group fairness.** The coordination pull looks at only the single oldest
  pending job. If that job's group is at its cap, the pull returns nothing — jobs
  from other groups queued behind it wait. Correctness holds (no cap is ever
  exceeded), throughput is not shared evenly, and a group that can never take a
  slot blocks the queue indefinitely.
- **An empty-string group id fails open** — it is stored as `NULL` and the job runs
  uncapped. Guard against an unset environment variable producing `''`.

## Shutdown

```ts
const shutdown = async () => {
  await tq.close({ timeout: '20s' });   // stop workers + reapers, drain, close pool
  process.exit(0);
};

process.on('SIGTERM', () => void shutdown());
process.on('SIGINT', () => void shutdown());
```

`close()` stops every worker and reaper this client started, waits for in-flight
handlers, then ends the pool if this client created it. Without a `timeout` it
waits indefinitely; with one it gives up at the deadline and logs how many workers
still had work running. It is idempotent — a second call is a no-op.

Give the timeout enough room for your slowest handler, and keep it below whatever
your orchestrator allows between `SIGTERM` and `SIGKILL`.

Anything not drained at the deadline is not lost: those jobs still hold leases, and
the reaper returns them to `PENDING` once the leases expire.

## Serializers

Payloads are stored as `TEXT`. By default they go through `JSON.stringify` /
`JSON.parse`. Swap that per queue for a schema codec:

```ts
import type { Serializer } from 'distributed-task-queue';

const emailCodec: Serializer<Email> = {
  serialize: (value) => JSON.stringify(EmailSchema.parse(value)),
  deserialize: (raw) => EmailSchema.parse(JSON.parse(raw)),
};

tq.defineQueue<Email>('emails', { serializer: emailCodec });
```

A `deserialize` that throws marks the job poison and discards it — which is what
makes a validating codec a quarantine mechanism for payloads written by an older
version of your code. Wire up `onError` if you use one that way, because a
discarded job leaves no trace.

### Why `payload` is `TEXT` and not `jsonb`

`metadata` is `jsonb`; `payload` is deliberately not. The library never looks
inside a payload — it does `SELECT payload` and hands the value to your
serializer — so `jsonb`'s one advantage, indexing and querying into the document,
is unused, while its costs are real. Measured on PostgreSQL 16:

| Payload | `jsonb` insert | `jsonb` read | `jsonb` on disk |
| --- | --- | --- | --- |
| ~120 B | −11% | ~equal | **+48%** |
| ~2 KB | **−54%** | ~equal | **+73%** |
| ~20 KB | −35% | −13% | −4% (both TOAST-compressed) |

`jsonb` has to parse every payload on write and re-serialize it on read, and it
stores small documents substantially larger. It would also **reject payloads that
`TEXT` accepts**: `JSON.stringify` encodes a NUL character as the escape
`\u0000`, which `jsonb` refuses with `22P05`, and inside `{ tx }` that error
aborts the caller's whole transaction. On top of that it would force every
payload to be JSON, which defeats the `Serializer` abstraction, and `jsonb`
reorders and de-duplicates object keys, so a payload would not round-trip
byte-for-byte — enough to break a signature or hash computed over it.

`BYTEA` measures within noise of `TEXT` for JSON payloads and stores identical
bytes, so it buys nothing on its own. It would only pay off alongside a change to
`Serializer` — which returns `string` today, so a binary codec (protobuf,
msgpack, CBOR) has to base64-encode and give back roughly a third of the size it
saved. That is a worthwhile change, but it is an API and schema break, not a
column-type swap.

Payload format is not the throughput limit in any case: see
[Performance](#performance) — pulling a job costs far more than storing one.

## Logging

Every component takes a `logger`. The library logs on ordinary outcomes — "queue
saturated", "group at its cap" — which flood stdout on a busy queue, so the sink
has to be your choice.

```ts
import { silentLogger } from 'distributed-task-queue';

TaskQueue.create({ connectionString, logger: silentLogger });
```

The default writes to `console`. Pass `silentLogger`, or your own
`{ debug, info, warn, error }` — a pino or winston instance satisfies the shape.
Workers and the reaper prefix their lines (`[worker:emails]`, `[reaper]`) and each
accepts its own `logger` override.

## Errors

Every error this library throws is a `TaskQueueError` carrying a stable `code` and a
`retryable` flag. **Switch on `code`, not on the class** — `instanceof` silently returns
false when two copies of this package end up in one dependency tree, and the string never
does. `isTaskQueueError` is brand-based for the same reason.

| Code | Meaning | What to do |
|---|---|---|
| `LEASE_LOST` | You do not hold this job's lease | Abandon the job. Do **not** settle it again. |
| `JOB_NOT_FOUND` | No live job with that id | Nothing to act on — finished jobs are deleted. |
| `QUEUE_NOT_FOUND` | No queue with that id | Fix the configuration. |
| `PUBLISH_CONFLICT` | A publish could neither insert nor read back its key | Under REPEATABLE READ or stricter, retry the whole transaction. |
| `INVALID_INPUT` | An argument rejected before touching the database | Fix the call — a bad duration, an unserializable payload, a missing group id. |

```ts
import { ErrorCodes, isTaskQueueError } from 'distributed-task-queue';

try {
  await queue.publish(payload);
} catch (error) {
  if (isTaskQueueError(error) && error.code === ErrorCodes.INVALID_INPUT) {
    // error.context carries structured detail; prefer it over parsing the message.
  }
  throw error;
}
```

`retryable` says whether retrying the **identical call** could plausibly succeed. It does
not mean the failure was harmless. Every code above is `false`, because each describes a
state an identical retry cannot change — `LEASE_LOST` most of all: retrying a settle you
were just fenced out of would be a correctness violation, not a recovery.

### Why a lost lease is not a "not found"

The settle predicate is `id = $1 AND lease_seq = $2 AND status = 'PROCESSING'`. A zero-row
result collapses several causes the database cannot separate: the lease expired and the
reaper reclaimed the job, another worker re-leased it, the job was already settled
(terminal jobs are deleted, leaving no tombstone), or the caller never held a lease at
all. They share the only fact a caller can act on — **this job is not yours** — so they
share one code. `JOB_NOT_FOUND` is reserved for a plain `getById` miss, where no lease was
presented.

Not every failure is translated: driver errors from `pg` (constraint violations,
connection loss) surface as-is, and a circular or `BigInt` payload raises a raw
`TypeError` from `JSON.stringify`.

## Guarantees and semantics

- **At-least-once delivery.** A handler that succeeds but whose completion is not
  recorded — process killed, connection lost — will see the job again after the
  lease expires. **Make handlers idempotent.** This is not a tuning knob; it is the
  contract.
- **Leases, not locks.** A pulled job is held for `leaseDuration`. There is no
  lease renewal yet, so **a handler that runs longer than its lease may be
  reclaimed and re-delivered while still running** — the job then executes twice
  concurrently, and the concurrency cap is exceeded. Set `leaseDuration`
  comfortably above your slowest handler.
- **Fencing.** Every lease carries a monotonic `lease_seq`. A worker returning from
  the dead cannot complete or fail a job that has since been reassigned; its write
  is rejected.
- **Attempts are counted at lease time**, so a crash costs an attempt exactly like
  an explicit failure. A job that crashes its worker three times with
  `maxAttempts: 3` is discarded, not retried forever.
- **No retry backoff.** A job that fails immediately is retried immediately, at
  full speed.
- **Terminal jobs are deleted.** Completed and failed jobs leave no record — no
  archive, no failure reason. Log what you need from inside the handler.
- **FIFO per queue, best effort.** Ordering is by `created_at`. Jobs published in
  the same transaction share a timestamp and have no deterministic order between
  them, and a busy queue with several workers interleaves freely.
- **Queue configuration is immutable.** There is no update path: a queue that
  already exists keeps its stored settings and warns on drift. Changing
  `concurrency` or `leaseDuration` means a new queue name or a manual `UPDATE`.
- **`publishMany` has a ceiling near 9,000 payloads** per call — seven bind
  parameters per row against the protocol's 65,535 limit. Past it you get an opaque
  driver error. Chunk large batches yourself.

## What it puts in your database

`migrate()` creates five tables. They are ordinary tables in your schema; nothing
is namespaced.

| Table | What it holds |
| --- | --- |
| `jobs` | Active (`PENDING` / `PROCESSING`) jobs, with lease columns and optional shard / group ids. Terminal jobs are deleted from it. |
| `queues` | One row per queue: concurrency, lease duration, max attempts, `requires_group_id`. |
| `queue_shards` | Per-shard running counters. Created automatically when a queue has `concurrency > 0`. |
| `group_queue_limits` | Per-group-per-queue running counters. |
| `schema_migrations` | Applied migration versions, for the runner. |

Because `jobs` is a normal table, you can inspect the backlog with SQL:

```sql
SELECT q.name, j.status, count(*)
FROM jobs j JOIN queues q ON q.id = j.queue_id
GROUP BY 1, 2;
```

## API reference

### `TaskQueue.create(options)`

`connectionString` · `host` · `port` · `user` · `password` · `database` · `ssl` ·
`applicationName` · `max` · `idleTimeoutMillis` · `connectionTimeoutMillis` ·
`pool` · `logger` · `migrationsPath` · `notify` (`true`)

| Method | Returns | |
| --- | --- | --- |
| `defineQueue<T>(name, config?)` | `QueueHandle<T>` | Declare a queue; idempotent |
| `publish(name, payload, options?)` | `Promise<PublishedJob>` | Shorthand for `defineQueue(name).publish(...)` |
| `transaction(fn)` | `Promise<R>` | Run `fn` in a transaction; hands it a `PoolClient` |
| `startReaper(options?)` | `Reaper` | Start the recovery loop |
| `migrate()` | `Promise<void>` | Apply pending migrations |
| `close(options?)` | `Promise<void>` | Stop everything, drain, close the pool |
| `pool` | `Pool` | The underlying pg pool |

### `defineQueue(name, config)`

`concurrency` (`0`) · `maxAttempts` (`3`) · `leaseDuration` (`'30s'`) ·
`requiresGroupId` (`false`) · `serializer` (JSON)

| Method | Returns | |
| --- | --- | --- |
| `publish(payload, options?)` | `Promise<PublishedJob>` | |
| `publishMany(payloads, options?)` | `Promise<PublishedJob[]>` | One round trip, all-or-nothing |
| `work(handler, options?)` | `Promise<Worker>` | Start consuming |
| `stats()` | `Promise<{ pending, processing }>` | Live backlog |
| `resolve()` / `id()` | `Promise<Queue>` / `Promise<number>` | Force queue creation early |
| `config` | `ResolvedQueueConfig` | Defaults filled in, durations in ms |

### `publish(payload, options)`

`tx` · `idempotencyKey` · `group` (`{ id, concurrency }`) · `metadata`

### `work(handler, options)`

`concurrency` (`1`) · `pollInterval` (`'1s'`) · `name` · `logger` · `onError` ·
`autoStart` (`true`)

Returns a `Worker`: `start()` · `stop({ timeout })` → `{ drained }` ·
`isRunning()` · `queue`

### `startReaper(options)`

`interval` (`'30s'`) · `batchSize` (`100`) · `logger`

Returns a `Reaper`: `runOnce()` → job ids · `stop()` · `isRunning()`

### Durations

Anywhere a duration is accepted, pass milliseconds as a number or a short string:
`'500ms'`, `'30s'`, `'5m'`, `'2h'`, `'1d'`.

## Known limitations

Things that do not exist yet. Several have workarounds; none are silently broken.

**Scheduling.** No delayed jobs, no `runAt`, no cron, no retry backoff, no
priorities. Every job is eligible the moment it is published, and a failing job
retries at full speed.

**Durability of outcomes.** Completed, failed, and discarded jobs are deleted with
no archive and no recorded failure reason. If you need an audit trail, write it
from inside your handler.

**Lease renewal.** A handler outliving its lease is re-delivered while still
running. There is no heartbeat or `extend()` yet — size `leaseDuration` for your
worst case.

**Counter drift.** Nothing recomputes `queue_shards.running` or
`group_queue_limits.running` from `jobs`. The reaper releases the slots of jobs it
reclaims, but a counter that drifts for any other reason stays drifted; the symptom
is a queue that throttles below its configured concurrency. `UPDATE` the counter
back to the real count if you hit it.

**Group fairness and rate limiting.** See
[Concurrency and groups](#concurrency-and-groups).

**Wakeups behind a transaction-mode pooler.** `LISTEN` is session state, so it
cannot work through PgBouncer in transaction mode. Set `notify: false` there;
pickup latency then falls back to `pollInterval`, and short intervals cost
queries.

**Queue mutation, job dependencies, and flows.** Not implemented. Queue config is
immutable after creation.

**Packaging.** Not published to npm; see [Install](#install).

This library is pre-1.0 in everything but its version number. The schema is not
settled, and future migrations may not be additive.

## Development

```bash
docker compose up -d
npm run migrate:up
npm run typecheck          # src + tests + examples
npm test                   # integration tests against real Postgres (testcontainers)
npm run build
```

Correctness is covered by integration tests in [tests/](tests/) that run against a
real PostgreSQL 16 container — no mocks, no in-memory fake. Individual suites:

```bash
npm run test:client        # the public facade
npm run test:producer      # publishing, idempotency, groups
npm run test:consumer      # workers, retries, poison payloads
npm run test:transaction   # transactional publish
npm run test:crash         # lease fencing across a killed process
npm run test:concurrency   # concurrency caps, as a property test
npm run test:external      # run against an existing Postgres instead of a container
```

Performance harnesses live in [examples/](examples/) — see
[examples/PERFORMANCE_TESTING.md](examples/PERFORMANCE_TESTING.md). They drive the
repositories directly, below the public API, to measure the storage layer.

Internals are reachable by deep import and the tests and benchmarks use that
deliberately, but only what [src/index.ts](src/index.ts) exports is covered by the
package version.

## License

MIT — see [LICENSE](LICENSE).
