# Running in production

## Process layout

A typical deployment has three kinds of process, all talking only to
PostgreSQL:

- **Application processes** publish jobs, often inside the transaction of the
  request that causes them.
- **Worker processes** run one `WorkerService` per queue they serve, plus a
  `ReaperService`.
- **A migration step** applies the schema before new code starts. See
  [Installation](../getting-started/installation.md#create-the-database-schema).

Each kind scales horizontally. All coordination happens in the database.

## Connection pool

`createPool` returns a node-postgres `Pool`:

```ts
const pool = createPool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: true },
  applicationName: 'billing-worker', // shows up in pg_stat_activity
  max: 20,                           // default 20
  connectionTimeoutMillis: 2000,     // default 2000
  idleTimeoutMillis: 30000,          // default 30000
});

pool.on('error', (err) => {
  console.warn('idle database client error', err);
});
```

**Always add an `error` listener.** When the database restarts or the network
drops, idle clients in the pool emit `error` on the pool. With no listener,
Node.js treats it as an unhandled error and the process exits.

**Sizing.** A worker slot uses a connection only while a pull or settle
statement runs, not while your handler runs, so `max` does not need to match
the worker's `concurrency`. If a slot waits longer than
`connectionTimeoutMillis` for a connection, its pull fails, is logged, and the
slot sleeps for `pollInterval`. The sum of `max` across all your processes
must stay within the server's `max_connections` (100 by default), with room
for everything else that connects.

`createPool` replaces `0` for `max`, `idleTimeoutMillis` or
`connectionTimeoutMillis` with its default. If you need node-postgres's
meaning of `0` (no timeout), create the pool yourself with `new Pool(...)`
(re-exported from this package) and pass it to the repositories.

## Logging

`JobRepository`, `QueueRepository` and `ReaperService` accept a logger. Without
one they write to the console. A logger is any object with four methods:

```ts
interface Logger {
  debug(message: string, ...args: unknown[]): void;
  info(message: string, ...args: unknown[]): void;
  warn(message: string, ...args: unknown[]): void;
  error(message: string, ...args: unknown[]): void;
}
```

The interface is not exported from the package, but any object with this shape
works. For example, forwarding to a pino-style structured logger:

```ts
const queueLogger = {
  debug: (message: string, ...args: unknown[]) => log.debug({ args }, message),
  info: (message: string, ...args: unknown[]) => log.info({ args }, message),
  warn: (message: string, ...args: unknown[]) => log.warn({ args }, message),
  error: (message: string, ...args: unknown[]) => log.error({ args }, message),
};

const jobRepo = new JobRepository(pool, queueLogger);
const queueRepo = new QueueRepository(pool, queueLogger);
const reaper = new ReaperService(jobRepo, { logger: queueLogger });
```

What gets logged:

| Source            | Level   | When |
| ----------------- | ------- | ---- |
| `JobRepository`   | `warn`  | A pull on the coordination path was refused because the queue has no free slot (`No available queue shard for queue Q`) or the job's group is at its cap (`Group queue limit reached for group G and queue Q`). |
| `QueueRepository` | `warn`  | `createQueue` found an existing queue with that name. |
| `ReaperService`   | `info`  | Start, stop, and each pass that returned jobs to the queue. |
| `ReaperService`   | `error` | A pass failed. |
| `WorkerService`   | console only | Start and stop, failed jobs, settle retries, lost leases, pull errors. See [Workers](workers.md#logging). |

The refused-pull warning is normal on a saturated queue, not an error. While a
queue or group is full, every slot logs it once per poll. On busy capped
queues, drop that message or send it to a sampled sink.

## Graceful shutdown

Stop the workers first, so they finish the jobs in hand; then the reaper; then
close the pool.

```ts
async function shutdown(): Promise<void> {
  await Promise.all(workers.map((worker) => worker.stop())); // finish jobs in hand
  await reaper.stop();                                        // finish a pass in flight
  await pool.end();
}

process.once('SIGTERM', () => {
  shutdown().then(
    () => process.exit(0),
    (err) => {
      console.error('shutdown failed', err);
      process.exit(1);
    },
  );
});
```

`worker.stop()` waits for running handlers without a timeout. If your
platform kills the process after a grace period, jobs that were cut off are
recovered by the reaper once their leases expire.

## Monitoring

`jobService.countByStatus(queueId)` returns `{ pending, processing }` for one
queue. `pending` includes delayed and snoozed jobs that are not due yet.

For more, query the tables directly. These are read-only and safe to run at
any time.

Due backlog, and how long the oldest due job has waited, per queue:

```sql
SELECT q.name, count(*) AS due, max(now() - j.available_at) AS oldest_wait
FROM jobs j
JOIN queues q ON q.id = j.queue_id
WHERE j.status = 'PENDING' AND j.available_at <= now()
GROUP BY q.name;
```

Jobs whose lease has expired. The reaper should keep this near zero; a growing
count means no reaper is running.

```sql
SELECT queue_id, count(*) AS expired
FROM jobs
WHERE status = 'PROCESSING' AND lease_expires_at <= now()
GROUP BY queue_id;
```

Slot usage of capped queues:

```sql
SELECT queue_id, sum(running) AS running, sum(max_running) AS cap
FROM queue_shards
GROUP BY queue_id;
```

Groups that currently have jobs running:

```sql
SELECT queue_id, group_id, running, max_running
FROM group_queue_limits
WHERE running > 0
ORDER BY running DESC
LIMIT 20;
```

Every job is inserted, updated and then deleted, so the `jobs` table produces
dead rows continuously. Leave autovacuum on, and watch the table's bloat on
busy queues.

## BIGINT values come back as numbers

Importing the library registers a node-postgres type parser that returns
`BIGINT` (`int8`) values as JavaScript numbers instead of strings. It applies
to every query that goes through the same copy of `pg` in the process,
**including your own queries**:

- `count(*)` and other `bigint` results come back as numbers, not strings.
- Values above `Number.MAX_SAFE_INTEGER` (2^53 − 1) lose precision. 64-bit
  identifiers such as snowflake ids are affected.

If your code relies on `BIGINT` arriving as a string, cast those columns in
your queries, for example `SELECT id::text AS id …`.

## Security

Payloads and metadata are stored in plain text in the `jobs` table. Do not put
secrets or credentials in them; store a reference and look the secret up in
the handler. The worker logs every error a handler throws, so keep secrets out
of error messages too.
