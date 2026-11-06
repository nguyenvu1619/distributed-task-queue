# Publishing jobs

## Publish one job

```ts
const job = await jobService.publishJob({
  queueId: queue.id,
  idempotencyKey: `invoice:${invoiceId}`,
  payload: JSON.stringify({ invoiceId }),
});
```

| Field            | Type                                  | Required | Meaning |
| ---------------- | ------------------------------------- | -------- | ------- |
| `queueId`        | number                                | yes      | Target queue. An id that does not exist fails with a foreign-key violation (SQLSTATE `23503`). |
| `idempotencyKey` | string                                | yes      | Identity of the job within its queue. See [Deduplication](#deduplication). |
| `payload`        | string                                | yes      | Stored as is. `WorkerService` parses it with `JSON.parse` and deletes a job whose payload does not parse, without retrying, so use JSON if you use `WorkerService`. |
| `delayMs`        | number                                | no       | Keeps the job from being pulled until this many milliseconds after the publish. See [Delayed jobs](#delayed-jobs). |
| `group`          | `{ id: string; concurrency: number }` | no       | Puts the job in a concurrency group. See [Group concurrency](../concepts/concurrency-control.md#group-concurrency). |
| `metadata`       | object                                | no       | JSON stored with the job and returned as `job.metadata`. The library does not use it. |
| `attempts`       | number                                | no       | Starts the job with this many attempts already spent. Leave it unset. |

The result is a `PublishedJob`: the stored job (`id`, `status`, `availableAt`
and so on) plus a `deduplicated` flag.

Payloads and metadata are stored in plain text in your database. Keep secrets
out of them; store a reference instead.

## Deduplication

Within a queue, an `idempotencyKey` can belong to only one live job. If a live
job already holds the key, publishing it again returns that job with
`deduplicated: true` and leaves the job unchanged:

```ts
const a = await jobService.publishJob({ queueId: queue.id, idempotencyKey: 'order-123', payload: '{"v":1}' });
const b = await jobService.publishJob({ queueId: queue.id, idempotencyKey: 'order-123', payload: '{"v":2}' });

a.deduplicated; // false: this call created the job
b.deduplicated; // true: the key was already taken
b.id === a.id;  // true
b.payload;      // '{"v":1}': the duplicate's payload, delay and group are ignored
```

What to know:

- **Keys are scoped to their queue.** The same key on two queues makes two
  independent jobs.
- **A key is only reserved while its job is alive.** When the job completes
  or is discarded, its row is deleted and the key is free again. Deduplication
  protects against publishing the same work twice while it is queued or
  running, for example when a request is retried. It does not prevent running
  work again after it has finished; record that in your own data if you need
  to.
- **A duplicate does not disturb a running job.** If the existing job is
  `PROCESSING`, it keeps its lease, attempts and status.
- **Derive keys from the business operation**, for example
  `welcome-email:user-42` or `order-confirmation:${orderId}`. A random key
  never matches anything, so it never deduplicates.
- **A duplicate publish locks the existing job's row** until your transaction
  commits. While the lock is held, completing or failing that one job waits.
  Keep transactions short.

Because of deduplication, retrying a publish that failed or timed out is safe:
if the first attempt did land, the retry returns the same job.

## Delayed jobs

```ts
await jobService.publishJob({
  queueId: queue.id,
  idempotencyKey: `trial-ending:${userId}`,
  payload: JSON.stringify({ userId }),
  delayMs: 3 * 24 * 60 * 60 * 1000, // three days
});
```

- The delay is counted from when the publish statement runs, using the
  database clock. Inside your own transaction, it starts at the publish, not
  at `BEGIN`.
- Omitted, `0` or negative: the job is due at once.
- It must be a finite number. `NaN` or `Infinity` throws a `RangeError` before
  anything is sent, and in a batch the whole batch is rejected.
- A delayed job is `PENDING` (`countByStatus` counts it as pending) but no
  pull returns it before it is due. It does not hold a queue or group slot
  while it waits.
- The delay is a minimum. The job runs when it is due, a worker polls, and
  the jobs that became due before it have been taken.
- Publishing the key again does not reschedule the existing job.

There are no recurring (cron) schedules; see
[Known limitations](../known-limitations.md#scheduling).

## Publishing in batches

```ts
const jobs = await jobService.publishJobs(
  orders.map((order) => ({
    queueId: queue.id,
    idempotencyKey: `order-confirmation:${order.id}`,
    payload: JSON.stringify({ orderId: order.id }),
  })),
);
// jobs[i] is the result for the i-th input
```

- **One statement, all or nothing.** The whole batch is one round trip and
  one transaction. If any input fails, for example because of an unknown queue
  id, none of the batch is stored.
- **Results come back in input order.**
- **A batch can mix queues, groups and delays.**
- **A key repeated inside one batch is stored once.** Its first occurrence
  reports `deduplicated: false`, and later occurrences report
  `deduplicated: true` with the same job.
- **An empty array** returns `[]` without touching the database.
- **No order within a batch.** Undelayed jobs in one batch share the same
  `availableAt`, so they come out in no particular order relative to each
  other. Publish one by one if order matters.

There is no enforced batch size limit. Keep batches in the low thousands: the
cost that grows with the batch is the transaction (lock duration, WAL volume,
one connection held), not the number of round trips. For larger volumes, split
the batch yourself. Each chunk is then its own all-or-nothing unit.

## Publishing inside your transaction

`publishJob` and `publishJobs` take an optional second argument: the database
handle to run on. Pass the client of a transaction you have open, and the job
commits together with your own writes, or not at all.

```ts
const client = await pool.connect();
try {
  await client.query('BEGIN');

  await client.query('INSERT INTO orders (id, total) VALUES ($1, $2)', [orderId, total]);

  await jobService.publishJob(
    {
      queueId: queue.id,
      idempotencyKey: `order-confirmation:${orderId}`,
      payload: JSON.stringify({ orderId }),
    },
    client, // same transaction as the INSERT
  );

  await client.query('COMMIT'); // the order and its job land together
} catch (err) {
  await client.query('ROLLBACK'); // neither lands
  throw err;
} finally {
  client.release();
}
```

This removes both failure modes of enqueueing separately. If you write first
and enqueue second, a crash in between loses the job. If you enqueue first,
a rolled-back write leaves a job for an order that does not exist. The job row
plays the role an outbox table usually plays, and the workers poll it.

What to know:

- **Same database only.** The queue tables must live in the same database as
  the rows you write. Across databases, you still need an outbox or two-phase
  commit.
- **The job is invisible until you commit.** No worker can pull it before
  then, and `countByStatus` does not count it.
- **The library never begins, commits or rolls back your transaction.**
- **A duplicate key does not abort your transaction.** It returns the
  existing job instead of raising, so the transaction stays usable. Any other
  error, such as an unknown queue id, aborts it, like any failed statement in
  PostgreSQL.
- **Keep the transaction short.** A publish that creates a group cap, or that
  resolves to an existing job, holds row locks until you commit. Other
  publishers into the same new group wait for them.

Without the second argument, the publish runs on the pool and commits on its
own.

### Other database libraries

The second argument is typed as `Executor`:

```ts
interface Executor {
  query(text: string, values?: any[]): Promise<{ rows: any[]; rowCount: number | null }>;
}
```

A `pg` `Pool`, `PoolClient` or `Client` fits it as is. For a query builder or
ORM, write an adapter that sends the SQL text and values to node-postgres
unchanged and returns the rows. The statements use PostgreSQL's numbered
placeholders (`$1`, `$2`, …) and array parameters, and some libraries rewrite
placeholders in raw SQL, so check that yours passes them through untouched.
The library's tests only cover node-postgres clients.

## Errors

| Situation                          | Error |
| ---------------------------------- | ----- |
| `queueId` does not exist           | node-postgres error with `code` `'23503'` (foreign-key violation) |
| `delayMs` is `NaN` or `Infinity`   | `RangeError`; nothing is published |
| Group `concurrency` is negative    | node-postgres error with `code` `'23514'` (check violation) |
| Database unreachable               | node-postgres connection error |

The library does not retry publishes. If you retry them yourself,
deduplication makes that safe.
