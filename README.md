# Distributed Task Queue

A lightweight TypeScript library for distributed task queue management using PostgreSQL. This library provides a clean, simple API for job producers and workers to manage task queues with features like job leasing, concurrency control, and automatic job recovery.

## Features

- **Job Management**: Publish, pull, complete, and fail jobs
- **Queue Management**: Create and manage queues with configurable concurrency
- **Job Leasing**: Automatic job locking with expiration to prevent duplicate processing
- **Job Recovery**: Reaper service to recover expired jobs
- **TypeScript**: Full TypeScript support with type safety
- **Lightweight**: Minimal dependencies (only `pg` and `dotenv`)

## Installation

```bash
npm install distributed-task-queue
```

Or with yarn:

```bash
yarn add distributed-task-queue
```

## Prerequisites

- Node.js 18+ 
- PostgreSQL 12+
- TypeScript 5+ (for TypeScript projects)

## Quick Start

### 1. Set up Database

Start PostgreSQL using Docker Compose:

```bash
docker-compose up -d postgres
```

### 2. Run Migrations

```bash
npm run migrate:up
```

Or using ts-node directly:

```bash
ts-node src/migration/runner.ts up
```

### 3. Use the Library

#### Producer Example

```typescript
import { createPool, JobService, QueueService } from 'distributed-task-queue';
import * as dotenv from 'dotenv';

dotenv.config();

// Create database connection pool
const pool = createPool({
  host: process.env.DATABASE_HOST || 'localhost',
  port: parseInt(process.env.DATABASE_PORT || '5432', 10),
  user: process.env.DATABASE_USER || 'user',
  password: process.env.DATABASE_PASS || 'password',
  database: process.env.DATABASE_NAME || 'queue',
});

// Initialize repositories and services
import { JobRepository, QueueRepository } from 'distributed-task-queue';

const jobRepo = new JobRepository(pool);
const queueRepo = new QueueRepository(pool);
const queueService = new QueueService(queueRepo);
const jobService = new JobService(jobRepo, queueRepo);

// Create a queue (if not exists)
const queue = await queueService.createQueue({
  name: 'my-queue',
  maxAttempts: 3,
  leaseDuration: 30000, // 30 seconds in milliseconds
  concurrency: 10, // 10 concurrent workers
});

// Publish a job
const job = await jobService.publishJob({
  idempotencyKey: 'unique-job-key-123',
  payload: JSON.stringify({ task: 'process-data', data: { id: 1 } }),
  queueId: queue.id,
  groupId: 'group-123',
});
```

#### Transactional Publish (no outbox table)

`publishJob` takes an optional second argument: any handle that can run
`query(text, values)`. Pass the client you are already holding open and the job
row commits with the writes that justify it, or not at all — which is what an
outbox table exists to buy you. The job row *is* the outbox row here, and the
worker is what polls it.

```typescript
const client = await pool.connect();
try {
  await client.query('BEGIN');

  await client.query('INSERT INTO orders (id, total) VALUES ($1, $2)', [orderId, total]);

  await jobService.publishJob(
    {
      idempotencyKey: `order-confirmation-${orderId}`,
      payload: JSON.stringify({ orderId }),
      queueId: queue.id,
    },
    client // <- same transaction as the INSERT above
  );

  await client.query('COMMIT'); // order and job land together
} catch (err) {
  await client.query('ROLLBACK'); // neither lands; no job for an order that does not exist
  throw err;
} finally {
  client.release();
}
```

Omit the argument and the publish runs on the pool and commits on its own, as
before.

Three things to know:

- **Same database only.** This works because the job row and the business rows
  share one transaction. If the queue lives in a different database or cluster,
  you still need an outbox (or two-phase commit) — nothing here makes that case
  atomic.
- **The job is invisible until you commit.** No worker can pull it before then,
  which is the intended behaviour but worth stating: publish latency becomes
  your transaction's latency.
- **Keep the transaction short.** A publish that seeds a group limit holds the
  `group_queue_limits` row lock until you commit, which stalls other publishers
  into the same group.

The parameter is typed structurally (`Executor`), not as a pg `PoolClient`, so a
Knex / Kysely / Drizzle transaction handle works through a small adapter:

```typescript
import type { Executor } from 'distributed-task-queue';

const asExecutor = (trx: Knex.Transaction): Executor => ({
  query: (text, values) => trx.raw(text, values ?? []),
});
```

#### Batched Publish

`publishJobs` takes an array and lands it as **one statement** — one round trip,
one transaction, all or nothing. It accepts the same optional executor, so a
batch can also ride inside your own transaction.

```typescript
const jobs = await jobService.publishJobs(
  orders.map((order) => ({
    idempotencyKey: `order-confirmation-${order.id}`,
    payload: JSON.stringify({ orderId: order.id }),
    queueId: queue.id,
  }))
);
// jobs[i] corresponds to orders[i]
```

The rows travel as parallel arrays through `unnest`, not as a generated `VALUES`
list, so the statement uses a fixed ten bind parameters no matter how large the
batch is. A 10,000-job batch is one statement, not a chunked loop — chunking is
what would silently cost the all-or-nothing guarantee.

Batches may mix queues and groups freely. Group caps are seeded once per
`(queue, group)` pair, and an existing cap is left alone — the same
`ON CONFLICT DO NOTHING` rule as a single publish.

**On batch size.** There is no enforced cap — the statement has no parameter
ceiling to hit, so nothing in the implementation forces one. Practical guidance:
keep a batch in the low thousands. What grows with batch size is the transaction
itself — lock duration, WAL volume, and how long one pooled connection is tied
up — not the round-trip count. For reference, Sidekiq's `push_bulk` and Oban's
`insert_all` both default to slicing at 1,000; River, BullMQ, pg-boss and
Graphile Worker document no limit at all. If you need to publish far more than
that, chunk it yourself — that way you choose the atomicity you want, rather
than having this library silently split one all-or-nothing batch into several
that are not.

Two things to know:

- **Jobs in one batch share a `created_at`.** `now()` is the transaction's start
  time, and pulls order by `created_at`, so jobs within a single batch come out
  in no particular order relative to each other. Ordering across batches is
  unaffected. If you need strict FIFO inside a batch, publish individually.
- **A key repeated inside one batch is inserted once.** Repeats are collapsed
  before the statement runs; the **first** occurrence owns the row and reports
  `deduplicated: false`, later occurrences describe the same row with
  `deduplicated: true`. A duplicate no longer takes the rest of the batch down
  with it.

#### Deduplicating Publish

Publishing an `idempotencyKey` that a live job already holds returns **that job**
with `deduplicated: true` instead of raising, so a retrying publisher gets
exactly one job and exactly one delivery.

```typescript
const first  = await jobService.publishJob({ idempotencyKey: 'order-123', ... });
const second = await jobService.publishJob({ idempotencyKey: 'order-123', ... });

first.deduplicated;   // false — this call inserted the row
second.deduplicated;  // true  — a live job already held the key
second.id === first.id;  // true, and only one row exists
```

This matters most inside a transaction. PostgreSQL aborts the **entire**
enclosing transaction on an unhandled constraint violation, so a publish that
raised on a duplicate would take the caller's business writes down with it —
the exact failure the `executor` argument exists to prevent. Returning the
existing job leaves the transaction usable.

**Keys are scoped to their queue.** `UNIQUE (queue_id, idempotency_key)`, so one
business id can back a job on the email queue and another on the invoicing queue
without either displacing the other:

```typescript
await jobService.publishJobs([
  { idempotencyKey: 'order-123', queueId: emailQueue.id,   payload: ... },
  { idempotencyKey: 'order-123', queueId: invoiceQueue.id, payload: ... },
]);
// two jobs, both deduplicated: false
```

**A key is only reserved while its job is alive.** Completed and failed jobs are
deleted, which frees the key — so the dedup window is the job's lifetime, not
for ever. Republishing `order-123` after its job finished creates a new job.

**It is one statement.** The insert and the conflict are resolved together via
`ON CONFLICT ... DO UPDATE ... RETURNING`, so the publish never has to ask a
second time who holds a key — which is where a duplicate could otherwise slip
away between statements by settling and being deleted. PostgreSQL re-drives the
insert internally if the conflicting row disappears mid-flight.

A duplicate does write the row it resolves to — that write is what makes
`RETURNING` hand the row back — but it writes only `jobs.revision`, a counter of
how many publishes have landed on that row after the first. `status`, `attempts`
and the lease columns are left alone, so a job a worker is holding right now is
unaffected.

The cost is a row lock on that job, held until **your** transaction commits.
`pullJob` and the reaper both use `SKIP LOCKED` and step around it, but that one
job's `completeJob`/`failJob` waits — one more reason to keep the transaction
short.

#### Worker Example

```typescript
import { createPool, JobService, QueueService, JobRepository, QueueRepository } from 'distributed-task-queue';
import * as dotenv from 'dotenv';

dotenv.config();

const pool = createPool({
  host: process.env.DATABASE_HOST || 'localhost',
  port: parseInt(process.env.DATABASE_PORT || '5432', 10),
  user: process.env.DATABASE_USER || 'user',
  password: process.env.DATABASE_PASS || 'password',
  database: process.env.DATABASE_NAME || 'queue',
});

const jobRepo = new JobRepository(pool);
const queueRepo = new QueueRepository(pool);
const queueService = new QueueService(queueRepo);
const jobService = new JobService(jobRepo, queueRepo);

// Worker loop
async function workerLoop(queueId: number) {
  while (true) {
    try {
      // Pull a job from the queue
      const job = await jobService.pullJob(queueId);

      if (!job) {
        // No jobs available, wait a bit before trying again
        await new Promise(resolve => setTimeout(resolve, 1000));
        continue;
      }

      try {
        // Process the job
        const payload = JSON.parse(job.payload);
        console.log('Processing job:', job.id, payload);

        // Your processing logic here
        await processJob(payload);

        // Mark job as completed
        await jobService.completeJob(job.id, job.lockToken);
        console.log('Job completed:', job.id);
      } catch (error) {
        console.error('Job processing failed:', error);
        // Mark job as failed
        await jobService.failJob(job.id, job.lockToken);
      }
    } catch (error) {
      console.error('Worker error:', error);
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
  }
}

async function processJob(payload: any) {
  // Your job processing logic
}

// Start worker
workerLoop(1); // queue ID 1
```

#### Reaper Service (Job Recovery)

```typescript
import { createPool, ReaperService, JobRepository, QueueRepository } from 'distributed-task-queue';

import { createPool, JobRepository, QueueRepository } from 'distributed-task-queue';

const pool = createPool({ /* ... */ });
const jobRepo = new JobRepository(pool);
const queueRepo = new QueueRepository(pool);

// Create reaper service
const reaper = new ReaperService(jobRepo, queueRepo, {
  interval: 30000, // Check every 30 seconds
  batchSize: 100,  // Process up to 100 jobs at a time
});

// Start the reaper
reaper.start();

// Stop the reaper when needed
// reaper.stop();
```

## API Reference

### JobService

#### `publishJob(input: CreateJobInput, executor?: Executor): Promise<PublishedJob>`
Publishes a new job to the queue. Pass `executor` — an open `PoolClient`, or any
handle exposing `query(text, values)` — to enrol the insert in your own
transaction so the job commits with your business writes. A key a live job
already holds comes back as that job with `deduplicated: true` rather than
raising. See [Transactional Publish](#transactional-publish-no-outbox-table) and
[Deduplicating Publish](#deduplicating-publish).

#### `publishJobs(inputs: CreateJobInput[], executor?: Executor): Promise<PublishedJob[]>`
Publishes many jobs as a single statement — one round trip, all-or-nothing.
Returns one result per input, in input order. See
[Batched Publish](#batched-publish).

#### `countByStatus(queueId: number): Promise<{ pending: number; processing: number }>`
Live backlog for a queue. Terminal jobs are deleted, so this is all of it.

#### `pullJob(queueId: number): Promise<Job | null>`
Pulls and locks a job from the specified queue. Returns `null` if no jobs are available.

#### `completeJob(id: number, lockToken: number): Promise<Job>`
Marks a job as completed. Requires the correct lock token.

#### `failJob(id: number, lockToken: number): Promise<Job>`
Marks a job as failed. Requires the correct lock token.

#### `getJob(id: number): Promise<Job>`
Retrieves a job by ID.

#### `pullJobs(status: JobStatus, limit: number): Promise<Job[]>`
Retrieves multiple jobs with the specified status.

### QueueService

#### `createQueue(input: CreateQueueInput): Promise<Queue>`
Creates a new queue with the specified configuration.

#### `getQueue(id: number): Promise<Queue>`
Retrieves a queue by ID.

#### `getAllQueues(): Promise<Queue[]>`
Retrieves all queues.

### ReaperService

#### `start(): void`
Starts the reaper service to automatically recover expired jobs.

#### `stop(): void`
Stops the reaper service.

#### `runOnce(): Promise<number[]>`
Manually run the reaper once. Returns array of recovered job IDs.

## Architecture

The library follows Clean Architecture principles:

```
src/
├── domain/           # Domain models and types
├── repository/       # PostgreSQL repository implementations
├── services/         # Business logic layer
└── index.ts         # Public API exports
```

### Domain Layer
- `job.ts`: Job domain model and types
- `queue.ts`: Queue domain model and types
- `group.ts`: Group domain model
- `errors.ts`: Custom error classes

### Repository Layer
- `job.repository.ts`: Job data access
- `queue.repository.ts`: Queue data access with caching
- `connection.ts`: Database connection pool setup

### Service Layer
- `job.service.ts`: Job business logic
- `queue.service.ts`: Queue business logic
- `reaper.service.ts`: Job recovery service

## Environment Variables

Create a `.env` file:

```env
DATABASE_HOST=localhost
DATABASE_PORT=5432
DATABASE_USER=user
DATABASE_PASS=password
DATABASE_NAME=queue
MIGRATIONS_PATH=migrations
```

## Migrations

### Apply Migrations

```bash
npm run migrate:up
```

### Rollback Migrations

```bash
npm run migrate:down
```

Or rollback specific number of migrations:

```bash
ts-node src/migration/runner.ts down 1
```

## Development

### Build

```bash
npm run build
```

### Development Mode (Watch)

```bash
npm run dev
```

## License

MIT
