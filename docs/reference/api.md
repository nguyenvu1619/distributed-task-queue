# API reference

Everything on this page is exported from the package root:

```ts
import { JobService, WorkerService /* … */ } from 'distributed-task-queue';
```

Most applications only need `createPool`, the two repositories, `JobService`,
`QueueService`, `WorkerService` and `ReaperService`, wired as in the
[Quick start](../getting-started/quick-start.md).

- [createPool](#createpool) · [Pool](#pool)
- [JobService](#jobservice) · [QueueService](#queueservice)
- [WorkerService](#workerservice) · [ReaperService](#reaperservice)
- [JobRepository](#jobrepository) · [QueueRepository](#queuerepository)
- [Types](#types)
- Errors: see [Errors](errors.md)

## createPool

```ts
function createPool(config: DatabaseConfig): Pool;
```

Creates a node-postgres `Pool`.

| Field                     | Type                 | Default | Notes |
| ------------------------- | -------------------- | ------- | ----- |
| `connectionString`        | string               | none    | `postgres://user:pass@host:5432/db`. When set, `host`, `port`, `user`, `password` and `database` are ignored. |
| `host`                    | string               | none    | |
| `port`                    | number               | none    | |
| `user`                    | string               | none    | |
| `password`                | string               | none    | |
| `database`                | string               | none    | |
| `ssl`                     | node-postgres `ssl`  | none    | Passed through to node-postgres. |
| `applicationName`         | string               | none    | Shown in `pg_stat_activity`. |
| `max`                     | number               | `20`    | Most connections in the pool. |
| `idleTimeoutMillis`       | number               | `30000` | Closes connections idle this long. |
| `connectionTimeoutMillis` | number               | `2000`  | Fails a checkout that waits this long. |

Fields left unset fall back to node-postgres's own defaults. `0` for `max`,
`idleTimeoutMillis` or `connectionTimeoutMillis` is replaced by the default
above.

## Pool

The `Pool` class of node-postgres, re-exported. Use it if you want to build the
pool yourself.

## JobService

```ts
new JobService(jobRepo: JobRepository, queueRepo: QueueRepository)
```

### publishJob

```ts
publishJob(input: CreateJobInput, executor?: Executor): Promise<PublishedJob>
```

Publishes one job. With `executor`, it runs on that handle, for example inside
your transaction. A key that a live job already holds returns that job with
`deduplicated: true`. See [Publishing jobs](../guides/publishing-jobs.md).

### publishJobs

```ts
publishJobs(inputs: CreateJobInput[], executor?: Executor): Promise<PublishedJob[]>
```

Publishes many jobs in one statement, all or nothing. Returns one result per
input, in input order.

### pullJob

```ts
pullJob(queueId: number): Promise<Job | null>
```

Leases the next due job of the queue, or returns `null` if there is none or it
cannot be admitted. Throws `QueueNotFoundError` for an unknown queue.

### pullJobDirect

```ts
pullJobDirect(queue: Queue): Promise<Job | null>
```

Same as `pullJob`, given the `Queue` object.

### completeJob

```ts
completeJob(id: number, lockSeq: number): Promise<void>
```

Records success and deletes the job. Reads the job and its queue first, so it
costs an extra query. Throws `JobNotFoundError` if the job no longer exists,
and `LeaseLostError` if `lockSeq` does not hold its lease.

### completeJobDirect

```ts
completeJobDirect(id: number, lockSeq: number, queue: Queue, timeoutMs?: number): Promise<void>
```

Same as `completeJob` without the reads. Throws `LeaseLostError` if `lockSeq`
does not hold the lease, including when the job no longer exists. `timeoutMs`
abandons the statement on the client after that many milliseconds.

### failJob

```ts
failJob(id: number, lockSeq: number): Promise<Job>
```

Records an ordinary, retryable failure. Returns the job: `status` is `PENDING`
if it will run again, `FAILED` if it was discarded. Reads the job first, like
`completeJob`, and throws the same errors.

### failJobDirect

```ts
failJobDirect(id: number, lockSeq: number, queue: Queue, nonRetryable?: boolean, timeoutMs?: number): Promise<void>
```

Same as `failJob` without the reads. `nonRetryable: true` discards the job
whatever attempts remain. Throws `LeaseLostError`.

### snoozeJobDirect

```ts
snoozeJobDirect(id: number, lockSeq: number, queue: Queue, delayMs: number, timeoutMs?: number): Promise<void>
```

Returns the job to `PENDING`, due after `delayMs`, releases its slots and
refunds the attempt. Throws `LeaseLostError`.

### getJob

```ts
getJob(id: number): Promise<Job>
```

Reads a live job. Throws `JobNotFoundError`, which includes jobs that have been
completed or discarded.

### getQueue

```ts
getQueue(queueId: number): Promise<Queue>
```

Throws `QueueNotFoundError`.

### countByStatus

```ts
countByStatus(queueId: number): Promise<{ pending: number; processing: number }>
```

Live jobs of one queue by status. `pending` includes jobs that are not due yet.

### pullJobs

```ts
pullJobs(status: JobStatus, limit: number): Promise<Job[]>
```

Lists up to `limit` jobs with the given status, across **all** queues, oldest
first. Despite the name, it does not lease anything. Use it for inspection.

## QueueService

```ts
new QueueService(queueRepo: QueueRepository)
```

### createQueue

```ts
createQueue(input: CreateQueueInput): Promise<Queue>
```

Creates a queue, or returns the existing queue with that name without changing
it. See [Queues](../guides/queues.md).

### getQueue

```ts
getQueue(id: number): Promise<Queue>
```

Throws `QueueNotFoundError`.

### getAllQueues

```ts
getAllQueues(): Promise<Queue[]>
```

## WorkerService

```ts
new WorkerService(jobService: JobService, options: WorkerOptions)
```

Runs `options.handler` for jobs of one queue. See [Workers](../guides/workers.md).

### start

```ts
start(): Promise<void>
```

Looks up the queue, starts the slots and resolves. The slots keep running until
`stop()`. Rejects with `QueueNotFoundError` if the queue does not exist. Does
nothing if the worker is already running.

### stop

```ts
stop(): Promise<void>
```

Stops pulling and resolves once every slot has finished its current job,
including the settle.

### isRunning

```ts
isRunning(): boolean
```

## ReaperService

```ts
new ReaperService(jobRepo: JobRepository, options?: ReaperServiceOptions)
```

Returns jobs with expired leases to the queue. See
[Crash recovery](../guides/crash-recovery.md).

### start

```ts
start(): void
```

Runs a pass immediately, then one every `interval` milliseconds after the
previous pass ends. Does nothing if already running.

### stop

```ts
stop(): Promise<void>
```

Stops scheduling passes and waits for one in flight.

### runOnce

```ts
runOnce(): Promise<number[]>
```

Runs one pass. Resolves with the ids of the jobs returned to the queue; jobs
deleted for running out of attempts are not included.

### isRunning

```ts
isRunning(): boolean
```

## JobRepository

```ts
new JobRepository(pool: Pool, logger?: Logger)
```

Runs the job SQL. The services delegate to it. Use it directly when you need a
method the services do not expose, such as `snoozeJob` with a return value or
`recoverJobs`.

| Method | Returns | Notes |
| ------ | ------- | ----- |
| `getById(id)` | `Promise<Job>` | Throws `JobNotFoundError`. |
| `publishJob(input, executor?)` | `Promise<PublishedJob>` | |
| `publishJobs(inputs, executor?)` | `Promise<PublishedJob[]>` | |
| `pullJob(queue)` | `Promise<Job \| null>` | Takes a `Queue` object. |
| `completeJob(id, lockSeq, queue, timeoutMs?)` | `Promise<void>` | Throws `LeaseLostError`. |
| `failJob(id, lockSeq, queue, nonRetryable = false, timeoutMs?)` | `Promise<Job>` | `status` is `PENDING` (retried) or `FAILED` (discarded). Throws `LeaseLostError`. |
| `snoozeJob(id, lockSeq, queue, delayMs, timeoutMs?)` | `Promise<Job>` | The job as rescheduled. Throws `LeaseLostError`. |
| `recoverJobs(limit = 100)` | `Promise<number[]>` | One reaper pass. |
| `countByStatus(queueId)` | `Promise<{ pending; processing }>` | |
| `pullJobs(status, limit)` | `Promise<Job[]>` | Read-only listing across all queues. |

`logger` defaults to the console. See [Logging](../guides/production.md#logging).

## QueueRepository

```ts
new QueueRepository(pool: Pool, logger?: Logger)
```

| Method | Returns | Notes |
| ------ | ------- | ----- |
| `createQueue(input)` | `Promise<Queue>` | |
| `getById(id)` | `Promise<Queue>` | Served from an in-memory cache after the first read. Throws `QueueNotFoundError`. |
| `getAll()` | `Promise<Queue[]>` | Always reads the database, and refreshes the cache. |

Each `QueueRepository` instance has its own cache.

## Types

### Job

```ts
interface Job {
  id: number;
  queueId: number;
  idempotencyKey: string;
  payload: string;
  status: JobStatus;
  groupId: string | null;
  attempts: number;
  metadata: Metadata;
  availableAt: Date;
  leaseExpiresAt: Date | null;
  lockSeq: number;
  queueShardNo: number | null;
  createdAt: Date;
  updatedAt: Date;
  completedAt: Date | null;
}
```

| Field | Meaning |
| ----- | ------- |
| `id` | Database id. Stable across the retries of one job. |
| `queueId` | Queue the job belongs to. |
| `idempotencyKey` | Identity of the job within its queue. |
| `payload` | The string given at publish. |
| `status` | `PENDING` or `PROCESSING` for a live job. `COMPLETED` and `FAILED` only appear on objects returned by settle calls. |
| `groupId` | Concurrency group, or `null`. |
| `attempts` | Leases taken so far, the current one included. `1` during the first run. Snoozes are refunded. |
| `metadata` | The object given at publish, or `{}`. |
| `availableAt` | Earliest time a pull may return the job. |
| `leaseExpiresAt` | When the current lease ends. `null` unless `PROCESSING`. |
| `lockSeq` | Fence token of the current lease. Pass it to every settle call. `0` means never leased. |
| `queueShardNo` | Counter row holding the job's queue slot, on a capped queue. Internal. |
| `createdAt` | Start time of the transaction that published the job. |
| `updatedAt` | Last change to the row. |
| `completedAt` | `null`, except on the object `failJob` returns when it discards the job. |

### PublishedJob

```ts
interface PublishedJob extends Job {
  deduplicated: boolean; // true if the key was already held by a live job
}
```

### CreateJobInput

```ts
interface CreateJobInput {
  queueId: number;
  idempotencyKey: string;
  payload: string;
  delayMs?: number;
  group?: Group;
  metadata?: Metadata;
  attempts?: number;
}
```

Fields are described in [Publishing jobs](../guides/publishing-jobs.md#publish-one-job).

### Group

```ts
interface Group {
  id: string;
  concurrency: number; // cap on PROCESSING jobs of this group in this queue
}
```

### Metadata

```ts
interface Metadata {
  consumerId?: string;
  lastPullAt?: Date;
  [key: string]: any;
}
```

Free-form JSON. The library does not set any of it. On read, the keys
`consumer_id` and `last_pull_at` are renamed to `consumerId` and `lastPullAt`.

### JobStatus

```ts
enum JobStatus {
  PENDING = 'PENDING',
  PROCESSING = 'PROCESSING',
  COMPLETED = 'COMPLETED',
  FAILED = 'FAILED',
}
```

### Queue

```ts
interface Queue {
  id: number;
  name: string;
  maxAttempts: number;
  leaseDuration: number; // milliseconds
  concurrency: number | null;
  requiresGroupId: boolean;
  createdAt: Date;
  updatedAt: Date;
}
```

### CreateQueueInput

```ts
interface CreateQueueInput {
  name: string;
  maxAttempts: number;
  leaseDuration: number;     // milliseconds
  concurrency: number;       // 0 = no cap
  requiresGroupId?: boolean; // default false
}
```

### WorkerOptions

```ts
interface WorkerOptions {
  queueId: number;
  handler: JobHandler;
  concurrency?: number;  // default 1
  pollInterval?: number; // ms, default 1000
  settleRetry?: SettleRetryOptions;
}
```

### JobHandler

```ts
type JobHandler<T = any> = (job: Job, payload: T) => Promise<void>;
```

### SettleRetryOptions

```ts
interface SettleRetryOptions {
  deadlineMs?: number;       // default 30000; 0 turns retries off
  attemptTimeoutMs?: number; // default 5000
  baseDelayMs?: number;      // default 100
  maxDelayMs?: number;       // default 2000
}
```

See [Recording outcomes through database errors](../guides/workers.md#recording-outcomes-through-database-errors).

### ReaperServiceOptions

```ts
interface ReaperServiceOptions {
  interval?: number;  // ms, default 30000
  batchSize?: number; // default 100
  logger?: Logger;    // default: console
}
```

### DatabaseConfig

The options of [createPool](#createpool).

### Executor

```ts
interface Executor {
  query(text: string, values?: any[]): Promise<QueryResultLike>;
}

interface QueryResultLike {
  rows: any[];
  rowCount: number | null;
}
```

Any handle that can run a statement: a `pg` `Pool`, `PoolClient` or `Client`,
or an adapter around another library. See
[Other database libraries](../guides/publishing-jobs.md#other-database-libraries).

### GroupQueueLimits and QueueShards

Shapes of the rows in `group_queue_limits` and `queue_shards`. No method
returns them; they are there for code that reads those tables directly.

### NUMBER_OF_SHARD

```ts
const NUMBER_OF_SHARD = 32;
```

The most counter rows a capped queue's `concurrency` is spread across.

### Logger

The logger interface is not exported. Any object with `debug`, `info`, `warn`
and `error` methods taking `(message: string, ...args: unknown[])` works. See
[Logging](../guides/production.md#logging).
