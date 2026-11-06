# Quick start

This page builds one file that creates a queue, publishes three jobs, processes
them with a worker, and shuts down cleanly. It takes about five minutes.

## 1. Start PostgreSQL

Any PostgreSQL 12+ server works. For a local one:

```bash
docker run -d --name taskqueue-pg -p 5432:5432 \
  -e POSTGRES_USER=user -e POSTGRES_PASSWORD=password -e POSTGRES_DB=queue \
  postgres:16
```

Then [create the schema](installation.md#create-the-database-schema).

## 2. Connect and wire up the services

The library is a few small classes that you wire together. Repositories run
the SQL; services sit on top of them.

```ts
import {
  createPool,
  JobRepository,
  QueueRepository,
  JobService,
  QueueService,
  WorkerService,
  ReaperService,
} from 'distributed-task-queue';

const pool = createPool({
  connectionString: process.env.DATABASE_URL ?? 'postgres://user:password@localhost:5432/queue',
});
pool.on('error', (err) => console.error('Idle database client error:', err));

const jobRepo = new JobRepository(pool);
const queueRepo = new QueueRepository(pool);
const jobService = new JobService(jobRepo, queueRepo);
const queueService = new QueueService(queueRepo);
```

The `pool.on('error')` listener matters. Without it, a dropped database
connection crashes the process. See
[Running in production](../guides/production.md#connection-pool).

## 3. Create a queue

```ts
const queue = await queueService.createQueue({
  name: 'emails',
  maxAttempts: 3,        // how many times a job may run before it is discarded
  leaseDuration: 60_000, // ms a worker may hold a job before it is presumed dead
  concurrency: 0,        // 0 = no queue-wide cap
});
```

`createQueue` is safe to call on every startup. If a queue with that name
already exists, you get the existing queue back. Its stored configuration is
kept and the values you passed are ignored. See [Queues](../guides/queues.md).

## 4. Publish jobs

```ts
for (const userId of [1, 2, 3]) {
  const job = await jobService.publishJob({
    queueId: queue.id,
    idempotencyKey: `welcome-email:${userId}`,
    payload: JSON.stringify({ userId }),
  });
  console.log(`published job ${job.id} (deduplicated: ${job.deduplicated})`);
}
```

The `idempotencyKey` identifies the job within its queue. Publishing the same
key again while the job is still queued or running returns the existing job
with `deduplicated: true`.

## 5. Process jobs

```ts
const worker = new WorkerService(jobService, {
  queueId: queue.id,
  concurrency: 2, // jobs this process runs at once
  handler: async (job, payload: { userId: number }) => {
    console.log(`sending welcome email to user ${payload.userId} (attempt ${job.attempts})`);
    await new Promise((resolve) => setTimeout(resolve, 200)); // pretend to work
  },
});
```

The handler receives the job and its parsed JSON payload. What the handler
does decides what happens to the job:

- **It returns normally:** the job is completed and deleted.
- **It throws:** the job is retried, up to `maxAttempts` runs in total.

[Workers](../guides/workers.md) covers the other outcomes: snoozing a job and
failing it for good.

## 6. Recover jobs whose worker died

```ts
const reaper = new ReaperService(jobRepo);
```

If a worker crashes mid-job, the job stays leased until its lease expires.
The reaper is what puts it back in the queue. Run at least one reaper
somewhere. See [Crash recovery](../guides/crash-recovery.md).

## 7. Start, and stop cleanly

```ts
await worker.start();
reaper.start();

process.once('SIGINT', async () => {
  await worker.stop(); // waits for the jobs in hand
  await reaper.stop();
  await pool.end();
});
```

## The whole file

```ts
import {
  createPool,
  JobRepository,
  QueueRepository,
  JobService,
  QueueService,
  WorkerService,
  ReaperService,
} from 'distributed-task-queue';

async function main() {
  const pool = createPool({
    connectionString: process.env.DATABASE_URL ?? 'postgres://user:password@localhost:5432/queue',
  });
  pool.on('error', (err) => console.error('Idle database client error:', err));

  const jobRepo = new JobRepository(pool);
  const queueRepo = new QueueRepository(pool);
  const jobService = new JobService(jobRepo, queueRepo);
  const queueService = new QueueService(queueRepo);

  const queue = await queueService.createQueue({
    name: 'emails',
    maxAttempts: 3,
    leaseDuration: 60_000,
    concurrency: 0,
  });

  for (const userId of [1, 2, 3]) {
    const job = await jobService.publishJob({
      queueId: queue.id,
      idempotencyKey: `welcome-email:${userId}`,
      payload: JSON.stringify({ userId }),
    });
    console.log(`published job ${job.id} (deduplicated: ${job.deduplicated})`);
  }

  const worker = new WorkerService(jobService, {
    queueId: queue.id,
    concurrency: 2,
    handler: async (job, payload: { userId: number }) => {
      console.log(`sending welcome email to user ${payload.userId} (attempt ${job.attempts})`);
      await new Promise((resolve) => setTimeout(resolve, 200));
    },
  });
  const reaper = new ReaperService(jobRepo);

  await worker.start();
  reaper.start();

  process.once('SIGINT', async () => {
    await worker.stop();
    await reaper.stop();
    await pool.end();
  });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
```

Run it, wait a second, then press Ctrl+C:

```text
published job 1 (deduplicated: false)
published job 2 (deduplicated: false)
published job 3 (deduplicated: false)
[Worker] Starting 2 slot(s) for queue 1
[reaper] Starting with interval 30000ms
sending welcome email to user 1 (attempt 1)
sending welcome email to user 2 (attempt 1)
sending welcome email to user 3 (attempt 1)
^C
[Worker] Stopped (queue 1)
[reaper] Stopped
```

Run it a second time and two things differ:

- A warning says that queue `emails` already exists. This is expected:
  `createQueue` returned the existing queue.
- The jobs are new (`deduplicated: false`) even though the keys are the same.
  The first run completed its jobs, and completed jobs are deleted, which
  frees their keys. Deduplication only covers jobs that are still queued or
  running.

## Next steps

- [Job lifecycle](../concepts/job-lifecycle.md): what happens on failure,
  retry and crash
- [Publishing jobs](../guides/publishing-jobs.md): delays, batches and
  publishing inside your own transaction
- [Running in production](../guides/production.md): pool sizing, logging and
  monitoring
