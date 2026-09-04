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

#### `publishJob(input: CreateJobInput): Promise<Job>`
Publishes a new job to the queue.

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

## Error Handling

Every error this library throws is a `TaskQueueError` carrying a stable `code` and a
`retryable` flag. **Switch on `code`, not on the class** — `instanceof` silently returns
false when two copies of this package end up in one dependency tree, and the string
never does. `isTaskQueueError` is brand-based for the same reason.

| Code | Meaning | `retryable` | What to do |
|---|---|---|---|
| `LEASE_LOST` | You do not hold this job's lease | `false` | Abandon the job. Do **not** settle it again. |
| `JOB_NOT_FOUND` | No live job with that id | `false` | Nothing to act on — finished jobs are deleted. |
| `QUEUE_NOT_FOUND` | No queue with that id | `false` | Fix the configuration; the id is wrong or the queue was never created. |
| `PUBLISH_CONFLICT` | A publish could neither insert nor read back its key | `false` | Under REPEATABLE READ or stricter, retry the whole transaction; under READ COMMITTED an identical retry may succeed. |

```typescript
import { ErrorCodes, isTaskQueueError } from 'distributed-task-queue';

try {
  await jobService.completeJobDirect(job.id, job.lockSeq!, queue);
} catch (error) {
  if (isTaskQueueError(error) && error.code === ErrorCodes.LEASE_LOST) {
    // Routine crash fencing: the lease expired and someone else owns the job now.
    // It is not lost — the new owner or the reaper will drive it. Just move on.
    logger.debug('lost lease', error.context);
    return;
  }
  throw error;
}
```

### `retryable` means one specific thing

`retryable` says whether retrying the **identical call** could plausibly succeed. It does
not mean the failure was harmless. Every code above is `false`, because each describes a
state an identical retry cannot change — `LEASE_LOST` most of all: retrying a settle you
were just fenced out of would be a correctness violation, not a recovery.

When you pass your own `executor` to `publishJob`, the unit of retry is your whole
transaction, never the single call.

### Why a lost lease is not a "not found"

The settle predicate is `id = $1 AND lease_seq = $2 AND status = 'PROCESSING'`. A zero-row
result collapses several causes the database cannot separate: the lease expired and the
reaper reclaimed the job, another worker re-leased it, the job was already settled
(terminal jobs are deleted, leaving no tombstone), or the caller never held a lease at
all. They share the only fact a caller can act on — **this job is not yours** — so they
share one code. `JOB_NOT_FOUND` is reserved for a plain `getById` miss, where no lease
was ever presented.

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
- `errors.ts`: `TaskQueueError` base, the `ErrorCodes` registry, and the typed errors

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
