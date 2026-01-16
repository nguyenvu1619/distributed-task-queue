# Distributed Task Queue

A lightweight TypeScript library for distributed task queue management using PostgreSQL. This library provides a clean, simple API for job producers and workers to manage task queues with features like job leasing, concurrency control, and automatic job recovery.

## Features

- **Job Management**: Publish, pull, complete, and fail jobs
- **Queue Management**: Create and manage queues with configurable concurrency
- **Dual Strategy Performance**: Fast path (~3-5ms latency) for simple queues, full coordination path for complex scenarios
- **Job Leasing**: Automatic job locking with expiration to prevent duplicate processing
- **Job Recovery**: Reaper service to recover expired jobs
- **TypeScript**: Full TypeScript support with type safety
- **Lightweight**: Minimal dependencies (only `pg` and `dotenv`)
- **High Performance**: Optimized for low latency, approaching Graphile Worker's performance benchmarks

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
  requiresGroupId: false, // Use fast path for maximum performance (~3-5ms latency)
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
        await jobService.completeJob(job.id, job.lockSeq);
        console.log('Job completed:', job.id);
      } catch (error) {
        console.error('Job processing failed:', error);
        // Mark job as failed
        await jobService.failJob(job.id, job.lockSeq);
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

#### `completeJob(id: number, lockSeq: number): Promise<Job>`
Marks a job as completed. Requires the correct lock token.

#### `failJob(id: number, lockSeq: number): Promise<Job>`
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

## Performance Testing

The library includes comprehensive performance testing tools to benchmark throughput, latency, and concurrency handling.

### Quick Performance Test

Run the default test (10,000 jobs, 100 workers):

```bash
npm run perf:test
```

### Predefined Test Scenarios

```bash
# Small test (1,000 jobs, 10 workers)
npm run perf:small

# Medium test (5,000 jobs, 50 workers)
npm run perf:medium

# Large test (10,000 jobs, 100 workers)
npm run perf:large

# Extreme test (50,000 jobs, 200 workers)
npm run perf:extreme
```

### Custom Configuration

```bash
NUM_JOBS=10000 \
NUM_WORKERS=100 \
QUEUE_CONCURRENCY=1000 \
JOB_PROCESSING_TIME_MS=0 \
npm run perf:test
```

### Performance Metrics

The test measures:
- **Throughput**: Jobs processed per second
- **Latency**: P50, P95, P99 percentiles
- **Success Rate**: Percentage of completed jobs
- **Worker Distribution**: Load balancing across workers

For detailed performance testing documentation, see [examples/PERFORMANCE_TESTING.md](examples/PERFORMANCE_TESTING.md).

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
