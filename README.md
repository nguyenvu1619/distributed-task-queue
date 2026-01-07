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
  max_attempts: 3,
  lease_duration: 30000, // 30 seconds in milliseconds
  concurrency: 10, // 10 concurrent workers
});

// Publish a job
const job = await jobService.publishJob({
  idempotency_key: 'unique-job-key-123',
  payload: JSON.stringify({ task: 'process-data', data: { id: 1 } }),
  queue_id: queue.id,
  group_id: 'group-123',
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

const queueService = new QueueService(new QueueRepository(pool));
const jobService = new JobService(new JobRepository(pool), queueService);

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
        await jobService.completeJob(job.id, job.lock_token);
        console.log('Job completed:', job.id);
      } catch (error) {
        console.error('Job processing failed:', error);
        // Mark job as failed
        await jobService.failJob(job.id, job.lock_token);
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
