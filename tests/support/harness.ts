import { Pool } from 'pg';
import { inject } from 'vitest';

import { createPool, DatabaseConfig } from '../../src/repository/postgresql/connection';
import { JobRepository } from '../../src/repository/postgresql/job.repository';
import { QueueRepository } from '../../src/repository/postgresql/queue.repository';
import { JobService } from '../../src/services/job.service';
import { QueueService } from '../../src/services/queue.service';
import { ReaperService } from '../../src/services/reaper.service';
import { CreateQueueInput, Queue } from '../../src/domain/queue';
import { CreateJobInput, Job } from '../../src/domain/job';

export const ALL_TABLES = ['jobs', 'queue_shards', 'group_queue_limits', 'queues'] as const;

/** Connection details, from vitest's global setup or (in a child process) from env. */
export function pgConfig(overrides: Partial<DatabaseConfig> = {}): DatabaseConfig {
  let base: Omit<DatabaseConfig, 'max'>;
  try {
    base = inject('pg');
  } catch {
    base = {
      host: process.env.DATABASE_HOST || 'localhost',
      port: parseInt(process.env.DATABASE_PORT || '5432', 10),
      user: process.env.DATABASE_USER || 'user',
      password: process.env.DATABASE_PASS || 'password',
      database: process.env.DATABASE_NAME || 'queue',
    };
  }
  return { ...base, ...overrides };
}

export interface Harness {
  pool: Pool;
  jobRepo: JobRepository;
  queueRepo: QueueRepository;
  jobService: JobService;
  queueService: QueueService;
  reaper: ReaperService;
  close(): Promise<void>;
}

export interface HarnessOptions {
  /** Pool size. Concurrency suites need one session per racing worker. */
  maxConnections?: number;
}

export function createHarness(options: HarnessOptions = {}): Harness {
  const pool = createPool(
    pgConfig({
      max: options.maxConnections ?? 20,
      // Racing suites momentarily saturate the pool; give checkout room.
      connectionTimeoutMillis: 30_000,
      idleTimeoutMillis: 10_000,
    })
  );

  const jobRepo = new JobRepository(pool);
  const queueRepo = new QueueRepository(pool);

  return {
    pool,
    jobRepo,
    queueRepo,
    jobService: new JobService(jobRepo, queueRepo),
    queueService: new QueueService(queueRepo),
    reaper: new ReaperService(jobRepo, queueRepo),
    close: () => pool.end(),
  };
}

/**
 * Wipes every table. QueueRepository memoises queues in-process, so tests must
 * also build a fresh harness (or at least a fresh QueueRepository) per case —
 * a truncated queue would otherwise still be served from that cache.
 */
export async function resetDatabase(pool: Pool): Promise<void> {
  await pool.query(`TRUNCATE ${ALL_TABLES.join(', ')} RESTART IDENTITY CASCADE`);
}

let seq = 0;
export function uniqueName(prefix: string): string {
  seq += 1;
  return `${prefix}-${process.pid}-${seq}`;
}

// ---------------------------------------------------------------------------
// Fixture builders
// ---------------------------------------------------------------------------

export function queueInput(overrides: Partial<CreateQueueInput> = {}): CreateQueueInput {
  return {
    name: uniqueName('q'),
    maxAttempts: 3,
    leaseDuration: 30_000,
    concurrency: 0,
    requiresGroupId: false,
    ...overrides,
  };
}

export function jobInput(queueId: number, overrides: Partial<CreateJobInput> = {}): CreateJobInput {
  return {
    idempotencyKey: uniqueName('job'),
    payload: JSON.stringify({ hello: 'world' }),
    queueId,
    ...overrides,
  };
}

/** Publishes `count` jobs onto `queue`, optionally spreading them over groups. */
export async function publishJobs(
  harness: Harness,
  queue: Queue,
  count: number,
  opts: { groupIds?: string[]; groupConcurrency?: number } = {}
): Promise<Job[]> {
  const jobs: Job[] = [];
  for (let i = 0; i < count; i++) {
    const group = opts.groupIds
      ? { id: opts.groupIds[i % opts.groupIds.length], concurrency: opts.groupConcurrency ?? 1 }
      : undefined;

    jobs.push(
      await harness.jobRepo.publishJob(
        jobInput(queue.id, { payload: JSON.stringify({ n: i }), group })
      )
    );
  }
  return jobs;
}

// ---------------------------------------------------------------------------
// Timing helpers
// ---------------------------------------------------------------------------

export const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

/** Polls `predicate` until it returns true or the deadline passes. */
export async function waitFor(
  predicate: () => Promise<boolean> | boolean,
  { timeout = 15_000, interval = 50, message = 'condition' }: {
    timeout?: number;
    interval?: number;
    message?: string;
  } = {}
): Promise<void> {
  const deadline = Date.now() + timeout;
  let last: unknown;
  while (Date.now() < deadline) {
    try {
      if (await predicate()) return;
    } catch (err) {
      last = err;
    }
    await sleep(interval);
  }
  throw new Error(
    `Timed out after ${timeout}ms waiting for ${message}${last ? ` (last error: ${last})` : ''}`
  );
}
