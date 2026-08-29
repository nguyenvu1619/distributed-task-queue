import { Pool } from 'pg';
import { Job, Metadata } from '../domain/job';
import { Executor } from '../domain/executor';
import { Logger } from '../domain/logger';
import { WorkerErrorEvent } from '../domain/worker';
import { DatabaseConfig } from '../repository/postgresql/connection';
import { Duration } from './duration';
import { Serializer } from './serializer';

export interface TaskQueueOptions extends DatabaseConfig {
  /**
   * Use an existing pool instead of building one. A borrowed pool is never
   * ended by `close()` — whoever created it owns its lifetime.
   */
  pool?: Pool;
  logger?: Logger;
  /** Where the .sql migration files live. Defaults to this package's own. */
  migrationsPath?: string;
  /**
   * Wake workers with Postgres LISTEN/NOTIFY the moment a job is published,
   * instead of leaving them to find it on their next poll. Default true.
   *
   * It costs one connection held for the whole process — budget for it in the
   * pool's `max`. Turn it off behind a connection pooler in transaction mode
   * (PgBouncer and friends), where a session-scoped LISTEN cannot work: jobs are
   * then delivered on the poll interval alone, which is the behaviour this
   * setting predates.
   */
  notify?: boolean;
}

export interface QueueConfig {
  /** Max jobs running at once across all workers. 0 (default) is unlimited. */
  concurrency?: number;
  /** Total tries per job, including the first. Default 3. */
  maxAttempts?: number;
  /** How long a pulled job stays leased before the reaper may reclaim it. Default 30s. */
  leaseDuration?: Duration;
  /** Reject jobs published without a group id. Default false. */
  requiresGroupId?: boolean;
  serializer?: Serializer;
}

export interface JobGroup {
  id: string;
  /**
   * Max jobs from this group running at once.
   *
   * Required, not optional: a group published without a concurrency gets no row
   * in `group_queue_limits`, and the pull's `running < max_running` check then
   * matches nothing — the job would never be delivered.
   */
  concurrency: number;
}

export interface PublishOptions {
  /**
   * Join a transaction the caller owns — a pg `PoolClient`, or anything with a
   * `query(text, values)`. The job commits with the caller's writes or not at
   * all, which is what removes the need for an outbox table.
   */
  tx?: Executor;
  /**
   * Publishing the same key twice returns the first job instead of inserting a
   * second. Defaults to a random UUID (i.e. no deduplication).
   *
   * The key is only held while the job is alive: terminal jobs are deleted,
   * which frees it.
   */
  idempotencyKey?: string;
  group?: JobGroup;
  metadata?: Metadata;
}

export interface JobContext {
  /** The raw job record — escape hatch for anything not surfaced here. */
  job: Job;
  id: number;
  /** 1-based; already counts the current run. */
  attempt: number;
  maxAttempts: number;
  queue: string;
  groupId: string | null;
  /** Aborted when the worker is stopping. */
  signal: AbortSignal;
  log: Logger;
}

export type TaskHandler<T> = (payload: T, ctx: JobContext) => Promise<void> | void;

export interface WorkOptions {
  /** Slots polling in parallel in this process. Default 1. */
  concurrency?: number;
  /** Wait between polls when the queue is empty. Default 1s. */
  pollInterval?: Duration;
  name?: string;
  logger?: Logger;
  onError?: (event: WorkerErrorEvent) => void;
  /** Set false to construct the worker without starting it. Default true. */
  autoStart?: boolean;
}

export interface ReaperOptions {
  /** Time between recovery passes. Default 30s. */
  interval?: Duration;
  /** Jobs reclaimed per pass. Default 100. */
  batchSize?: number;
  logger?: Logger;
}

export interface CloseOptions {
  /** How long to wait for in-flight handlers before closing anyway. */
  timeout?: Duration;
}
