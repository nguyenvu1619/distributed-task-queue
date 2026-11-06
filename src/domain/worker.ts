import { Job } from './job';

export type JobHandler<T = any> = (job: Job, payload: T) => Promise<void>;

export interface WorkerOptions {
  queueId: number;
  concurrency?: number;  // number of concurrent slots, default 1
  pollInterval?: number; // ms to wait when queue is empty, default 1000
  handler: JobHandler;
  settleRetry?: SettleRetryOptions;
}

/**
 * How long a worker keeps trying to record a job's outcome — complete, fail or
 * snooze — through transient database failures. A settle given up on leaves
 * the job PROCESSING until its lease runs out, and the reaper then hands it out
 * to run again.
 */
export interface SettleRetryOptions {
  /** No retry starts later than this many ms after the first attempt; 0 turns retries off. Default 30000. */
  deadlineMs?: number;
  /**
   * Abandons one attempt's statement after this many ms. Waiting for a pooled
   * connection is bounded by the pool's own `connectionTimeoutMillis` instead.
   * Default 5000.
   */
  attemptTimeoutMs?: number;
  /** Backoff ceiling for the first retry, doubling per retry up to `maxDelayMs`. Default 100. */
  baseDelayMs?: number;
  /** Default 2000. */
  maxDelayMs?: number;
}
