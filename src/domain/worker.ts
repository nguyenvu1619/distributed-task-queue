import { Job } from './job';
import { Queue } from './queue';
import { Logger } from './logger';

/**
 * Everything a handler may need beyond the job itself. Passed as a third
 * argument so existing two-argument handlers keep working unchanged.
 */
export interface HandlerContext {
  /** 1-based; already incremented for the current run. */
  attempt: number;
  maxAttempts: number;
  queue: Queue;
  /** Aborted when the worker is asked to stop — use it to bail out early. */
  signal: AbortSignal;
  log: Logger;
}

export type JobHandler<T = any> = (
  job: Job,
  payload: T,
  ctx: HandlerContext
) => Promise<void>;

/** Where in the slot loop something went wrong. */
export type WorkerErrorPhase =
  | 'pull'
  | 'deserialize'
  | 'handler'
  | 'complete'
  | 'fail'
  | 'discard';

export interface WorkerErrorEvent {
  phase: WorkerErrorPhase;
  error: unknown;
  slot: number;
  job?: Job;
}

/**
 * A wake-up source for the poll loop — in practice a Postgres LISTEN
 * subscription, but the worker only needs the promise.
 *
 * `next()` must be captured *before* the pull it guards. Capturing it after an
 * empty pull leaves a window in which an arrival is announced and lost, and the
 * slot then waits out a full poll interval on a queue that is not empty.
 */
export interface JobWakeup {
  /** Resolves when the queue may have work. Never rejects. */
  next(): Promise<void>;
}

export interface WorkerOptions {
  queueId: number;
  concurrency?: number; // number of concurrent slots, default 1
  pollInterval?: number; // ms to wait when queue is empty, default 1000
  /**
   * Cuts the wait on an empty queue short when a job is announced. The poll
   * interval stays as the floor, so delivery never depends on this.
   */
  wakeup?: JobWakeup;
  handler: JobHandler;
  /** Label used in log lines. Defaults to the queue id. */
  name?: string;
  logger?: Logger;
  /**
   * Turns the stored payload into whatever the handler expects.
   * Defaults to `JSON.parse`. Throwing here marks the job as poison.
   */
  deserialize?: (job: Job) => unknown;
  onError?: (event: WorkerErrorEvent) => void;
}

export interface StopOptions {
  /** Milliseconds to wait for in-flight handlers before giving up on the drain. */
  timeout?: number;
}

export interface StopResult {
  /** False when the timeout elapsed with handlers still running. */
  drained: boolean;
}
