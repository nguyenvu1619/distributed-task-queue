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

export interface WorkerOptions {
  queueId: number;
  concurrency?: number; // number of concurrent slots, default 1
  pollInterval?: number; // ms to wait when queue is empty, default 1000
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
