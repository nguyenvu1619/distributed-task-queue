/**
 * Every error this library throws is a `TaskQueueError` carrying a stable `code`
 * and a `retryable` flag.
 *
 * Switch on `code`, not on the class. `instanceof` silently returns false when two
 * copies of this package end up in one dependency tree; the string never does.
 * `isTaskQueueError` is brand-based for the same reason, so it survives duplication.
 */

/**
 * The complete set of codes this library can throw. Every one of them has a real
 * throw site — there is deliberately no code here that nothing raises.
 */
export const ErrorCodes = {
  /** You do not hold this job's lease. See {@link LeaseLostError}. */
  LEASE_LOST: 'LEASE_LOST',
  /** No live job with that id. See {@link JobNotFoundError}. */
  JOB_NOT_FOUND: 'JOB_NOT_FOUND',
  /** No queue with that id. See {@link QueueNotFoundError}. */
  QUEUE_NOT_FOUND: 'QUEUE_NOT_FOUND',
  /** A publish could neither insert nor read back its key. See {@link PublishConflictError}. */
  PUBLISH_CONFLICT: 'PUBLISH_CONFLICT',
} as const;

/**
 * A const object rather than a TS `enum`: an enum forces consumers to import it to
 * compare, and `const enum` breaks under `isolatedModules`. This way `err.code ===
 * 'LEASE_LOST'` type-checks against a bare string literal and still autocompletes.
 */
export type ErrorCode = (typeof ErrorCodes)[keyof typeof ErrorCodes];

/** The operation that failed, for logs that need to say which call it was. */
export type TaskQueueOperation = 'completeJob' | 'failJob' | 'discardJob' | 'publishJobs';

/** Structured detail attached to an error, for log aggregation rather than for parsing messages. */
export interface ErrorContext {
  operation?: TaskQueueOperation;
  jobId?: number;
  queueId?: number;
  /** The fence token the caller presented, when the failure was about a lease. */
  leaseSeq?: number | null;
  idempotencyKey?: string;
}

const TASK_QUEUE_ERROR_BRAND = Symbol.for('distributed-task-queue.TaskQueueError');

/** Base class for every error this library throws. */
export abstract class TaskQueueError extends Error {
  /** Stable, documented discriminator. Prefer this over `instanceof`. */
  readonly code: ErrorCode;

  /**
   * Whether retrying the *identical* call could plausibly succeed.
   *
   * It does not mean "this was harmless", and it is not a licence to loop: every
   * code in this library is currently `false`, because each one describes a state
   * an identical retry cannot change. A future `true` will mean the library owned
   * the transaction — when you supply your own `executor`, the unit of retry is
   * your whole transaction, never the single call.
   */
  readonly retryable: boolean;

  /** Structured detail; prefer reading this over parsing `message`. */
  readonly context: ErrorContext;

  protected constructor(
    code: ErrorCode,
    retryable: boolean,
    message: string,
    context: ErrorContext = {}
  ) {
    super(message);
    // new.target is the concrete subclass, so the name cannot drift from the class.
    this.name = new.target.name;
    this.code = code;
    this.retryable = retryable;
    this.context = context;
    // Branded in the constructor rather than as a declared field: a `unique symbol`
    // property would land in the public type, so an error from another copy of this
    // package would fail to type-check against this copy's TaskQueueError even though
    // Symbol.for gives them the same brand at runtime.
    (this as Record<symbol, unknown>)[TASK_QUEUE_ERROR_BRAND] = true;
  }

  /** Compact view for structured logs; picked up automatically by `JSON.stringify`. */
  toJSON(): Record<string, unknown> {
    return {
      name: this.name,
      code: this.code,
      retryable: this.retryable,
      message: this.message,
      context: this.context,
    };
  }
}

/**
 * Narrows an unknown caught value. Brand-based rather than `instanceof`, so it still
 * works when the thrower and the catcher loaded different copies of this package.
 */
export function isTaskQueueError(error: unknown): error is TaskQueueError {
  return (
    typeof error === 'object' &&
    error !== null &&
    (error as Record<symbol, unknown>)[TASK_QUEUE_ERROR_BRAND] === true
  );
}

/**
 * You do not hold this job's lease, so the settle was refused.
 *
 * The settle predicate is `id = $1 AND lease_seq = $2 AND status = 'PROCESSING'`, and
 * a zero-row result collapses several causes that the database cannot tell apart:
 * the lease expired and the reaper reclaimed the job, another worker re-leased it,
 * the job was already settled (terminal jobs are deleted, leaving no tombstone), or
 * the caller never held a lease on it at all. What they share is the only thing a
 * caller can act on: this job is not yours.
 *
 * **What to do:** stop working on it and discard the handler's result. Do *not* call
 * `failJob` — it tests the same predicate and will be refused identically. The job is
 * not lost: whoever owns it now, or the reaper, will drive it. This is a routine
 * outcome of crash fencing on any queue with a tight `leaseDuration`, so log it at
 * debug, not error.
 */
export class LeaseLostError extends TaskQueueError {
  constructor(
    jobId: number,
    leaseSeq: number | null,
    operation: 'completeJob' | 'failJob' | 'discardJob',
    context: ErrorContext = {}
  ) {
    super(
      ErrorCodes.LEASE_LOST,
      // Not retryable — but note the reason. It is *not* that the same (id, lockSeq)
      // could never match again: settling a never-leased job throws this while the row
      // sits PENDING with lease_seq NULL, and the next pull sets lease_seq to 1, so a
      // retry holding 1 would match. It is that a retry which *did* match would be a
      // correctness violation — settling a job whose lease you do not hold.
      false,
      `Lease lost for job ${jobId}: no PROCESSING row matches lease_seq ${String(leaseSeq)}. ` +
        `The lease expired, another worker owns the job, or it was already settled. Abandon it.`,
      { operation, jobId, leaseSeq, ...context }
    );
  }
}

/**
 * No live job with that id.
 *
 * This library keeps no history — completed and failed jobs are deleted — so "never
 * published", "rolled back before commit" and "ran to completion" are one state here
 * and cannot be told apart. Record terminal outcomes in your handler if you need them.
 */
export class JobNotFoundError extends TaskQueueError {
  constructor(jobId: number, context: ErrorContext = {}) {
    super(
      ErrorCodes.JOB_NOT_FOUND,
      false,
      `No live job with id ${jobId}. It was never published, or it reached a terminal ` +
        `state — this queue does not retain finished jobs.`,
      { jobId, ...context }
    );
  }
}

/**
 * No queue with that id.
 *
 * Kept separate from {@link JobNotFoundError} because the remedy differs: a missing
 * queue is a configuration or bootstrap fault, while a missing job is an ordinary
 * lifecycle outcome.
 */
export class QueueNotFoundError extends TaskQueueError {
  constructor(queueId: number, context: ErrorContext = {}) {
    super(ErrorCodes.QUEUE_NOT_FOUND, false, `No queue with id ${queueId}.`, {
      queueId,
      ...context,
    });
  }
}

/**
 * A publish could neither insert its idempotency key nor read back the row holding it.
 *
 * This is *not* the ordinary duplicate-publish path — that one dedups and returns the
 * live job with `deduplicated: true`. It means a concurrent publisher holds the key in
 * a state this transaction cannot see, or the conflicting job reached a terminal state
 * and was deleted in the window between the two statements.
 *
 * **What to do:** your transaction is still usable — this is a typed error, not an
 * aborted-transaction unique violation. Whether an identical retry can succeed depends
 * on your isolation level: under REPEATABLE READ or SERIALIZABLE the snapshot cannot
 * change, so it will fail forever and you must retry the whole transaction in a new
 * one. Under READ COMMITTED — the default, and what this library's own `BEGIN` opens —
 * a retry may succeed once the competing publisher commits.
 */
export class PublishConflictError extends TaskQueueError {
  constructor(idempotencyKey: string, context: ErrorContext = {}) {
    super(
      ErrorCodes.PUBLISH_CONFLICT,
      false,
      `Idempotency key "${idempotencyKey}" is held by a concurrent publish this ` +
        `statement cannot see. Under REPEATABLE READ or stricter, retry the whole ` +
        `transaction; under READ COMMITTED an identical retry may succeed.`,
      { operation: 'publishJobs', idempotencyKey, ...context }
    );
  }
}
