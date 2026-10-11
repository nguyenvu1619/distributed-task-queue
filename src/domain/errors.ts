/** Base for errors the library raises to its callers. */
export class TaskQueueError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'TaskQueueError';
  }
}

export class QueueNotFoundError extends TaskQueueError {
  constructor(message: string = 'Queue not found') {
    super(message);
    this.name = 'QueueNotFoundError';
  }
}

export class JobNotFoundError extends TaskQueueError {
  constructor(message: string = 'Job not found') {
    super(message);
    this.name = 'JobNotFoundError';
  }
}

/**
 * A settle carried a `lease_seq` that no longer owns the job: the lease expired
 * and was reclaimed, the job was already settled, or it was never pulled. The
 * caller has been fenced off and must not assume its work was recorded.
 */
export class LeaseLostError extends TaskQueueError {
  constructor(message: string = 'Lease is no longer held') {
    super(message);
    this.name = 'LeaseLostError';
  }
}

/**
 * The queue was created by a racing transaction and dropped again before it
 * could be read back. Retryable.
 */
export class QueueCreateRaceError extends TaskQueueError {
  constructor(message: string = 'Queue was created and removed mid-create') {
    super(message);
    this.name = 'QueueCreateRaceError';
  }
}

/**
 * A publish statement returned no row for one of its inputs. Guards an
 * invariant rather than a race — there is no known way to reach it.
 */
export class PublishInvariantError extends TaskQueueError {
  constructor(message: string = 'Publish returned no row for an input') {
    super(message);
    this.name = 'PublishInvariantError';
  }
}

/**
 * Base for what a job handler throws to choose how its job is settled. These
 * are instructions to the worker, not failures of the queue, so they sit
 * outside TaskQueueError. Anything else a handler throws is an ordinary
 * failure and retries within the queue's attempt budget.
 */
export class JobSignal extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'JobSignal';
  }
}

/** Puts the job back to run again after `retryAfterMs`. */
export class JobSnooze extends JobSignal {
  constructor(public readonly retryAfterMs: number, message: string = `Job snoozed for ${retryAfterMs}ms`) {
    super(message);
    this.name = 'JobSnooze';
  }
}

/** Fails the job for good, skipping any attempts it has left. */
export class NonRetryable extends JobSignal {
  constructor(message: string = 'Job is not retryable') {
    super(message);
    this.name = 'NonRetryable';
  }
}
