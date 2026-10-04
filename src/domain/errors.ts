export class TaskQueueError extends Error {
  
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

export class JobSnooze extends TaskQueueError {
  constructor(message: string = 'Job Snooze'){
    super(message);
    this.name = 'JobSnooze'
  }
}
