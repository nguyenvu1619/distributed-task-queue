import { JobService } from './job.service';
import {
  HandlerContext,
  JobHandler,
  JobWakeup,
  StopOptions,
  StopResult,
  WorkerErrorEvent,
  WorkerOptions,
} from '../domain/worker';
import { Job } from '../domain/job';
import { Queue } from '../domain/queue';
import { ErrorCodes, isTaskQueueError } from '../domain/errors';
import { Logger, consoleLogger, prefixed } from '../domain/logger';

export {
  WorkerOptions,
  JobHandler,
  JobWakeup,
  HandlerContext,
  StopOptions,
  StopResult,
  WorkerErrorEvent,
};

export class WorkerService {
  private readonly concurrency: number;
  private readonly pollInterval: number;
  private readonly wakeup: JobWakeup | undefined;
  private readonly logger: Logger;
  private readonly deserialize: (job: Job) => unknown;

  private running: boolean = false;
  private slots: Promise<void>[] = [];
  private startPromise: Promise<void> | null = null;
  private abort: AbortController = new AbortController();

  constructor(
    private readonly jobService: JobService,
    private readonly options: WorkerOptions,
  ) {
    this.concurrency = options.concurrency ?? 1;
    this.pollInterval = options.pollInterval ?? 1000;
    this.wakeup = options.wakeup;
    this.logger = prefixed(
      options.logger ?? consoleLogger,
      `[worker:${options.name ?? options.queueId}]`,
    );
    this.deserialize = options.deserialize ?? ((job: Job) => JSON.parse(job.payload));
  }

  /**
   * Fetches the queue once, then starts all concurrent slots sharing that
   * resolved object — this is what removes the per-call queue lookup from
   * pullJob / completeJob / failJob.
   *
   * The in-flight promise is stored before the first `await`, so two concurrent
   * calls cannot both get past the guard and orphan a set of slots.
   */
  async start(): Promise<void> {
    if (this.startPromise) {
      return this.startPromise;
    }

    this.startPromise = this.doStart();
    try {
      await this.startPromise;
    } catch (error) {
      this.startPromise = null;
      throw error;
    }
  }

  private async doStart(): Promise<void> {
    const queue = await this.jobService.getQueue(this.options.queueId);

    this.abort = new AbortController();
    this.running = true;
    this.logger.info(`Starting ${this.concurrency} slot(s) for queue ${queue.id}`);

    this.slots = Array.from({ length: this.concurrency }, (_, i) =>
      this.runSlot(i, queue).catch((error) => {
        // runSlot handles its own errors; this is a backstop so an unexpected
        // throw cannot surface as an unhandled rejection.
        this.report({ phase: 'pull', error, slot: i });
      }),
    );
  }

  /**
   * Signals every slot to stop and waits for in-flight handlers to finish.
   *
   * `timeout` bounds that wait: without it a single stuck handler blocks
   * shutdown for ever. The returned `drained` says whether the wait completed
   * or the deadline won.
   */
  async stop(options: StopOptions = {}): Promise<StopResult> {
    const starting = this.startPromise;
    if (!starting) {
      return { drained: true };
    }

    // Let an in-flight start finish first. doStart() sets running = true after
    // its queue lookup await, so clearing the flag ahead of that continuation
    // would leave the slots running with no way to stop them.
    await starting.catch(() => undefined);

    this.running = false;
    // Wakes any slot parked in its poll sleep and lets handlers watching
    // ctx.signal bail out, instead of burning a full pollInterval per slot.
    this.abort.abort();

    const slots = this.slots;
    let drained = true;

    if (options.timeout === undefined) {
      await Promise.all(slots);
    } else {
      let timer: NodeJS.Timeout | undefined;
      const deadline = new Promise<false>((resolve) => {
        timer = setTimeout(() => resolve(false), options.timeout);
        timer.unref?.();
      });
      drained = await Promise.race([Promise.all(slots).then(() => true), deadline]);
      if (timer) {
        clearTimeout(timer);
      }
    }

    this.slots = [];
    this.startPromise = null;
    this.logger.info(
      drained
        ? `Stopped (queue ${this.options.queueId})`
        : `Stopped with handlers still running after ${options.timeout}ms (queue ${this.options.queueId})`,
    );

    return { drained };
  }

  isRunning(): boolean {
    return this.running;
  }

  private async runSlot(slotIndex: number, queue: Queue): Promise<void> {
    while (this.running) {
      let job: Job | null;

      // Captured before the pull, not after it. A job published while the pull
      // is in flight resolves this exact promise, so the slot that just saw an
      // empty queue still wakes immediately instead of sitting out a poll
      // interval on a queue that has work in it.
      const announced = this.wakeup?.next();

      try {
        job = await this.jobService.pullJobDirect(queue);
      } catch (error) {
        this.report({ phase: 'pull', error, slot: slotIndex });
        // Deliberately not woken early here: if the pull is failing, an arrival
        // is no reason to believe the next one will succeed.
        await this.idle();
        continue;
      }

      if (!job) {
        await this.idle(announced);
        continue;
      }

      // Deserialization sits OUTSIDE the handler's try. A payload that cannot
      // be decoded fails identically on every redelivery, so treating it as a
      // handler error would burn the whole attempt budget one useless run at a
      // time. It is a poison message: discard it and say so.
      let payload: unknown;
      try {
        payload = this.deserialize(job);
      } catch (error) {
        this.report({ phase: 'deserialize', error, slot: slotIndex, job });
        await this.settle('discard', job, queue, slotIndex);
        continue;
      }

      const ctx: HandlerContext = {
        attempt: job.attempts,
        maxAttempts: queue.maxAttempts,
        queue,
        signal: this.abort.signal,
        log: this.logger,
      };

      let handlerFailed = false;
      try {
        await this.options.handler(job, payload, ctx);
      } catch (error) {
        handlerFailed = true;
        this.report({ phase: 'handler', error, slot: slotIndex, job });
      }

      if (handlerFailed) {
        await this.settle('fail', job, queue, slotIndex);
        continue;
      }

      // Recording success is NOT inside the handler's try. When it was, a
      // transient DB error while completing routed into failJob, which put an
      // already-successful job straight back to PENDING and re-ran it. Now the
      // lease is simply left to expire and the reaper decides.
      await this.settle('complete', job, queue, slotIndex);
    }
  }

  private async settle(
    outcome: 'complete' | 'fail' | 'discard',
    job: Job,
    queue: Queue,
    slotIndex: number,
  ): Promise<void> {
    try {
      const lockSeq = job.lockSeq!;
      if (outcome === 'complete') {
        await this.jobService.completeJobDirect(job.id, lockSeq, queue);
      } else if (outcome === 'fail') {
        await this.jobService.failJobDirect(job.id, lockSeq, queue);
      } else {
        await this.jobService.discardJobDirect(job.id, lockSeq, queue);
      }
    } catch (error) {
      // A lost lease is a routine fencing outcome, not an incident: the lease expired
      // and the reaper or another worker owns this job now and will drive it. It fires
      // once per slow job on any queue with a tight leaseDuration, so error level is
      // wrong. The onError hook still fires -- it is the only way a consumer can
      // observe that a zombie's settle was refused rather than silently applied.
      //
      // Safe to attribute to THIS job: the try above wraps only the settle calls, all
      // of which are made with job.id, so a LEASE_LOST raised by the handler settling
      // some other job cannot reach here.
      if (isTaskQueueError(error) && error.code === ErrorCodes.LEASE_LOST) {
        this.logger.debug(
          `Slot ${slotIndex} lost the lease on job ${job.id} during ${outcome}; abandoning it`,
        );
        this.notify({ phase: outcome, error, slot: slotIndex, job });
        return;
      }
      this.report({ phase: outcome, error, slot: slotIndex, job });
    }
  }

  private report(event: WorkerErrorEvent): void {
    const suffix = event.job ? ` (job ${event.job.id})` : '';
    this.logger.error(`Slot ${event.slot} ${event.phase} error${suffix}:`, event.error);
    this.notify(event);
  }

  /**
   * Hands an event to the caller's onError hook without logging it as an error.
   * Split out of report() so a routine outcome can still be observed: onError is
   * the only signal a consumer has that a settle was fenced off at all.
   */
  private notify(event: WorkerErrorEvent): void {
    if (!this.options.onError) {
      return;
    }
    try {
      this.options.onError(event);
    } catch (hookError) {
      this.logger.error('onError hook threw:', hookError);
    }
  }

  /**
   * The wait after an empty poll. Ends at whichever comes first: the poll
   * interval, a job announced on `announced`, or stop().
   *
   * The timer is always cleared, including when a notification wins the race —
   * a busy queue would otherwise leave one live timer per empty poll behind.
   */
  private idle(announced?: Promise<void>): Promise<void> {
    const signal = this.abort.signal;
    if (signal.aborted) {
      return Promise.resolve();
    }

    return new Promise<void>((resolve) => {
      let settled = false;
      const done = () => {
        if (settled) {
          return;
        }
        settled = true;
        clearTimeout(timer);
        signal.removeEventListener('abort', done);
        resolve();
      };
      const timer = setTimeout(done, this.pollInterval);
      signal.addEventListener('abort', done, { once: true });
      announced?.then(done, done);
    });
  }
}
