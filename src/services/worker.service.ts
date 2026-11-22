import { JobService } from './job.service';
import { WorkerOptions, JobHandler } from '../domain/worker';
import { Queue } from '../domain/queue';
import { Job } from '../domain/job';
import { JobSnooze, LeaseLostError, NonRetryable } from '../domain/errors';
import { retrySettle } from './settle-retry';

export { WorkerOptions, JobHandler };

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

// One line per retry: a stack trace for every attempt through an outage drowns
// the log. AggregateError (a refused dual-stack connect) has an empty message.
const summarize = (error: unknown): string =>
  error instanceof Error ? error.message || (error as { code?: string }).code || error.name : String(error);

export class WorkerService {
  private readonly concurrency: number;
  private readonly pollInterval: number;
  private running: boolean = false;
  private slots: Promise<void>[] = [];
  // Handed to every handler, and aborted by stop(). An aborted controller
  // cannot be reset, so each start() makes a new one.
  private stopping = new AbortController();

  constructor(
    private readonly jobService: JobService,
    private readonly options: WorkerOptions,
  ) {
    this.concurrency = options.concurrency ?? 1;
    this.pollInterval = options.pollInterval ?? 1000;
  }

  // Fetches the queue once, then starts all concurrent slots sharing that resolved object.
  // This eliminates per-call queue lookups inside pullJob / completeJob / failJob.
  async start(): Promise<void> {
    if (this.running) {
      return;
    }

    const queue = await this.jobService.getQueue(this.options.queueId);

    this.running = true;
    this.stopping = new AbortController();
    console.log(
      `[Worker] Starting ${this.concurrency} slot(s) for queue ${queue.id}`,
    );

    const signal = this.stopping.signal;
    this.slots = Array.from({ length: this.concurrency }, (_, i) => this.runSlot(i, queue, signal));
  }

  // Tells the handlers to stop, then waits for every slot to finish the job in
  // hand, including a settle still retrying through an outage — bounded by
  // settleRetry.deadlineMs. Aborting is all the worker can do: JS cannot stop
  // an async function from outside, so a handler that ignores the signal still
  // runs to its end, and stop() waits for it.
  async stop(): Promise<void> {
    this.running = false;
    this.stopping.abort();
    await Promise.all(this.slots);
    this.slots = [];
    console.log(`[Worker] Stopped (queue ${this.options.queueId})`);
  }

  isRunning(): boolean {
    return this.running;
  }

  private async pullSlot(queue: Queue): Promise<Job | null>{
    const job = await this.jobService.pullJobDirect(queue);
    return job
  }

  // Settles a job whose handler threw. `thrown` is unknown because a handler
  // can throw anything, not just Errors: a JobSignal picks the settle, and
  // everything else is an ordinary failure that retries within the budget.
  //
  // Except once stop() has aborted the signal: then the failure is most likely
  // the abort itself, a fetch or timer the worker cancelled, and charging the
  // job an attempt for the worker's own shutdown would let a deploy delete a
  // job on its last attempt. So it goes back due at once, attempt refunded. A
  // real error that lands during shutdown is refunded too and gets one more
  // run; that beats guessing which errors an abort produces. One line rather
  // than a stack: a real bug fails again, with its stack, on the next run.
  private async settleThrown(
    slotIndex: number,
    job: Job,
    queue: Queue,
    thrown: unknown,
    stopping: boolean,
  ): Promise<void> {
    if (thrown instanceof JobSnooze) {
      await this.settle(slotIndex, job, (timeoutMs) =>
        this.jobService.snoozeJobDirect(job.id, job.lockSeq, queue, thrown.retryAfterMs, timeoutMs)
      );
      return;
    }
    if (stopping && !(thrown instanceof NonRetryable)) {
      console.warn(
        `[Worker] Slot ${slotIndex} returning job ${job.id} to the queue, interrupted by stop(): ${summarize(thrown)}`
      );
      await this.settle(slotIndex, job, (timeoutMs) =>
        this.jobService.snoozeJobDirect(job.id, job.lockSeq, queue, 0, timeoutMs)
      );
      return;
    }
    console.error(`[Worker] Slot ${slotIndex} failed job ${job.id}:`, thrown);
    await this.settle(slotIndex, job, (timeoutMs) =>
      this.jobService.failJobDirect(job.id, job.lockSeq, queue, thrown instanceof NonRetryable, timeoutMs)
    );
  }

  // Settles a job whose payload is not JSON. Retrying cannot help, since every
  // attempt parses the same bytes, and NonRetryable cannot be thrown for it,
  // since the handler never runs. Only the worker can tell, so it fails the job
  // for good on its first pull instead of spending the whole budget on the same
  // SyntaxError. One line rather than a stack: the trace only points here.
  private async discardMalformed(slotIndex: number, job: Job, queue: Queue, parseError: unknown): Promise<void> {
    console.error(
      `[Worker] Slot ${slotIndex} discarding job ${job.id} (key ${job.idempotencyKey}): payload cannot be parsed: ${summarize(parseError)}`
    );
    await this.settle(slotIndex, job, (timeoutMs) =>
      this.jobService.failJobDirect(job.id, job.lockSeq, queue, true, timeoutMs)
    );
  }

  // Records a job's outcome — complete, fail or snooze — retrying through
  // transient failures. Dropping it on the first connection reset leaves the
  // job PROCESSING until its lease runs out, and the reaper then hands it to
  // another worker to run again: the blip becomes a duplicate run.
  //
  // Retries do not watch `running`. stop() waits for the slot, so a settle
  // under way at shutdown is carried through rather than dropped.
  private async settle(
    slotIndex: number,
    job: Job,
    attempt: (timeoutMs: number) => Promise<void>,
  ): Promise<void> {
    try {
      await retrySettle(attempt, this.options.settleRetry, (error, n, delayMs) => {
        console.warn(
          `[Worker] Slot ${slotIndex} could not settle job ${job.id} (attempt ${n}), retrying in ${delayMs}ms: ${summarize(error)}`,
        );
      });
    } catch (settleError) {
      if (settleError instanceof LeaseLostError) {
        // Either an earlier attempt committed and only its reply was lost, or
        // the lease ran out and another worker holds the job now. The slot
        // cannot tell which, and does not need to: both mean let go.
        console.warn(`[Worker] Slot ${slotIndex} no longer holds job ${job.id}; letting it go`);
        return;
      }
      console.error(`[Worker] Slot ${slotIndex} could not settle job ${job.id}:`, settleError);
    }
  }

  // `signal` is this run's, captured at start(): a restart makes a new one.
  private async runSlot(slotIndex: number, queue: Queue, signal: AbortSignal): Promise<void> {
    while (this.running) {
      try {
        const job = await this.jobService.pullJobDirect(queue);

        if (!job) {
          await sleep(this.pollInterval);
          continue;
        }

        // Parsed outside the handler's try, so a payload that can never parse
        // is not mistaken for a handler failure worth retrying.
        let payload: unknown;
        try {
          payload = JSON.parse(job.payload);
        } catch (parseError) {
          await this.discardMalformed(slotIndex, job, queue, parseError);
          continue;
        }

        // A pull still in flight when stop() is called still hands out its
        // job, so a handler can start with the signal already aborted.
        try {
          await this.options.handler(job, payload, signal);
        } catch (handlerError) {
          await this.settleThrown(slotIndex, job, queue, handlerError, signal.aborted);
          continue
        }

        await this.settle(slotIndex, job, (timeoutMs) =>
          this.jobService.completeJobDirect(job.id, job.lockSeq, queue, timeoutMs)
        );
      } catch (slotError) {
        console.error(`[Worker] Slot ${slotIndex} error:`, slotError);
        await sleep(this.pollInterval);
      }
    }
  }
}
