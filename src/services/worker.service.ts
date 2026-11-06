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
    console.log(
      `[Worker] Starting ${this.concurrency} slot(s) for queue ${queue.id}`,
    );

    this.slots = Array.from({ length: this.concurrency }, (_, i) => this.runSlot(i, queue));
  }

  // Waits for every slot to finish the job in hand, including a settle still
  // retrying through an outage — bounded by settleRetry.deadlineMs.
  async stop(): Promise<void> {
    this.running = false;
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
  private async settleThrown(slotIndex: number, job: Job, queue: Queue, thrown: unknown): Promise<void> {
    if (thrown instanceof JobSnooze) {
      await this.settle(slotIndex, job, (timeoutMs) =>
        this.jobService.snoozeJobDirect(job.id, job.lockSeq, queue, thrown.retryAfterMs, timeoutMs)
      );
      return;
    }
    console.error(`[Worker] Slot ${slotIndex} failed job ${job.id}:`, thrown);
    await this.settle(slotIndex, job, (timeoutMs) =>
      this.jobService.failJobDirect(job.id, job.lockSeq, queue, thrown instanceof NonRetryable, timeoutMs)
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

  private async runSlot(slotIndex: number, queue: Queue): Promise<void> {
    while (this.running) {
      try {
        const job = await this.jobService.pullJobDirect(queue);

        if (!job) {
          await sleep(this.pollInterval);
          continue;
        }

        try {
          const payload = JSON.parse(job.payload);
          await this.options.handler(job, payload);
          
        } catch (handlerError) {
          await this.settleThrown(slotIndex, job, queue, handlerError);
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
