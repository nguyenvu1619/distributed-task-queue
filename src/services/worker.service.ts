import { JobService } from './job.service';
import { WorkerOptions, JobHandler } from '../domain/worker';
import { Queue } from '../domain/queue';
import { ErrorCodes, isTaskQueueError } from '../domain/errors';

export { WorkerOptions, JobHandler };

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Scoped to a specific job on purpose. A handler is free to call this library itself —
 * settling a child job, say — and a LEASE_LOST it raises is about THAT job, not this
 * slot's. Matching on the code alone would swallow a genuine handler failure and log
 * the wrong job id.
 */
const isLeaseLostFor = (error: unknown, jobId: number): boolean =>
  isTaskQueueError(error) &&
  error.code === ErrorCodes.LEASE_LOST &&
  error.context.jobId === jobId;

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

  async stop(): Promise<void> {
    this.running = false;
    await Promise.all(this.slots);
    this.slots = [];
    console.log(`[Worker] Stopped (queue ${this.options.queueId})`);
  }

  isRunning(): boolean {
    return this.running;
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
          await this.jobService.completeJobDirect(job.id, job.lockSeq!, queue);
        } catch (handlerError) {
          // A lost lease is a routine fencing outcome, not a job failure. The reaper
          // or another worker owns this job now and will drive it to completion, and
          // settling again is pointless: failJob tests the same predicate that just
          // refused us, so it would throw too and log a second, equally wrong line.
          if (isLeaseLostFor(handlerError, job.id)) {
            console.debug(
              `[Worker] Slot ${slotIndex} lost the lease on job ${job.id}; abandoning it`,
            );
            continue;
          }

          console.error(`[Worker] Slot ${slotIndex} failed job ${job.id}:`, handlerError);
          try {
            await this.jobService.failJobDirect(job.id, job.lockSeq!, queue);
          } catch (failError) {
            // Same race, reached from the other side: the handler genuinely failed and
            // the lease was gone by the time we tried to record that.
            if (isLeaseLostFor(failError, job.id)) {
              console.debug(
                `[Worker] Slot ${slotIndex} lost the lease on job ${job.id} before it could be failed`,
              );
              continue;
            }
            console.error(
              `[Worker] Slot ${slotIndex} could not fail job ${job.id}:`,
              failError,
            );
          }
        }
      } catch (slotError) {
        console.error(`[Worker] Slot ${slotIndex} error:`, slotError);
        await sleep(this.pollInterval);
      }
    }
  }
}
