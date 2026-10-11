import { JobService } from './job.service';
import { WorkerOptions, JobHandler } from '../domain/worker';
import { Queue } from '../domain/queue';
import { Job } from '../domain/job';
import { JobSnooze, NonRetryable } from '../domain/errors';

export { WorkerOptions, JobHandler };

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

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

  private async pullSlot(queue: Queue): Promise<Job | null>{
    const job = await this.jobService.pullJobDirect(queue);
    return job
  }

  // Settles a job whose handler threw. `thrown` is unknown because a handler
  // can throw anything, not just Errors: a JobSignal picks the settle, and
  // everything else is an ordinary failure that retries within the budget.
  private async settleThrown(slotIndex: number, job: Job, queue: Queue, thrown: unknown): Promise<void> {
    if (thrown instanceof JobSnooze) {
      await this.jobService.snoozeJobDirect(job.id, job.lockSeq, queue, thrown.retryAfterMs);
      return;
    }
    console.error(`[Worker] Slot ${slotIndex} failed job ${job.id}:`, thrown);
    await this.jobService.failJobDirect(job.id, job.lockSeq, queue, thrown instanceof NonRetryable);
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
          try {
            await this.settleThrown(slotIndex, job, queue, handlerError);
          } catch (settleError) {
            console.error(
              `[Worker] Slot ${slotIndex} could not settle job ${job.id}:`,
              settleError,
            );
          }
          continue
        }
        // add retry here 
        try {
        await this.jobService.completeJobDirect(job.id, job.lockSeq, queue);
        } catch (completeError){
          // handle case lease lost error
          // it could be the job completed but the ack from DB is lost in the network, or the job already taken by another work
          // todo: identify this later
          console.error(`[Worker] Complete ${slotIndex} error:`, completeError)
        }
      } catch (slotError) {
        console.error(`[Worker] Slot ${slotIndex} error:`, slotError);
        await sleep(this.pollInterval);
      }
    }
  }
}
