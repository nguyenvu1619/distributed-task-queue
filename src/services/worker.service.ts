import { JobService } from './job.service';
import { WorkerOptions, JobHandler } from '../domain/worker';
import { Queue } from '../domain/queue';

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
          console.error(`[Worker] Slot ${slotIndex} failed job ${job.id}:`, handlerError);
          try {
            await this.jobService.failJobDirect(job.id, job.lockSeq!, queue);
          } catch (failError) {
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
