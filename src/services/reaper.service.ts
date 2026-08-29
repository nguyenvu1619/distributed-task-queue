import { JobRepository } from '../repository/postgresql/job.repository';
import { QueueRepository } from '../repository/postgresql/queue.repository';

export interface ReaperServiceOptions {
  interval?: number; // milliseconds, default 30000 (30 seconds)
  batchSize?: number; // default 100
}

export class ReaperService {
  private interval: number;
  private batchSize: number;
  private running: boolean = false;
  private timer: NodeJS.Timeout | null = null;

  constructor(
    private jobRepo: JobRepository,
    private queueRepo: QueueRepository,
    options: ReaperServiceOptions = {}
  ) {
    this.interval = options.interval || 30000; // 30 seconds
    this.batchSize = options.batchSize || 100;
  }


  async runOnce(): Promise<number[]> {
    return this.jobRepo.recoverJobs(this.batchSize);
  }

  start(): void {
    if (this.running) {
      return;
    }

    this.running = true;
    console.log(`[Reaper] Starting with interval ${this.interval}ms`);

    const run = async () => {
      if (!this.running) {
        return;
      }

      try {
        const ids = await this.runOnce();
        console.log(`[Reaper] Recovered ${ids.length} jobs`);
      } catch (error) {
        console.error('[Reaper] Process error:', error);
      }

      if (this.running) {
        this.timer = setTimeout(run, this.interval);
      }
    };

    // Start immediately
    run();
  }

  stop(): void {
    this.running = false;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    console.log('[Reaper] Stopped');
  }

  isRunning(): boolean {
    return this.running;
  }
}

