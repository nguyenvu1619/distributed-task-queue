import { JobRepository } from '../repository/postgresql/job.repository';
import { Logger, consoleLogger, prefixed } from '../domain/logger';

export interface ReaperServiceOptions {
  interval?: number; // milliseconds, default 30000 (30 seconds)
  batchSize?: number; // default 100
  logger?: Logger;
}

export class ReaperService {
  private interval: number;
  private batchSize: number;
  private logger: Logger;
  private running: boolean = false;
  private timer: NodeJS.Timeout | null = null;
  /** The pass currently in flight, so stop() can wait for it to land. */
  private inFlight: Promise<void> | null = null;

  constructor(
    private jobRepo: JobRepository,
    options: ReaperServiceOptions = {}
  ) {
    this.interval = options.interval || 30000; // 30 seconds
    this.batchSize = options.batchSize || 100;
    this.logger = prefixed(options.logger ?? consoleLogger, '[reaper]');
  }

  async runOnce(): Promise<number[]> {
    return this.jobRepo.recoverJobs(this.batchSize);
  }

  start(): void {
    if (this.running) {
      return;
    }

    this.running = true;
    this.logger.info(`Starting with interval ${this.interval}ms`);

    const run = async (): Promise<void> => {
      if (!this.running) {
        return;
      }

      try {
        const ids = await this.runOnce();
        // Silent when there is nothing to recover — that is the normal case,
        // and a heartbeat every interval is just noise in a log.
        if (ids.length > 0) {
          this.logger.info(`Recovered ${ids.length} jobs`);
        }
      } catch (error) {
        this.logger.error('Process error:', error);
      }

      if (this.running) {
        this.timer = setTimeout(schedule, this.interval);
      }
    };

    const schedule = (): void => {
      this.inFlight = run();
    };

    // Start immediately
    schedule();
  }

  /**
   * Stops scheduling and waits for a pass already in flight, so the caller can
   * safely close the pool afterwards without cancelling a live transaction.
   */
  async stop(): Promise<void> {
    this.running = false;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    if (this.inFlight) {
      await this.inFlight;
      this.inFlight = null;
    }
    this.logger.info('Stopped');
  }

  isRunning(): boolean {
    return this.running;
  }
}
