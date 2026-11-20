import { JobRepository } from '../repository/postgresql/job.repository';
import { RecoveryResult } from '../domain/job';
import { Logger, consoleLogger, prefixed } from '../domain/logger';

export interface ReaperServiceOptions {
  interval?: number; // milliseconds between passes, default 30000 (30 seconds)
  batchSize?: number; // jobs per statement, default 100; a pass runs as many as the backlog needs
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

  /**
   * One pass: reclaims expired leases a batch of `batchSize` at a time until a
   * batch comes back short. Throughput follows the backlog while each
   * statement stays small — every batch commits on its own, so slots come back
   * batch by batch and no lock is held longer than one batch.
   */
  async runOnce(): Promise<RecoveryResult> {
    const pass: RecoveryResult = { recovered: [], discarded: [] };
    await this.drain(pass, () => true);
    return pass;
  }

  /**
   * Runs batches into `pass` until one comes back short or `keepGoing` says
   * stop. Short means drained, or that the rest is locked by another reaper,
   * which is draining it. Each batch lands in `pass` as it commits, so a pass
   * that fails part-way still accounts for the batches before the failure.
   */
  private async drain(pass: RecoveryResult, keepGoing: () => boolean): Promise<void> {
    for (;;) {
      const batch = await this.jobRepo.recoverJobs(this.batchSize);
      for (const id of batch.recovered) pass.recovered.push(id);
      for (const id of batch.discarded) pass.discarded.push(id);

      // Logged per batch rather than once per pass: a deletion is permanent,
      // and a later batch failing must not take its record down with it.
      if (batch.discarded.length > 0) {
        this.logger.warn(
          `Discarded ${batch.discarded.length} jobs out of attempts: ${batch.discarded.join(', ')}`
        );
      }

      // Discarded jobs count towards a full batch: they left PROCESSING too.
      // Counting only the recovered ones would end the pass at the first batch
      // with a deletion in it and leave the rest of the backlog to the next
      // interval.
      const handled = batch.recovered.length + batch.discarded.length;
      if (handled < this.batchSize || !keepGoing()) {
        return;
      }
    }
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

      const pass: RecoveryResult = { recovered: [], discarded: [] };
      try {
        // `running` is checked between batches, so stop() waits for the
        // statement in flight rather than for the whole backlog to drain.
        await this.drain(pass, () => this.running);
      } catch (error) {
        this.logger.error('Process error:', error);
      }
      // Silent when there is nothing to recover — that is the normal case,
      // and a heartbeat every interval is just noise in a log. Still reported
      // after a failure, for the batches that committed before it.
      if (pass.recovered.length > 0) {
        this.logger.info(`Recovered ${pass.recovered.length} jobs`);
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
