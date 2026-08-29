import { CreateJobInput, PublishedJob } from '../domain/job';
import { CreateQueueInput, Queue } from '../domain/queue';
import { BadParamInputError } from '../domain/errors';
import { Logger } from '../domain/logger';
import { StopResult, WorkerOptions } from '../domain/worker';
import { JobRepository } from '../repository/postgresql/job.repository';
import { PgNotifier, jobChannel } from '../repository/postgresql/notifier';
import { QueueRepository } from '../repository/postgresql/queue.repository';
import { JobService } from '../services/job.service';
import { WorkerService } from '../services/worker.service';
import { Duration, parseDuration } from './duration';
import { Serializer, jsonSerializer } from './serializer';
import {
  JobContext,
  PublishOptions,
  QueueConfig,
  TaskHandler,
  WorkOptions,
} from './options';

const DEFAULT_MAX_ATTEMPTS = 3;
const DEFAULT_LEASE_DURATION = 30_000;

/** A running worker. Stopping it never ends the pool — that is `TaskQueue.close()`. */
export interface Worker {
  readonly queue: string;
  start(): Promise<void>;
  stop(options?: { timeout?: Duration }): Promise<StopResult>;
  isRunning(): boolean;
}

/** Queue config with every default filled in and durations normalised to ms. */
export interface ResolvedQueueConfig {
  concurrency: number;
  maxAttempts: number;
  leaseDuration: number;
  requiresGroupId: boolean;
}

export interface QueueHandleDeps {
  jobRepo: JobRepository;
  queueRepo: QueueRepository;
  jobService: JobService;
  logger: Logger;
  /** Absent when the caller turned LISTEN/NOTIFY off — workers then only poll. */
  notifier?: PgNotifier | null;
  registerWorker(worker: Worker): void;
  newIdempotencyKey(): string;
}

/**
 * A typed reference to one queue.
 *
 * The underlying `Queue` row is resolved once, on first use, and reused for
 * every publish and every pull — which is exactly what the repository's
 * `*Direct` method variants existed to enable, without the caller having to
 * know about them.
 */
export class QueueHandle<T = unknown> {
  private resolved: Promise<Queue> | null = null;
  private readonly serializer: Serializer;
  readonly config: ResolvedQueueConfig;

  constructor(
    readonly name: string,
    config: QueueConfig,
    private readonly deps: QueueHandleDeps
  ) {
    this.serializer = config.serializer ?? jsonSerializer;
    this.config = {
      concurrency: config.concurrency ?? 0,
      maxAttempts: config.maxAttempts ?? DEFAULT_MAX_ATTEMPTS,
      leaseDuration: parseDuration(config.leaseDuration ?? DEFAULT_LEASE_DURATION),
      requiresGroupId: config.requiresGroupId ?? false,
    };
  }

  /** Creates the queue if it does not exist yet, then memoizes it. */
  async resolve(): Promise<Queue> {
    if (!this.resolved) {
      const input: CreateQueueInput = {
        name: this.name,
        maxAttempts: this.config.maxAttempts,
        leaseDuration: this.config.leaseDuration,
        concurrency: this.config.concurrency,
        requiresGroupId: this.config.requiresGroupId,
      };
      // Cleared on failure so a transient DB error does not poison the handle.
      this.resolved = this.deps.queueRepo.ensureQueue(input).catch((error) => {
        this.resolved = null;
        throw error;
      });
    }
    return this.resolved;
  }

  async id(): Promise<number> {
    return (await this.resolve()).id;
  }

  async publish(payload: T, options: PublishOptions = {}): Promise<PublishedJob> {
    const queue = await this.resolve();
    return this.deps.jobRepo.publishJob(this.toInput(queue, payload, options), options.tx);
  }

  /** Publishes a batch in one round trip. All-or-nothing within the batch. */
  async publishMany(payloads: T[], options: PublishOptions = {}): Promise<PublishedJob[]> {
    if (payloads.length === 0) {
      return [];
    }
    const queue = await this.resolve();
    const inputs = payloads.map((payload) => this.toInput(queue, payload, options));
    return this.deps.jobRepo.publishJobs(inputs, options.tx);
  }

  private toInput(queue: Queue, payload: T, options: PublishOptions): CreateJobInput {
    if (queue.requiresGroupId && !options.group) {
      throw new BadParamInputError(
        `Queue "${this.name}" requires a group id, but none was given`
      );
    }

    return {
      idempotencyKey: options.idempotencyKey ?? this.deps.newIdempotencyKey(),
      payload: this.serializer.serialize(payload),
      queueId: queue.id,
      metadata: options.metadata,
      group: options.group,
    };
  }

  /** Live backlog for this queue. Terminal jobs are deleted, so this is what is left. */
  async stats(): Promise<{ pending: number; processing: number }> {
    const queue = await this.resolve();
    return this.deps.jobRepo.countByStatus(queue.id);
  }

  /**
   * Starts polling this queue and running `handler` on each job.
   *
   * A handler that returns marks the job complete; one that throws marks it
   * failed and retried until `maxAttempts` is spent. A payload the serializer
   * cannot decode is treated as poison and discarded without consuming the
   * budget one run at a time.
   */
  async work(handler: TaskHandler<T>, options: WorkOptions = {}): Promise<Worker> {
    const queue = await this.resolve();

    // Held for the life of the handle rather than the life of a run: the worker
    // this returns can be stopped and started again, and re-subscribing on each
    // start would leave a restarted worker deaf until its first reconnect.
    // `TaskQueue.close()` closes the notifier and with it every watch.
    const wakeup = this.deps.notifier?.watch(jobChannel(queue.id));

    const workerOptions: WorkerOptions = {
      queueId: queue.id,
      concurrency: options.concurrency,
      pollInterval:
        options.pollInterval === undefined ? undefined : parseDuration(options.pollInterval),
      wakeup,
      name: options.name ?? this.name,
      logger: options.logger ?? this.deps.logger,
      onError: options.onError,
      deserialize: (job) => this.serializer.deserialize(job.payload),
      handler: async (job, payload, ctx) => {
        const jobContext: JobContext = {
          job,
          id: job.id,
          attempt: ctx.attempt,
          maxAttempts: ctx.maxAttempts,
          queue: this.name,
          groupId: job.groupId,
          signal: ctx.signal,
          log: ctx.log,
        };
        await handler(payload as T, jobContext);
      },
    };

    const service = new WorkerService(this.deps.jobService, workerOptions);

    const worker: Worker = {
      queue: this.name,
      start: () => service.start(),
      stop: (stopOptions = {}) =>
        service.stop({
          timeout:
            stopOptions.timeout === undefined ? undefined : parseDuration(stopOptions.timeout),
        }),
      isRunning: () => service.isRunning(),
    };

    this.deps.registerWorker(worker);

    if (options.autoStart !== false) {
      await service.start();
    }

    return worker;
  }
}
