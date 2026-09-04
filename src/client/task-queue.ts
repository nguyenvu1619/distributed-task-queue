import { randomUUID } from 'crypto';
import * as path from 'path';
import { Pool, PoolClient } from 'pg';

import { PublishedJob } from '../domain/job';
import { InvalidInputError } from '../domain/errors';
import { Logger, consoleLogger } from '../domain/logger';
import { createPool } from '../repository/postgresql/connection';
import { JobRepository } from '../repository/postgresql/job.repository';
import { PgNotifier } from '../repository/postgresql/notifier';
import { QueueRepository } from '../repository/postgresql/queue.repository';
import { JobService } from '../services/job.service';
import { ReaperService } from '../services/reaper.service';
import { migrateUp } from '../migration/runner';
import { QueueHandle, Worker } from './queue-handle';
import { parseDuration } from './duration';
import {
  CloseOptions,
  PublishOptions,
  QueueConfig,
  ReaperOptions,
  TaskQueueOptions,
} from './options';

/** A running reaper. */
export interface Reaper {
  stop(): Promise<void>;
  isRunning(): boolean;
  /** Runs one recovery pass immediately and reports the reclaimed job ids. */
  runOnce(): Promise<number[]>;
}

/**
 * The entry point.
 *
 * ```ts
 * const tq = TaskQueue.create({ connectionString: process.env.DATABASE_URL });
 * const emails = tq.defineQueue<Email>('emails', { concurrency: 10 });
 *
 * await emails.publish({ to: 'a@b.c' });
 * await emails.work(async (email) => sendEmail(email), { concurrency: 4 });
 * ```
 */
export class TaskQueue {
  private readonly handles = new Map<string, QueueHandle<any>>();
  private readonly workers: Worker[] = [];
  private readonly reapers: ReaperService[] = [];
  private closed = false;

  private constructor(
    readonly pool: Pool,
    private readonly ownsPool: boolean,
    private readonly logger: Logger,
    private readonly migrationsPath: string,
    private readonly jobRepo: JobRepository,
    private readonly queueRepo: QueueRepository,
    private readonly jobService: JobService,
    private readonly notifier: PgNotifier | null
  ) {}

  /**
   * Builds a client. No connection is opened here — the first publish or worker
   * resolves its queue and, with it, the pool.
   */
  static create(options: TaskQueueOptions = {}): TaskQueue {
    const logger = options.logger ?? consoleLogger;
    const ownsPool = !options.pool;
    const pool = options.pool ?? createPool(options);

    const jobRepo = new JobRepository(pool, logger);
    const queueRepo = new QueueRepository(pool, logger);

    return new TaskQueue(
      pool,
      ownsPool,
      logger,
      // dist/client/… and src/client/… sit at the same depth, so one path works
      // for both the compiled package and ts-node.
      options.migrationsPath ?? path.join(__dirname, '..', '..', 'migrations'),
      jobRepo,
      queueRepo,
      new JobService(jobRepo, queueRepo),
      // Lazy: no connection is taken until a worker actually subscribes.
      options.notify === false ? null : new PgNotifier(pool, logger)
    );
  }

  /**
   * Declares a queue and returns a typed handle to it. Idempotent: the queue is
   * created on first use if it does not exist, and calling this again with the
   * same name returns the same handle.
   *
   * Queue configuration is immutable once created — a queue that already exists
   * with different settings keeps its stored settings, with a warning.
   */
  defineQueue<T = unknown>(name: string, config: QueueConfig = {}): QueueHandle<T> {
    const existing = this.handles.get(name);
    if (existing) {
      this.assertSameConfig(existing, name, config);
      return existing as QueueHandle<T>;
    }

    const handle = new QueueHandle<T>(name, config, {
      jobRepo: this.jobRepo,
      queueRepo: this.queueRepo,
      jobService: this.jobService,
      logger: this.logger,
      notifier: this.notifier,
      registerWorker: (worker) => this.workers.push(worker),
      newIdempotencyKey: () => randomUUID(),
    });

    this.handles.set(name, handle);
    return handle;
  }

  private assertSameConfig(
    existing: QueueHandle<any>,
    name: string,
    config: QueueConfig
  ): void {
    if (Object.keys(config).length === 0) {
      return;
    }

    const requested = new QueueHandle(name, config, {
      jobRepo: this.jobRepo,
      queueRepo: this.queueRepo,
      jobService: this.jobService,
      logger: this.logger,
      // Never worked, so it never watches a channel.
      notifier: null,
      registerWorker: () => {},
      newIdempotencyKey: () => '',
    }).config;

    const mismatched = (Object.keys(requested) as Array<keyof typeof requested>).filter(
      (key) => requested[key] !== existing.config[key]
    );

    if (mismatched.length > 0) {
      // Silently ignoring the second call would hand back a handle configured
      // differently from what this call asked for.
      throw new InvalidInputError(
        `Queue "${name}" was already defined with a different configuration ` +
          `(${mismatched.join(', ')}). Define each queue once and share the handle.`
      );
    }
  }

  /** Shorthand for `defineQueue(name).publish(payload, options)`. */
  async publish<T>(
    queueName: string,
    payload: T,
    options: PublishOptions = {}
  ): Promise<PublishedJob> {
    return this.defineQueue<T>(queueName).publish(payload, options);
  }

  /**
   * Runs `fn` inside a transaction and hands it the client. Publishes made with
   * `{ tx }` on that client commit together with whatever else `fn` wrote — the
   * point being that no outbox table is needed to keep the two in step.
   *
   * ```ts
   * await tq.transaction(async (tx) => {
   *   await tx.query('INSERT INTO orders (id) VALUES ($1)', [id]);
   *   await emails.publish({ orderId: id }, { tx });
   * });
   * ```
   */
  async transaction<R>(fn: (tx: PoolClient) => Promise<R>): Promise<R> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const result = await fn(client);
      await client.query('COMMIT');
      return result;
    } catch (error) {
      // A failing ROLLBACK must not mask the error that caused it.
      await client.query('ROLLBACK').catch((rollbackError) => {
        this.logger.error('Rollback failed', rollbackError);
      });
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * Starts the background recovery loop. Jobs whose lease expired — a worker
   * crashed, a process was killed — go back to PENDING; those out of attempts
   * are discarded. Run at least one per deployment.
   */
  startReaper(options: ReaperOptions = {}): Reaper {
    const service = new ReaperService(this.jobRepo, {
      interval: options.interval === undefined ? undefined : parseDuration(options.interval),
      batchSize: options.batchSize,
      logger: options.logger ?? this.logger,
    });

    this.reapers.push(service);
    service.start();

    return {
      stop: () => service.stop(),
      isRunning: () => service.isRunning(),
      runOnce: () => service.runOnce(),
    };
  }

  /** Applies any pending schema migrations. Safe to call on every boot. */
  async migrate(): Promise<void> {
    await migrateUp(this.pool, this.migrationsPath);
  }

  /**
   * Stops every worker and reaper this client started, then closes the pool if
   * this client created it. `timeout` bounds the wait for in-flight handlers.
   */
  async close(options: CloseOptions = {}): Promise<void> {
    if (this.closed) {
      return;
    }
    this.closed = true;

    const timeout =
      options.timeout === undefined ? undefined : parseDuration(options.timeout);

    const results = await Promise.all(this.workers.map((worker) => worker.stop({ timeout })));
    const stuck = results.filter((result) => !result.drained).length;
    if (stuck > 0) {
      this.logger.warn(`${stuck} worker(s) still had handlers running at close`);
    }

    await Promise.all(this.reapers.map((reaper) => reaper.stop()));

    this.workers.length = 0;
    this.reapers.length = 0;

    // Before the pool, always: the listener holds a checked-out client, and
    // `pool.end()` waits for every one of those to come back.
    await this.notifier?.close();

    if (this.ownsPool) {
      await this.pool.end();
    }
  }
}
