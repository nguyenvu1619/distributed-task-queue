/**
 * TaskQueue facade lifecycle: constructing a client, declaring queues, and
 * shutting down. Publishing lives in producer.test.ts, workers in
 * consumer.test.ts, and transactional publish in transaction.test.ts.
 */
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { TaskQueue } from '../src/client/task-queue';
import { BadParamInputError, ConflictError, NotFoundError } from '../src/domain/errors';
import { Logger, silentLogger } from '../src/domain/logger';
import { parseDuration } from '../src/client/duration';
import { jsonSerializer } from '../src/client/serializer';
import { pgConfig } from './support/harness';
import { TaskQueueHarness, createTaskQueueHarness } from './support/task-queue';

let live: TaskQueueHarness | undefined;
let h!: TaskQueueHarness;

beforeEach(async () => {
  h = await createTaskQueueHarness();
  live = h;
});

// Snapshot and clear before closing: if a beforeEach throws part-way, closing the
// PREVIOUS test's already-ended harness would bury the real error under
// "Called end on pool more than once".
afterEach(async () => {
  const finished = live;
  live = undefined;
  await finished?.close();
});

describe('quickstart', () => {
  it('defines a queue, publishes, and runs the job to completion', async () => {
    const emails = h.tq.defineQueue<{ to: string }>(h.name('emails'));

    const published = await emails.publish({ to: 'a@b.c' });
    expect(published.deduplicated).toBe(false);
    expect(typeof published.id).toBe('number');

    const seen: Array<{ to: string }> = [];
    h.track(
      await emails.work(
        async (payload, ctx) => {
          expect(ctx.queue).toBe(emails.name);
          expect(ctx.id).toBe(published.id);
          expect(ctx.attempt).toBe(1);
          expect(ctx.maxAttempts).toBe(3);
          seen.push(payload);
        },
        { pollInterval: '10ms' }
      )
    );

    await h.waitForDrain(await emails.id());
    expect(seen).toEqual([{ to: 'a@b.c' }]);
  });
});

describe('defineQueue', () => {
  it('creates the queue lazily, on first use rather than on declaration', async () => {
    const name = h.name('lazy');
    h.tq.defineQueue(name);

    const before = await h.pool.query('SELECT 1 FROM queues WHERE name = $1', [name]);
    expect(before.rowCount).toBe(0);

    await h.tq.defineQueue(name).resolve();

    const after = await h.pool.query('SELECT 1 FROM queues WHERE name = $1', [name]);
    expect(after.rowCount).toBe(1);
  });

  it('returns the same handle for the same name', () => {
    const name = h.name('same');
    expect(h.tq.defineQueue(name)).toBe(h.tq.defineQueue(name));
  });

  it('refuses a second definition with a different configuration', () => {
    const name = h.name('conflict');
    h.tq.defineQueue(name, { concurrency: 4 });

    expect(() => h.tq.defineQueue(name, { concurrency: 8 })).toThrow(BadParamInputError);
    // Restating the same configuration is fine.
    expect(h.tq.defineQueue(name, { concurrency: 4 })).toBe(h.tq.defineQueue(name));
  });

  it('applies the documented defaults', async () => {
    const queue = await h.tq.defineQueue(h.name('defaults')).resolve();

    expect(queue.concurrency).toBe(0);
    expect(queue.maxAttempts).toBe(3);
    expect(queue.leaseDuration).toBe(30_000);
    expect(queue.requiresGroupId).toBe(false);
  });

  it('accepts durations as numbers or short strings', async () => {
    const q = h.tq.defineQueue(h.name('dur'), { leaseDuration: '45s', maxAttempts: 5 });
    const queue = await q.resolve();

    expect(queue.leaseDuration).toBe(45_000);
    expect(queue.maxAttempts).toBe(5);
  });

  it('rejects an unparseable duration', () => {
    expect(() => h.tq.defineQueue(h.name('bad-dur'), { leaseDuration: 'soon' })).toThrow(
      BadParamInputError
    );
  });

  it('provisions one shard row per shard when concurrency is capped', async () => {
    const q = h.tq.defineQueue(h.name('sharded'), { concurrency: 64 });
    const queue = await q.resolve();

    const { rows } = await h.pool.query(
      'SELECT count(*)::int AS n, sum(max_running)::int AS total FROM queue_shards WHERE queue_id = $1',
      [queue.id]
    );
    expect(rows[0].n).toBe(32);
    // The whole configured concurrency is distributed, remainder included.
    expect(rows[0].total).toBe(64);
  });

  it('creates no shards for an uncapped queue', async () => {
    const queue = await h.tq.defineQueue(h.name('uncapped'), { concurrency: 0 }).resolve();

    const { rows } = await h.pool.query(
      'SELECT count(*)::int AS n FROM queue_shards WHERE queue_id = $1',
      [queue.id]
    );
    expect(rows[0].n).toBe(0);
  });
});

describe('defineQueue is idempotent across clients', () => {
  it('lets two clients define the same queue concurrently', async () => {
    const other = TaskQueue.create({ pool: h.pool, logger: silentLogger });
    const name = h.name('shared');

    const [a, b] = await Promise.all([
      h.tq.defineQueue(name).resolve(),
      other.defineQueue(name).resolve(),
    ]);

    expect(a.id).toBe(b.id);
    const { rows } = await h.pool.query(
      'SELECT count(*)::int AS n FROM queues WHERE name = $1',
      [name]
    );
    expect(rows[0].n).toBe(1);
  });

  it('keeps the stored configuration when the queue already exists', async () => {
    const name = h.name('immutable');
    const first = await h.tq.defineQueue(name, { maxAttempts: 7 }).resolve();

    const other = TaskQueue.create({ pool: h.pool, logger: silentLogger });
    const second = await other.defineQueue(name, { maxAttempts: 2 }).resolve();

    expect(second.id).toBe(first.id);
    expect(second.maxAttempts).toBe(7);
  });

  it('warns rather than failing when the stored config differs', async () => {
    const name = h.name('drift');
    await h.tq.defineQueue(name, { maxAttempts: 7 }).resolve();

    const warnings: string[] = [];
    const recording: Logger = {
      ...silentLogger,
      warn: (message) => warnings.push(String(message)),
    };

    const other = TaskQueue.create({ pool: h.pool, logger: recording });
    await other.defineQueue(name, { maxAttempts: 2 }).resolve();

    expect(warnings.some((w) => w.includes('different configuration'))).toBe(true);
    expect(warnings.some((w) => w.includes('maxAttempts 7 != 2'))).toBe(true);
  });
});

describe('stats', () => {
  it('reports the live backlog', async () => {
    const q = h.tq.defineQueue<{ n: number }>(h.name('stats'));

    expect(await q.stats()).toEqual({ pending: 0, processing: 0 });

    await q.publishMany([{ n: 1 }, { n: 2 }, { n: 3 }]);
    expect(await q.stats()).toEqual({ pending: 3, processing: 0 });
  });
});

describe('migrate', () => {
  it('is safe to call when the schema is already current', async () => {
    await expect(h.tq.migrate()).resolves.toBeUndefined();
    await expect(h.tq.migrate()).resolves.toBeUndefined();

    const { rows } = await h.pool.query('SELECT count(*)::int AS n FROM schema_migrations');
    expect(rows[0].n).toBeGreaterThan(0);
  });
});

describe('close', () => {
  it('stops workers and reapers but leaves a borrowed pool open', async () => {
    const q = h.tq.defineQueue(h.name('close'));
    const worker = await q.work(async () => {}, { pollInterval: '10ms' });
    const reaper = h.tq.startReaper({ interval: '10s' });

    await h.tq.close({ timeout: '5s' });

    expect(worker.isRunning()).toBe(false);
    expect(reaper.isRunning()).toBe(false);
    // The caller owns a borrowed pool's lifetime.
    await expect(h.pool.query('SELECT 1')).resolves.toBeTruthy();
  });

  it('ends a pool it created itself', async () => {
    const owned = TaskQueue.create({ ...pgConfig({ max: 2 }), logger: silentLogger });
    await owned.defineQueue(h.name('owned')).resolve();

    await owned.close();

    await expect(owned.pool.query('SELECT 1')).rejects.toThrow();
  });

  it('is idempotent', async () => {
    const owned = TaskQueue.create({ ...pgConfig({ max: 2 }), logger: silentLogger });
    await owned.defineQueue(h.name('twice')).resolve();

    await owned.close();
    // A second close must not fail on ending an already-ended pool.
    await expect(owned.close()).resolves.toBeUndefined();
  });
});

describe('utilities', () => {
  it('parses every documented duration form', () => {
    expect(parseDuration(250)).toBe(250);
    expect(parseDuration('500ms')).toBe(500);
    expect(parseDuration('30s')).toBe(30_000);
    expect(parseDuration('5m')).toBe(300_000);
    expect(parseDuration('2h')).toBe(7_200_000);
    expect(parseDuration('1d')).toBe(86_400_000);
    expect(parseDuration('1.5s')).toBe(1_500);
  });

  it('rejects malformed durations', () => {
    for (const bad of ['', 'soon', '10 weeks', '-5s', 'ms']) {
      expect(() => parseDuration(bad), `"${bad}" should be rejected`).toThrow(
        BadParamInputError
      );
    }
    expect(() => parseDuration(-1)).toThrow(BadParamInputError);
    expect(() => parseDuration(NaN)).toThrow(BadParamInputError);
  });

  it('round-trips payloads through the default serializer', () => {
    const value = { a: 1, b: [true, null, 'x'], c: { d: 'e' } };
    expect(jsonSerializer.deserialize(jsonSerializer.serialize(value))).toEqual(value);
  });

  it('refuses a payload JSON cannot represent', () => {
    expect(() => jsonSerializer.serialize(undefined)).toThrow(BadParamInputError);
  });

  it('exposes the domain error types', () => {
    for (const error of [
      new ConflictError('x'),
      new NotFoundError('x'),
      new BadParamInputError('x'),
    ]) {
      expect(error).toBeInstanceOf(Error);
      expect(error.name).toBe(error.constructor.name);
    }
  });
});
