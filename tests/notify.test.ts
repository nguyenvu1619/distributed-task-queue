/**
 * LISTEN/NOTIFY wake-ups.
 *
 * Every test here sets a poll interval far longer than the assertion's own
 * timeout, so a job that arrives at all can only have arrived because the
 * worker was woken — polling could not have found it in time. That is the whole
 * claim: notifications remove the up-to-one-poll-interval delay, and nothing
 * about delivery itself depends on them.
 */
import { Pool } from 'pg';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { TaskQueue } from '../src/client/task-queue';
import { silentLogger } from '../src/domain/logger';
import { jobChannel } from '../src/repository/postgresql/notifier';
import {
  TaskQueueHarness,
  collect,
  createTaskQueueHarness,
  sleep,
  waitFor,
} from './support/task-queue';

/** Longer than any wait in this file, by two orders of magnitude. */
const LONG_POLL = '120s';
/** Generous next to LONG_POLL, tight enough that only a wake-up can meet it. */
const WAKE_BUDGET = 5_000;

let live: TaskQueueHarness | undefined;
let h!: TaskQueueHarness;

beforeEach(async () => {
  live = undefined;
  h = await createTaskQueueHarness();
  live = h;
});

afterEach(async () => {
  const finished = live;
  live = undefined;
  await finished?.close();
});

/**
 * Waits until a session is actually parked on the queue's channel.
 *
 * `work()` returns before its LISTEN has round-tripped, and a job published in
 * that window is announced to nobody — the test would then be waiting out the
 * poll interval and failing on a race rather than on the behaviour. An idle
 * backend reports its last statement in `pg_stat_activity`, which is the one
 * observable that says the LISTEN has landed.
 */
async function waitForListener(pool: Pool, queueId: number): Promise<void> {
  const statement = `LISTEN "${jobChannel(queueId)}"`;
  await waitFor(
    async () => {
      const { rows } = await pool.query(
        `SELECT count(*)::int AS n FROM pg_stat_activity
         WHERE datname = current_database() AND query = $1`,
        [statement]
      );
      return rows[0].n > 0;
    },
    { timeout: 10_000, message: `a session listening on ${jobChannel(queueId)}` }
  );
}

describe('notify wake-ups', () => {
  it('delivers a job published while every slot is idle, without waiting for the poll', async () => {
    const queue = h.tq.defineQueue<{ n: number }>(h.name('notify'));
    const seen = collect<{ n: number }>();

    h.track(await queue.work(seen.handler, { pollInterval: LONG_POLL }));
    await waitForListener(h.pool, await queue.id());

    const published = Date.now();
    await queue.publish({ n: 1 });
    await seen.waitForCalls(1, { timeout: WAKE_BUDGET * 2 });

    expect(Date.now() - published).toBeLessThan(WAKE_BUDGET);
    expect(seen.seen).toEqual([{ n: 1 }]);
  });

  it('wakes on a batch publish', async () => {
    const queue = h.tq.defineQueue<{ n: number }>(h.name('notify-batch'));
    const seen = collect<{ n: number }>();

    h.track(await queue.work(seen.handler, { pollInterval: LONG_POLL }));
    await waitForListener(h.pool, await queue.id());

    const published = Date.now();
    await queue.publishMany([{ n: 1 }, { n: 2 }, { n: 3 }]);
    await seen.waitForCalls(3, { timeout: WAKE_BUDGET * 2 });

    expect(Date.now() - published).toBeLessThan(WAKE_BUDGET);
  });

  it('holds the announcement until the publisher commits', async () => {
    const queue = h.tq.defineQueue<{ id: string }>(h.name('notify-tx'));
    const seen = collect<{ id: string }>();

    h.track(await queue.work(seen.handler, { pollInterval: LONG_POLL }));
    await waitForListener(h.pool, await queue.id());

    let committed = 0;
    await h.tq.transaction(async (tx) => {
      await queue.publish({ id: 'a' }, { tx });
      // Still uncommitted: no puller can see the row, and NOTIFY is held with
      // it. Announcing at insert time instead would wake a worker that then
      // finds nothing.
      await sleep(300);
      expect(seen.calls).toBe(0);
      committed = Date.now();
    });

    await seen.waitForCalls(1, { timeout: WAKE_BUDGET * 2 });
    expect(Date.now() - committed).toBeLessThan(WAKE_BUDGET);
    expect(seen.seen).toEqual([{ id: 'a' }]);
  });

  it('says nothing when the publisher rolls back', async () => {
    const queue = h.tq.defineQueue<{ id: string }>(h.name('notify-rollback'));
    const seen = collect<{ id: string }>();

    h.track(await queue.work(seen.handler, { pollInterval: LONG_POLL }));
    await waitForListener(h.pool, await queue.id());

    await expect(
      h.tq.transaction(async (tx) => {
        await queue.publish({ id: 'gone' }, { tx });
        throw new Error('business rule said no');
      })
    ).rejects.toThrow('business rule said no');

    await sleep(1_000);
    expect(seen.calls).toBe(0);
    expect(await h.jobCount(await queue.id())).toBe(0);
  });

  it('wakes a worker in another client — the case polling exists for', async () => {
    const name = h.name('notify-cross');
    const publisher = h.tq.defineQueue<{ n: number }>(name);

    // A second TaskQueue over the same database: same shape as a worker
    // deployment that shares no memory with whoever publishes.
    const consumerSide = TaskQueue.create({ pool: h.pool, logger: silentLogger });
    const seen = collect<{ n: number }>();

    try {
      const consumer = consumerSide.defineQueue<{ n: number }>(name);
      await consumer.work(seen.handler, { pollInterval: LONG_POLL });
      await waitForListener(h.pool, await consumer.id());

      const published = Date.now();
      await publisher.publish({ n: 42 });
      await seen.waitForCalls(1, { timeout: WAKE_BUDGET * 2 });

      expect(Date.now() - published).toBeLessThan(WAKE_BUDGET);
      expect(seen.seen).toEqual([{ n: 42 }]);
    } finally {
      await consumerSide.close({ timeout: '5s' });
    }
  });

  it('still delivers by polling when notifications are turned off', async () => {
    const polling = TaskQueue.create({ pool: h.pool, logger: silentLogger, notify: false });
    const seen = collect<{ n: number }>();

    try {
      const queue = polling.defineQueue<{ n: number }>(h.name('no-notify'));
      await queue.work(seen.handler, { pollInterval: '200ms' });
      await queue.publish({ n: 7 });

      await seen.waitForCalls(1, { timeout: 10_000 });
      expect(seen.seen).toEqual([{ n: 7 }]);

      // And nothing was listening: the wake-up path is genuinely off, not just
      // unused.
      const { rows } = await h.pool.query(
        `SELECT count(*)::int AS n FROM pg_stat_activity
         WHERE datname = current_database() AND query = $1`,
        [`LISTEN "${jobChannel(await queue.id())}"`]
      );
      expect(rows[0].n).toBe(0);
    } finally {
      await polling.close({ timeout: '5s' });
    }
  });
});
