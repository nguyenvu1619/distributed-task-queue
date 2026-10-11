import { Pool } from 'pg';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { NUMBER_OF_SHARD } from '../src/domain/queue';
import { Harness, createHarness, jobInput, queueInput, resetDatabase } from './support/harness';
import { countByStatus, readJobRow, readShardCounters } from './support/invariants';

let h: Harness;

beforeEach(async () => {
  h = createHarness({ maxConnections: 10 });
  await resetDatabase(h.pool);
});

afterEach(async () => {
  await h.close();
});

/** Pull `available_at` into the past so a delayed job is due without waiting. */
async function makeDue(pool: Pool, id: number | string): Promise<void> {
  await pool.query(`UPDATE jobs SET available_at = now() - interval '1 second' WHERE id = $1`, [id]);
}

describe('delayed publish', () => {
  it('hides a delayed job from pull until the delay passes', async () => {
    const queue = await h.queueService.createQueue(queueInput({ concurrency: 0 }));
    const published = await h.jobRepo.publishJob(jobInput(queue.id, { delayMs: 60_000 }));

    expect(published.availableAt).not.toBeNull();
    expect((await readJobRow(h.pool, published.id)).deferred).toBe(true);
    expect(await h.jobRepo.pullJob(queue)).toBeNull();

    await makeDue(h.pool, published.id);
    expect((await h.jobRepo.pullJob(queue))?.id).toBe(published.id);
  });

  it('makes a job published without a delay available at once', async () => {
    const queue = await h.queueService.createQueue(queueInput({ concurrency: 0 }));
    const published = await h.jobRepo.publishJob(jobInput(queue.id));

    expect(published.availableAt).toBeNull();
    expect((await h.jobRepo.pullJob(queue))?.id).toBe(published.id);
  });

  it('treats a zero or negative delay as available at once', async () => {
    const queue = await h.queueService.createQueue(queueInput({ concurrency: 0 }));
    await h.jobRepo.publishJob(jobInput(queue.id, { delayMs: 0 }));
    await h.jobRepo.publishJob(jobInput(queue.id, { delayMs: -5_000 }));

    expect(await h.jobRepo.pullJob(queue)).not.toBeNull();
    expect(await h.jobRepo.pullJob(queue)).not.toBeNull();
  });

  it('applies each delay to its own job within a batch', async () => {
    const queue = await h.queueService.createQueue(queueInput({ concurrency: 0 }));
    const [first, delayed, last] = await h.jobRepo.publishJobs([
      jobInput(queue.id),
      jobInput(queue.id, { delayMs: 60_000 }),
      jobInput(queue.id),
    ]);

    const pulled = [await h.jobRepo.pullJob(queue), await h.jobRepo.pullJob(queue)];
    expect(pulled.map((j) => j?.id).sort((a, b) => a! - b!)).toEqual(
      [first.id, last.id].sort((a, b) => a - b)
    );
    expect(await h.jobRepo.pullJob(queue)).toBeNull();
    expect((await readJobRow(h.pool, delayed.id)).deferred).toBe(true);
  });

  it('keeps the existing schedule when a duplicate publish asks for a different delay', async () => {
    const queue = await h.queueService.createQueue(queueInput({ concurrency: 0 }));
    const input = jobInput(queue.id, { delayMs: 60_000 });
    const original = await h.jobRepo.publishJob(input);
    const duplicate = await h.jobRepo.publishJob({ ...input, delayMs: undefined });

    expect(duplicate.deduplicated).toBe(true);
    expect(duplicate.id).toBe(original.id);
    expect(duplicate.availableAt).toEqual(original.availableAt);
    expect(await h.jobRepo.pullJob(queue)).toBeNull();
  });

  it('rejects a non-finite delay without publishing any of the batch', async () => {
    const queue = await h.queueService.createQueue(queueInput({ concurrency: 0 }));

    await expect(
      h.jobRepo.publishJobs([jobInput(queue.id), jobInput(queue.id, { delayMs: NaN })])
    ).rejects.toBeInstanceOf(RangeError);
    expect((await countByStatus(h.pool, queue.id)).PENDING).toBe(0);
  });

  it('does not take a shard for a job that is not due yet', async () => {
    const queue = await h.queueService.createQueue(
      queueInput({ concurrency: NUMBER_OF_SHARD * 2 })
    );
    await h.jobRepo.publishJob(jobInput(queue.id, { delayMs: 60_000 }));

    expect(await h.jobRepo.pullJob(queue)).toBeNull();
    const shards = await readShardCounters(h.pool, queue.id);
    expect(shards.reduce((sum, s) => sum + s.running, 0)).toBe(0);
  });

  it('counts the delay from the publish statement, not from the start of the caller transaction', async () => {
    const queue = await h.queueService.createQueue(queueInput({ concurrency: 0 }));

    const tx = await h.pool.connect();
    let id: number;
    try {
      await tx.query('BEGIN');
      await tx.query('SELECT pg_sleep(0.3)');
      id = (await h.jobRepo.publishJob(jobInput(queue.id, { delayMs: 1_000 }), tx)).id;
      await tx.query('COMMIT');
    } finally {
      tx.release();
    }

    // created_at is now() — the transaction start — so the gap includes the
    // sleep. Counted from now(), it would be exactly the 1s delay.
    const { rows } = await h.pool.query(
      `SELECT EXTRACT(EPOCH FROM available_at - created_at) AS gap FROM jobs WHERE id = $1`,
      [id]
    );
    expect(Number(rows[0].gap)).toBeGreaterThanOrEqual(1.3);
  });
});
