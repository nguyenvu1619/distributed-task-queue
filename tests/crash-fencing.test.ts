import { Pool } from 'pg';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { JobStatus } from '../src/domain/job';
import { NUMBER_OF_SHARD, Queue } from '../src/domain/queue';
import { LeaseLostError } from '../src/domain/errors';
import { silentLogger } from '../src/domain/logger';
import { ReaperService } from '../src/services/reaper.service';
import {
  Harness,
  createHarness,
  jobInput,
  queueInput,
  resetDatabase,
  sleep,
  waitFor,
} from './support/harness';
import { readJobRow, readShardCounters } from './support/invariants';
import { spawnCrashWorker } from './support/crash-process';

let h: Harness;
const spawned: Array<{ kill(): Promise<void> }> = [];

beforeEach(async () => {
  h = createHarness({ maxConnections: 10 });
  await resetDatabase(h.pool);
});

afterEach(async () => {
  await Promise.all(spawned.splice(0).map((w) => w.kill()));
  await h.close();
});

/** Force a lease into the past without waiting for wall-clock time. */
async function expireLease(pool: Pool, id: number | string): Promise<void> {
  await pool.query(`UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1`, [
    id,
  ]);
}

/** Leases `count` jobs on `queue`, then forces every lease in the table into the past. */
async function strandJobs(queue: Queue, count: number): Promise<number[]> {
  const ids: number[] = [];
  for (let i = 0; i < count; i++) {
    ids.push((await h.jobRepo.publishJob(jobInput(queue.id))).id);
    await h.jobRepo.pullJob(queue);
  }
  await h.pool.query(`UPDATE jobs SET lease_expires_at = now() - interval '1 second'`);
  return ids;
}

const byId = (a: number, b: number): number => a - b;

async function fastQueue(overrides = {}): Promise<Queue> {
  return h.queueService.createQueue(
    queueInput({ concurrency: 0, leaseDuration: 2_000, ...overrides })
  );
}

async function coordinatedQueue(overrides = {}): Promise<Queue> {
  return h.queueService.createQueue(
    queueInput({ concurrency: NUMBER_OF_SHARD * 2, leaseDuration: 2_000, ...overrides })
  );
}

describe('crash recovery — a worker killed mid-job', () => {
  it('leaves the job PROCESSING with a live lease when the worker dies', async () => {
    const queue = await fastQueue({ leaseDuration: 60_000 });
    await h.jobRepo.publishJob(jobInput(queue.id));

    const worker = spawnCrashWorker(queue.id);
    spawned.push(worker);
    const leased = await worker.pulled;
    await worker.kill();

    const row = await readJobRow(h.pool, leased.id);
    expect(row).not.toBeNull();
    expect(row.status).toBe(JobStatus.PROCESSING);
    expect(new Date(row.lease_expires_at).getTime()).toBeGreaterThan(Date.now());
  });

  it('does not reclaim a crashed job while its lease is still valid', async () => {
    const queue = await fastQueue({ leaseDuration: 60_000 });
    await h.jobRepo.publishJob(jobInput(queue.id));

    const worker = spawnCrashWorker(queue.id);
    spawned.push(worker);
    await worker.pulled;
    await worker.kill();

    expect(await h.reaper.runOnce()).toEqual({ recovered: [], discarded: [] });
    expect((await readJobRow(h.pool, (await h.jobRepo.pullJobs(JobStatus.PROCESSING, 1))[0].id)).status)
      .toBe(JobStatus.PROCESSING);
  });

  it('reclaims the job once the lease expires and re-delivers it', async () => {
    const queue = await fastQueue({ leaseDuration: 2_000 });
    const published = await h.jobRepo.publishJob(jobInput(queue.id));

    const worker = spawnCrashWorker(queue.id);
    spawned.push(worker);
    const leased = await worker.pulled;
    expect(leased.id).toBe(String(published.id));
    await worker.kill();

    // Real wall-clock wait: this is the lease semantics under test.
    await waitFor(
      async () => {
        const row = await readJobRow(h.pool, published.id);
        return new Date(row.lease_expires_at).getTime() <= Date.now();
      },
      { timeout: 10_000, message: 'lease to expire' }
    );

    const { recovered } = await h.reaper.runOnce();
    expect(recovered.map(String)).toContain(String(published.id));

    const row = await readJobRow(h.pool, published.id);
    expect(row.status).toBe(JobStatus.PENDING);

    // A healthy worker must be able to take it over.
    const retaken = await h.jobRepo.pullJob(queue);
    expect(retaken).not.toBeNull();
    expect(String(retaken!.id)).toBe(String(published.id));
  });

  it('rejects the zombie when the killed worker comes back and settles', async () => {
    const queue = await fastQueue({ leaseDuration: 2_000 });
    const published = await h.jobRepo.publishJob(jobInput(queue.id));

    const worker = spawnCrashWorker(queue.id);
    spawned.push(worker);
    const zombie = await worker.pulled;
    await worker.kill();

    await sleep(2_200);
    await h.reaper.runOnce();

    const retaken = await h.jobRepo.pullJob(queue);
    expect(retaken, 'job was not re-delivered after recovery').not.toBeNull();

    // The zombie still believes it owns the job. Its lease_seq is stale and the
    // settle must be refused, or the new owner's work is silently discarded.
    await expect(
      h.jobRepo.completeJob(published.id, zombie.lockSeq as number, queue),
      'a settle carrying the crashed worker\'s lease_seq was accepted'
    ).rejects.toBeInstanceOf(LeaseLostError);

    // The rightful owner can still settle.
    await h.jobRepo.completeJob(retaken!.id, retaken!.lockSeq, queue);
  });
});

describe('lease fencing (deterministic — lease forced to expire)', () => {
  it('issues a lease_seq that differs from the reclaimed one (fast path)', async () => {
    const queue = await fastQueue();
    const published = await h.jobRepo.publishJob(jobInput(queue.id));

    const first = await h.jobRepo.pullJob(queue);
    await expireLease(h.pool, published.id);
    await h.reaper.runOnce();

    const second = await h.jobRepo.pullJob(queue);
    expect(second).not.toBeNull();
    expect(
      String(second!.lockSeq),
      'the re-issued lease reuses the fence token of the lease it replaced'
    ).not.toBe(String(first!.lockSeq));
  });

  it('advances lease_seq by exactly one on each re-lease', async () => {
    // A generous budget: this test is about the fence token, not the retry cap.
    const queue = await fastQueue({ maxAttempts: 10 });
    const published = await h.jobRepo.publishJob(jobInput(queue.id));

    const seen: number[] = [];
    for (let i = 0; i < 4; i++) {
      const pulled = await h.jobRepo.pullJob(queue);
      expect(pulled, `re-lease ${i} produced no job`).not.toBeNull();
      seen.push(Number(pulled!.lockSeq));
      await expireLease(h.pool, published.id);
      await h.reaper.runOnce();
    }

    expect(seen).toEqual([1, 2, 3, 4]);
  });

  it('rejects a settle from a fenced-off worker (fast path)', async () => {
    const queue = await fastQueue();
    const published = await h.jobRepo.publishJob(jobInput(queue.id));

    const stale = await h.jobRepo.pullJob(queue);
    await expireLease(h.pool, published.id);
    await h.reaper.runOnce();
    const current = await h.jobRepo.pullJob(queue);
    expect(current).not.toBeNull();

    await expect(
      h.jobRepo.failJob(published.id, stale!.lockSeq, queue)
    ).rejects.toBeInstanceOf(LeaseLostError);

    // The job is still owned by the current lease holder, untouched.
    const row = await readJobRow(h.pool, published.id);
    expect(row.status).toBe(JobStatus.PROCESSING);
    expect(String(row.lease_seq)).toBe(String(current!.lockSeq));
  });

  it('rejects a settle from a fenced-off worker (coordination path)', async () => {
    const queue = await coordinatedQueue();
    const published = await h.jobRepo.publishJob(jobInput(queue.id));

    const stale = await h.jobRepo.pullJob(queue);
    expect(stale).not.toBeNull();
    await expireLease(h.pool, published.id);
    await h.reaper.runOnce();

    const current = await h.jobRepo.pullJob(queue);
    expect(current, 'reaper did not return the coordinated job to the queue').not.toBeNull();

    await expect(
      h.jobRepo.completeJob(published.id, stale!.lockSeq, queue)
    ).rejects.toBeInstanceOf(LeaseLostError);
  });
});

describe('reaper', () => {
  it('is a no-op when nothing has expired', async () => {
    const queue = await fastQueue({ leaseDuration: 60_000 });
    await h.jobRepo.publishJob(jobInput(queue.id));
    await h.jobRepo.pullJob(queue);

    expect(await h.reaper.runOnce()).toEqual({ recovered: [], discarded: [] });
  });

  it('reclaims several expired jobs in one pass', async () => {
    const queue = await fastQueue();
    const ids: string[] = [];
    for (let i = 0; i < 5; i++) {
      const job = await h.jobRepo.publishJob(jobInput(queue.id));
      ids.push(String(job.id));
      await h.jobRepo.pullJob(queue);
    }
    await h.pool.query(`UPDATE jobs SET lease_expires_at = now() - interval '1 second'`);

    const recovered = (await h.reaper.runOnce()).recovered.map(String);
    expect(recovered.sort()).toEqual(ids.sort());

    const { rows } = await h.pool.query(
      `SELECT count(*)::int AS n FROM jobs WHERE status = 'PENDING' AND lease_expires_at IS NULL`
    );
    expect(rows[0].n).toBe(5);
  });

  it('returns an expired coordinated job to PENDING', async () => {
    const queue = await coordinatedQueue();
    const published = await h.jobRepo.publishJob(jobInput(queue.id));
    await h.jobRepo.pullJob(queue);
    await expireLease(h.pool, published.id);

    const recovered = (await h.reaper.runOnce()).recovered.map(String);
    expect(recovered, 'coordinated jobs are never reclaimed').toContain(String(published.id));

    const row = await readJobRow(h.pool, published.id);
    expect(row.status).toBe(JobStatus.PENDING);
  });

  it('releases the shard slot held by a reclaimed job', async () => {
    const queue = await coordinatedQueue();
    const published = await h.jobRepo.publishJob(jobInput(queue.id));
    await h.jobRepo.pullJob(queue);
    await expireLease(h.pool, published.id);
    await h.reaper.runOnce();

    const shards = await readShardCounters(h.pool, queue.id);
    expect(shards.reduce((sum, s) => sum + s.running, 0)).toBe(0);
  });

  it('does not release the same slot twice when run repeatedly', async () => {
    const queue = await coordinatedQueue();
    const published = await h.jobRepo.publishJob(jobInput(queue.id));
    await h.jobRepo.pullJob(queue);
    await expireLease(h.pool, published.id);

    for (let i = 0; i < 3; i++) await h.reaper.runOnce();

    const shards = await readShardCounters(h.pool, queue.id);
    const negative = shards.filter((s) => s.running < 0);
    expect(negative, 'repeated reaper passes drove a shard counter negative').toEqual([]);
  });

  it('gives up on a job that burns through its attempts by crashing', async () => {
    const maxAttempts = 2;
    const queue = await fastQueue({ maxAttempts });
    const published = await h.jobRepo.publishJob(jobInput(queue.id));

    for (let i = 0; i < maxAttempts; i++) {
      const pulled = await h.jobRepo.pullJob(queue);
      expect(pulled, `attempt ${i + 1} was not offered`).not.toBeNull();
      await expireLease(h.pool, published.id);
      await h.reaper.runOnce();
    }

    // A job that kills its worker every time must not be reclaimed for ever.
    expect(await readJobRow(h.pool, published.id)).toBeNull();
    expect(await h.jobRepo.pullJob(queue)).toBeNull();
  });

  it('releases the coordination slot of a job it gives up on', async () => {
    const queue = await coordinatedQueue({ maxAttempts: 1 });
    const published = await h.jobRepo.publishJob(jobInput(queue.id));

    await h.jobRepo.pullJob(queue);
    await expireLease(h.pool, published.id);
    // A pass that destroyed a job must not look like a pass that found nothing.
    expect(await h.reaper.runOnce()).toEqual({ recovered: [], discarded: [published.id] });

    expect(await readJobRow(h.pool, published.id)).toBeNull();
    const shards = await readShardCounters(h.pool, queue.id);
    expect(shards.reduce((sum, s) => sum + s.running, 0)).toBe(0);
  });

  it('leaves jobs on other queues alone', async () => {
    const victim = await fastQueue();
    const bystander = await fastQueue({ leaseDuration: 60_000 });

    const victimJob = await h.jobRepo.publishJob(jobInput(victim.id));
    const bystanderJob = await h.jobRepo.publishJob(jobInput(bystander.id));
    await h.jobRepo.pullJob(victim);
    await h.jobRepo.pullJob(bystander);
    await expireLease(h.pool, victimJob.id);

    await h.reaper.runOnce();

    expect((await readJobRow(h.pool, bystanderJob.id)).status).toBe(JobStatus.PROCESSING);
    expect((await readJobRow(h.pool, victimJob.id)).status).toBe(JobStatus.PENDING);
  });
});

describe('reaper — a backlog larger than one batch', () => {
  const reaperWith = (options: { batchSize: number; interval?: number }): ReaperService =>
    new ReaperService(h.jobRepo, { logger: silentLogger, ...options });

  it('drains the whole backlog in one pass, a batch at a time', async () => {
    const queue = await fastQueue();
    const ids = await strandJobs(queue, 5);

    const { recovered, discarded } = await reaperWith({ batchSize: 2 }).runOnce();

    expect(recovered.sort(byId)).toEqual(ids.sort(byId));
    expect(discarded).toEqual([]);
    expect(await h.jobRepo.countByStatus(queue.id)).toEqual({ pending: 5, processing: 0 });
  });

  it('keeps draining when a whole batch is discarded', async () => {
    // Every job is on its last attempt, so each batch deletes and returns
    // nothing to the queue. Counting only the returned jobs would read the
    // first batch as short and stop with three jobs still stranded.
    const queue = await fastQueue({ maxAttempts: 1 });
    const ids = await strandJobs(queue, 5);

    const { recovered, discarded } = await reaperWith({ batchSize: 2 }).runOnce();

    expect(recovered).toEqual([]);
    expect(discarded.sort(byId)).toEqual(ids.sort(byId));
    expect(await h.jobRepo.countByStatus(queue.id)).toEqual({ pending: 0, processing: 0 });
  });

  it('reclaims the longest-expired lease first, whatever the creation order', async () => {
    const queue = await fastQueue();
    const [a, b, c] = await strandJobs(queue, 3);
    // Created a, b, c; expired b first, then c, then a.
    const expiredAgo: Array<[number, number]> = [[a, 1], [b, 3], [c, 2]];
    for (const [id, seconds] of expiredAgo) {
      await h.pool.query(
        `UPDATE jobs SET lease_expires_at = now() - make_interval(secs => $2) WHERE id = $1`,
        [id, seconds]
      );
    }

    const order: number[] = [];
    for (let i = 0; i < 3; i++) {
      order.push(...(await h.jobRepo.recoverJobs(1)).recovered);
    }
    expect(order).toEqual([b, c, a]);
  });

  it('a scheduled pass drains the backlog without waiting for the next interval', async () => {
    const queue = await fastQueue();
    await strandJobs(queue, 5);
    // The next pass is a minute away, so only the pass start() runs at once
    // can bring all five back before the deadline.
    const reaper = reaperWith({ batchSize: 2, interval: 60_000 });

    reaper.start();
    try {
      await waitFor(async () => (await h.jobRepo.countByStatus(queue.id)).pending === 5, {
        timeout: 5_000,
        message: 'the whole backlog back in PENDING',
      });
    } finally {
      await reaper.stop();
    }
  });

  it('stop() during a drain waits for the batch in flight, not the whole backlog', async () => {
    const queue = await fastQueue();
    await strandJobs(queue, 20);
    const reaper = reaperWith({ batchSize: 1, interval: 60_000 });

    reaper.start();
    await reaper.stop();

    const { pending, processing } = await h.jobRepo.countByStatus(queue.id);
    expect(pending, 'the drain ran on after stop()').toBeLessThanOrEqual(1);
    expect(processing).toBeGreaterThanOrEqual(19);
  });
});
