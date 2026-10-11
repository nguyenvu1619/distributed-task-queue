import { Pool } from 'pg';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { JobStatus } from '../src/domain/job';
import { NUMBER_OF_SHARD } from '../src/domain/queue';
import { JobSnooze, LeaseLostError } from '../src/domain/errors';
import { WorkerService } from '../src/services/worker.service';
import { Harness, createHarness, jobInput, queueInput, resetDatabase, waitFor } from './support/harness';
import { readGroupCounters, readJobRow, readShardCounters } from './support/invariants';

let h: Harness;

beforeEach(async () => {
  h = createHarness({ maxConnections: 10 });
  await resetDatabase(h.pool);
});

afterEach(async () => {
  await h.close();
});

/** Pull `available_at` into the past so a snoozed job is due without waiting. */
async function makeDue(pool: Pool, id: number | string): Promise<void> {
  await pool.query(`UPDATE jobs SET available_at = now() - interval '1 second' WHERE id = $1`, [id]);
}

describe('snooze — fast path', () => {
  it('returns the job to PENDING and hides it from pull until available_at', async () => {
    const queue = await h.queueService.createQueue(queueInput({ concurrency: 0 }));
    const published = await h.jobRepo.publishJob(jobInput(queue.id));

    const pulled = await h.jobRepo.pullJob(queue);
    const outcome = await h.jobRepo.snoozeJob(pulled!.id, pulled!.lockSeq, queue, 60_000);
    expect(outcome.status).toBe(JobStatus.PENDING);
    expect(outcome.availableAt).not.toBeNull();

    const row = await readJobRow(h.pool, published.id);
    expect(row.status).toBe(JobStatus.PENDING);
    expect(row.lease_expires_at).toBeNull();
    expect(row.deferred).toBe(true);
    // lease_seq survives the snooze — it is the fence token, not lease state.
    expect(String(row.lease_seq)).toBe(String(pulled!.lockSeq));

    expect(await h.jobRepo.pullJob(queue)).toBeNull();
  });

  it('hands the job out again once available_at passes, under a newer lease', async () => {
    const queue = await h.queueService.createQueue(queueInput({ concurrency: 0 }));
    const published = await h.jobRepo.publishJob(jobInput(queue.id));

    const first = await h.jobRepo.pullJob(queue);
    await h.jobRepo.snoozeJob(first!.id, first!.lockSeq, queue, 60_000);
    await makeDue(h.pool, published.id);

    const second = await h.jobRepo.pullJob(queue);
    expect(second).not.toBeNull();
    expect(second!.id).toBe(published.id);
    expect(second!.lockSeq).toBeGreaterThan(first!.lockSeq);
  });

  it('does not spend the attempt budget', async () => {
    const queue = await h.queueService.createQueue(queueInput({ concurrency: 0, maxAttempts: 1 }));
    const published = await h.jobRepo.publishJob(jobInput(queue.id));

    for (let i = 0; i < 3; i++) {
      const pulled = await h.jobRepo.pullJob(queue);
      expect(pulled, `snooze ${i + 1} left the job unpullable`).not.toBeNull();
      expect(pulled!.attempts).toBe(1);
      await h.jobRepo.snoozeJob(pulled!.id, pulled!.lockSeq, queue, 60_000);
      expect((await readJobRow(h.pool, published.id)).attempts).toBe(0);
      await makeDue(h.pool, published.id);
    }

    // The single real attempt is still there to spend.
    const last = await h.jobRepo.pullJob(queue);
    expect((await h.jobRepo.failJob(last!.id, last!.lockSeq, queue)).status).toBe(JobStatus.FAILED);
  });

  it('rejects a snooze carrying the wrong lease_seq', async () => {
    const queue = await h.queueService.createQueue(queueInput({ concurrency: 0 }));
    await h.jobRepo.publishJob(jobInput(queue.id));

    const pulled = await h.jobRepo.pullJob(queue);
    await expect(
      h.jobRepo.snoozeJob(pulled!.id, pulled!.lockSeq + 1, queue, 60_000)
    ).rejects.toBeInstanceOf(LeaseLostError);

    const row = await readJobRow(h.pool, pulled!.id);
    expect(row.status).toBe(JobStatus.PROCESSING);
    expect(row.available_at).toBeNull();
  });

  it('rejects snoozing a job that was already completed', async () => {
    const queue = await h.queueService.createQueue(queueInput({ concurrency: 0 }));
    await h.jobRepo.publishJob(jobInput(queue.id));

    const pulled = await h.jobRepo.pullJob(queue);
    await h.jobRepo.completeJob(pulled!.id, pulled!.lockSeq, queue);
    await expect(
      h.jobRepo.snoozeJob(pulled!.id, pulled!.lockSeq, queue, 60_000)
    ).rejects.toBeInstanceOf(LeaseLostError);
  });
});

describe('snooze — coordination path', () => {
  it('gives the shard back and hides the job until available_at', async () => {
    const queue = await h.queueService.createQueue(
      queueInput({ concurrency: NUMBER_OF_SHARD * 2 })
    );
    const published = await h.jobRepo.publishJob(jobInput(queue.id));

    const pulled = await h.jobRepo.pullJob(queue);
    await h.jobRepo.snoozeJob(pulled!.id, pulled!.lockSeq, queue, 60_000);

    const shards = await readShardCounters(h.pool, queue.id);
    expect(shards.reduce((sum, s) => sum + s.running, 0)).toBe(0);

    const row = await readJobRow(h.pool, published.id);
    expect(row.status).toBe(JobStatus.PENDING);
    expect(row.queue_shard_no, 'a snoozed job must not keep claiming a shard').toBeNull();
    expect(row.attempts).toBe(0);
    expect(row.deferred).toBe(true);

    expect(await h.jobRepo.pullJob(queue)).toBeNull();
    await makeDue(h.pool, published.id);
    expect(await h.jobRepo.pullJob(queue)).not.toBeNull();
  });

  it('gives the group slot back', async () => {
    const queue = await h.queueService.createQueue(
      queueInput({ concurrency: 0, requiresGroupId: true })
    );
    await h.jobRepo.publishJob(
      jobInput(queue.id, { group: { id: 'tenant-a', concurrency: 1 } })
    );

    const pulled = await h.jobRepo.pullJob(queue);
    expect((await readGroupCounters(h.pool, queue.id))[0].running).toBe(1);

    await h.jobRepo.snoozeJob(pulled!.id, pulled!.lockSeq, queue, 60_000);
    expect((await readGroupCounters(h.pool, queue.id))[0].running).toBe(0);
  });

  it('rejects a snooze carrying the wrong lease_seq without releasing the slot', async () => {
    const queue = await h.queueService.createQueue(
      queueInput({ concurrency: NUMBER_OF_SHARD * 2 })
    );
    await h.jobRepo.publishJob(jobInput(queue.id));

    const pulled = await h.jobRepo.pullJob(queue);
    await expect(
      h.jobRepo.snoozeJob(pulled!.id, pulled!.lockSeq + 1, queue, 60_000)
    ).rejects.toBeInstanceOf(LeaseLostError);

    const shards = await readShardCounters(h.pool, queue.id);
    expect(shards.reduce((sum, s) => sum + s.running, 0)).toBe(1);
  });
});

describe('snooze — worker', () => {
  it('snoozes the job when its handler throws JobSnooze', async () => {
    const queue = await h.queueService.createQueue(queueInput({ concurrency: 0 }));
    const published = await h.jobRepo.publishJob(jobInput(queue.id));

    let calls = 0;
    const worker = new WorkerService(h.jobService, {
      queueId: queue.id,
      pollInterval: 20,
      handler: async () => {
        calls += 1;
        throw new JobSnooze(60_000);
      },
    });

    await worker.start();
    try {
      await waitFor(
        async () => {
          const row = await readJobRow(h.pool, published.id);
          return row.status === JobStatus.PENDING && row.deferred === true;
        },
        { message: 'the job to be snoozed' }
      );
    } finally {
      await worker.stop();
    }

    expect(calls).toBe(1);
    expect((await readJobRow(h.pool, published.id)).attempts).toBe(0);
  });
});
