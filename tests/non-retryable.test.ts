import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { JobStatus } from '../src/domain/job';
import { NUMBER_OF_SHARD } from '../src/domain/queue';
import { LeaseLostError, NonRetryable } from '../src/domain/errors';
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

describe('non-retryable failure — fast path', () => {
  it('discards the job on the first failure, whatever budget it has left', async () => {
    const queue = await h.queueService.createQueue(queueInput({ concurrency: 0, maxAttempts: 3 }));
    const published = await h.jobRepo.publishJob(jobInput(queue.id));

    const pulled = await h.jobRepo.pullJob(queue);
    const outcome = await h.jobRepo.failJob(pulled!.id, pulled!.lockSeq, queue, true);

    expect(outcome.status).toBe(JobStatus.FAILED);
    expect(await readJobRow(h.pool, published.id)).toBeNull();
    expect(await h.jobRepo.pullJob(queue)).toBeNull();
  });

  it('leaves every other job alone', async () => {
    const queue = await h.queueService.createQueue(queueInput({ concurrency: 0 }));
    await h.jobRepo.publishJob(jobInput(queue.id));
    const bystander = await h.jobRepo.publishJob(jobInput(queue.id));

    const pulled = await h.jobRepo.pullJob(queue);
    await h.jobRepo.failJob(pulled!.id, pulled!.lockSeq, queue, true);

    expect((await readJobRow(h.pool, bystander.id))?.status).toBe(JobStatus.PENDING);
  });

  it('rejects a non-retryable failure carrying the wrong lease_seq, touching nothing', async () => {
    const queue = await h.queueService.createQueue(queueInput({ concurrency: 0 }));
    await h.jobRepo.publishJob(jobInput(queue.id));
    const bystander = await h.jobRepo.publishJob(jobInput(queue.id));

    const pulled = await h.jobRepo.pullJob(queue);
    await expect(
      h.jobRepo.failJob(pulled!.id, pulled!.lockSeq + 1, queue, true)
    ).rejects.toBeInstanceOf(LeaseLostError);

    expect((await readJobRow(h.pool, pulled!.id))?.status).toBe(JobStatus.PROCESSING);
    expect((await readJobRow(h.pool, bystander.id))?.status).toBe(JobStatus.PENDING);
  });

  it('treats an omitted flag as retryable', async () => {
    const queue = await h.queueService.createQueue(queueInput({ concurrency: 0, maxAttempts: 3 }));
    await h.jobRepo.publishJob(jobInput(queue.id));

    const pulled = await h.jobRepo.pullJob(queue);
    const outcome = await h.jobRepo.failJob(pulled!.id, pulled!.lockSeq, queue);

    expect(outcome.status).toBe(JobStatus.PENDING);
  });
});

describe('non-retryable failure — coordination path', () => {
  it('discards the job and gives the shard back', async () => {
    const queue = await h.queueService.createQueue(
      queueInput({ concurrency: NUMBER_OF_SHARD * 2, maxAttempts: 3 })
    );
    const published = await h.jobRepo.publishJob(jobInput(queue.id));

    const pulled = await h.jobRepo.pullJob(queue);
    const outcome = await h.jobRepo.failJob(pulled!.id, pulled!.lockSeq, queue, true);

    expect(outcome.status).toBe(JobStatus.FAILED);
    expect(await readJobRow(h.pool, published.id)).toBeNull();
    const shards = await readShardCounters(h.pool, queue.id);
    expect(shards.reduce((sum, s) => sum + s.running, 0)).toBe(0);
  });

  it('gives the group slot back', async () => {
    const queue = await h.queueService.createQueue(
      queueInput({ concurrency: 0, requiresGroupId: true, maxAttempts: 3 })
    );
    const published = await h.jobRepo.publishJob(
      jobInput(queue.id, { group: { id: 'tenant-a', concurrency: 1 } })
    );

    const pulled = await h.jobRepo.pullJob(queue);
    await h.jobRepo.failJob(pulled!.id, pulled!.lockSeq, queue, true);

    expect(await readJobRow(h.pool, published.id)).toBeNull();
    expect((await readGroupCounters(h.pool, queue.id))[0].running).toBe(0);
  });

  it('leaves every other job alone', async () => {
    const queue = await h.queueService.createQueue(
      queueInput({ concurrency: NUMBER_OF_SHARD * 2 })
    );
    await h.jobRepo.publishJob(jobInput(queue.id));
    const bystander = await h.jobRepo.publishJob(jobInput(queue.id));

    const pulled = await h.jobRepo.pullJob(queue);
    await h.jobRepo.failJob(pulled!.id, pulled!.lockSeq, queue, true);

    expect((await readJobRow(h.pool, bystander.id))?.status).toBe(JobStatus.PENDING);
  });
});

describe('non-retryable failure — worker', () => {
  it('runs the handler once when it throws NonRetryable', async () => {
    const queue = await h.queueService.createQueue(queueInput({ concurrency: 0, maxAttempts: 3 }));
    const published = await h.jobRepo.publishJob(jobInput(queue.id));

    let calls = 0;
    const worker = new WorkerService(h.jobService, {
      queueId: queue.id,
      pollInterval: 20,
      handler: async () => {
        calls += 1;
        throw new NonRetryable('payload can never succeed');
      },
    });

    await worker.start();
    try {
      await waitFor(async () => (await readJobRow(h.pool, published.id)) === null, {
        message: 'the job to be discarded',
      });
    } finally {
      await worker.stop();
    }

    expect(calls).toBe(1);
  });
});
