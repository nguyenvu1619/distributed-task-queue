import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { JobStatus } from '../src/domain/job';
import { CreateQueueInput, NUMBER_OF_SHARD } from '../src/domain/queue';
import { ErrorCodes } from '../src/domain/errors';
import {
  Harness,
  createHarness,
  jobInput,
  queueInput,
  resetDatabase,
  uniqueName,
} from './support/harness';
import { readGroupCounters, readJobRow, readShardCounters } from './support/invariants';

let h: Harness;

// A fresh harness per test: QueueRepository memoises queues in-process, so a
// reused instance would keep serving rows that TRUNCATE has already removed.
beforeEach(async () => {
  h = createHarness({ maxConnections: 10 });
  await resetDatabase(h.pool);
});

afterEach(async () => {
  await h.close();
});

describe('queue lifecycle', () => {
  it('round-trips queue configuration through the nanosecond-encoded lease column', async () => {
    const created = await h.queueService.createQueue(
      queueInput({ name: uniqueName('cfg'), maxAttempts: 5, leaseDuration: 30_000, concurrency: 0 })
    );

    expect(created.maxAttempts).toBe(5);
    expect(created.leaseDuration).toBe(30_000);
    expect(created.requiresGroupId).toBe(false);

    const { rows } = await h.pool.query('SELECT lease_duration FROM queues WHERE id = $1', [
      created.id,
    ]);
    // Stored as nanoseconds for cross-language (Go time.Duration) compatibility.
    expect(String(rows[0].lease_duration)).toBe('30000000000');
  });

  it('creates no shards for a fast-path queue', async () => {
    const queue = await h.queueService.createQueue(queueInput({ concurrency: 0 }));
    expect(await readShardCounters(h.pool, queue.id)).toHaveLength(0);
  });

  it('creates one shard row per shard, splitting concurrency evenly', async () => {
    const concurrency = NUMBER_OF_SHARD * 2; // 64
    const queue = await h.queueService.createQueue(queueInput({ concurrency }));

    const shards = await readShardCounters(h.pool, queue.id);
    expect(shards).toHaveLength(NUMBER_OF_SHARD);
    expect(shards.map((s) => s.shardNo)).toEqual([...Array(NUMBER_OF_SHARD).keys()]);
    expect(shards.every((s) => s.maxRunning === 2)).toBe(true);
    expect(shards.every((s) => s.running === 0)).toBe(true);

    // The advertised cap must equal what the shards can actually admit.
    const admissible = shards.reduce((sum, s) => sum + s.maxRunning, 0);
    expect(admissible).toBe(concurrency);
  });

  it('rejects a lookup for an unknown queue', async () => {
    await expect(h.queueService.getQueue(999_999)).rejects.toMatchObject({
      code: ErrorCodes.QUEUE_NOT_FOUND,
    });
  });

  it('lists queues', async () => {
    await h.queueService.createQueue(queueInput());
    await h.queueService.createQueue(queueInput());
    expect(await h.queueService.getAllQueues()).toHaveLength(2);
  });
});

describe('job lifecycle — fast path (concurrency = 0, no groups)', () => {
  const fastQueue = (overrides: Partial<CreateQueueInput> = {}) =>
    h.queueService.createQueue(queueInput({ concurrency: 0, ...overrides }));

  it('publishes a job in PENDING with no lease and no shard', async () => {
    const queue = await fastQueue();
    const job = await h.jobRepo.publishJob(jobInput(queue.id, { payload: '{"a":1}' }));

    expect(job.status).toBe(JobStatus.PENDING);
    expect(job.leaseExpiresAt).toBeNull();
    expect(job.lockSeq).toBeNull();
    expect(job.queueShardNo).toBeNull();
    expect(job.attempts).toBe(0);
  });

  it('deduplicates a repeated idempotency key instead of inserting twice', async () => {
    const queue = await fastQueue();
    const key = uniqueName('dupe');

    const first = await h.jobRepo.publishJob(jobInput(queue.id, { idempotencyKey: key }));
    const second = await h.jobRepo.publishJob(jobInput(queue.id, { idempotencyKey: key }));

    expect(first.deduplicated).toBe(false);
    expect(second.deduplicated).toBe(true);
    expect(second.id).toBe(first.id);

    // Exactly one row, so exactly one delivery.
    const count = await h.pool.query('SELECT count(*)::int AS n FROM jobs WHERE queue_id = $1', [
      queue.id,
    ]);
    expect(count.rows[0].n).toBe(1);
  });

  it('returns null when the queue is empty', async () => {
    const queue = await fastQueue();
    expect(await h.jobRepo.pullJob(queue)).toBeNull();
  });

  it('moves a pulled job to PROCESSING and stamps a lease', async () => {
    const queue = await fastQueue();
    const published = await h.jobRepo.publishJob(jobInput(queue.id));

    const before = Date.now();
    const pulled = await h.jobRepo.pullJob(queue);
    expect(pulled).not.toBeNull();

    expect(String(pulled!.id)).toBe(String(published.id));
    expect(pulled!.status).toBe(JobStatus.PROCESSING);
    expect(pulled!.leaseExpiresAt).toBeInstanceOf(Date);
    expect(pulled!.leaseExpiresAt!.getTime()).toBeGreaterThan(before);
    expect(Number(pulled!.lockSeq)).toBe(1);

    const row = await readJobRow(h.pool, published.id);
    expect(row.status).toBe(JobStatus.PROCESSING);
    // Fast path performs no shard coordination.
    expect(row.queue_shard_no).toBeNull();
  });

  it('hands jobs out in FIFO order', async () => {
    const queue = await fastQueue();
    const published = [];
    for (let i = 0; i < 3; i++) {
      published.push(await h.jobRepo.publishJob(jobInput(queue.id, { payload: `{"n":${i}}` })));
    }

    const order = [];
    for (let i = 0; i < 3; i++) {
      order.push(String((await h.jobRepo.pullJob(queue))!.id));
    }

    expect(order).toEqual(published.map((j) => String(j.id)));
  });

  it('removes the row from `jobs` when a job completes', async () => {
    const queue = await fastQueue();
    const published = await h.jobRepo.publishJob(jobInput(queue.id));
    const pulled = await h.jobRepo.pullJob(queue);

    const completed = await h.jobRepo.completeJob(pulled!.id, pulled!.lockSeq!, queue);

    expect(completed.status).toBe(JobStatus.COMPLETED);
    expect(completed.completedAt).toBeInstanceOf(Date);
    expect(await readJobRow(h.pool, published.id)).toBeNull();
  });

  it('removes the row from `jobs` when a job exhausts its attempts', async () => {
    const queue = await fastQueue({ maxAttempts: 1 });
    const published = await h.jobRepo.publishJob(jobInput(queue.id));
    const pulled = await h.jobRepo.pullJob(queue);

    const failed = await h.jobRepo.failJob(pulled!.id, pulled!.lockSeq!, queue);

    expect(failed.status).toBe(JobStatus.FAILED);
    expect(await readJobRow(h.pool, published.id)).toBeNull();
  });

  it('rejects a settle carrying the wrong lease_seq', async () => {
    const queue = await fastQueue();
    await h.jobRepo.publishJob(jobInput(queue.id));
    const pulled = await h.jobRepo.pullJob(queue);

    await expect(
      h.jobRepo.completeJob(pulled!.id, Number(pulled!.lockSeq) + 99, queue)
    ).rejects.toMatchObject({ code: ErrorCodes.LEASE_LOST, retryable: false });

    // ...and the job is untouched by the rejected attempt.
    const row = await readJobRow(h.pool, pulled!.id);
    expect(row.status).toBe(JobStatus.PROCESSING);
  });

  it('rejects completing the same job twice', async () => {
    const queue = await fastQueue();
    await h.jobRepo.publishJob(jobInput(queue.id));
    const pulled = await h.jobRepo.pullJob(queue);

    await h.jobRepo.completeJob(pulled!.id, pulled!.lockSeq!, queue);

    // LEASE_LOST rather than JOB_NOT_FOUND, deliberately. The first settle DELETEd
    // the row, so the second one's predicate matches nothing — but the settle cannot
    // tell "row gone" from "another worker holds it" without a second probe, and both
    // mean the same thing to the caller: this job is not yours, abandon it.
    await expect(
      h.jobRepo.completeJob(pulled!.id, pulled!.lockSeq!, queue)
    ).rejects.toMatchObject({ code: ErrorCodes.LEASE_LOST });
  });

  // The settle path resolves the queue with a getById first. That lookup missing must
  // NOT surface as JOB_NOT_FOUND, or the fencing race would report one code through
  // JobService and another through the *Direct methods for the very same event.
  it('reports a lost lease, not a missing job, when settling through JobService', async () => {
    const queue = await fastQueue();
    await h.jobRepo.publishJob(jobInput(queue.id));
    const pulled = await h.jobRepo.pullJob(queue);

    // Settling deletes the row, so the second attempt's pre-flight lookup misses.
    await h.jobRepo.completeJob(pulled!.id, pulled!.lockSeq!, queue);

    await expect(
      h.jobService.completeJob(pulled!.id, Number(pulled!.lockSeq))
    ).rejects.toMatchObject({
      code: ErrorCodes.LEASE_LOST,
      context: { operation: 'completeJob' },
    });

    await expect(
      h.jobService.failJob(pulled!.id, Number(pulled!.lockSeq))
    ).rejects.toMatchObject({
      code: ErrorCodes.LEASE_LOST,
      context: { operation: 'failJob' },
    });
  });

  it('rejects settling a job that was never pulled', async () => {
    const queue = await fastQueue();
    const published = await h.jobRepo.publishJob(jobInput(queue.id));

    // The row exists and no lease was ever taken, so LEASE_LOST reads here as
    // "you do not hold this job's lease" rather than "your lease was taken".
    await expect(h.jobRepo.completeJob(published.id, 1, queue)).rejects.toMatchObject({
      code: ErrorCodes.LEASE_LOST,
    });
  });

  it('does not retain terminal jobs — getById after completion is a miss', async () => {
    const queue = await fastQueue();
    await h.jobRepo.publishJob(jobInput(queue.id));
    const pulled = await h.jobRepo.pullJob(queue);
    await h.jobRepo.completeJob(pulled!.id, pulled!.lockSeq!, queue);

    // Characterisation, not a requirement: there is no `job_status` archive in
    // the current schema, so terminal jobs are simply gone. See test report.
    await expect(h.jobRepo.getById(pulled!.id)).rejects.toMatchObject({
      code: ErrorCodes.JOB_NOT_FOUND,
    });
  });
});

describe('retry policy', () => {
  it('counts each lease as an attempt', async () => {
    const queue = await h.queueService.createQueue(
      queueInput({ concurrency: 0, maxAttempts: 5 })
    );
    await h.jobRepo.publishJob(jobInput(queue.id));

    const first = await h.jobRepo.pullJob(queue);
    expect(first!.attempts).toBe(1);

    await h.jobRepo.failJob(first!.id, first!.lockSeq!, queue);
    const second = await h.jobRepo.pullJob(queue);
    expect(second!.attempts).toBe(2);
  });

  it('returns a failed job to the queue while it still has attempts left', async () => {
    const queue = await h.queueService.createQueue(
      queueInput({ concurrency: 0, maxAttempts: 3 })
    );
    const published = await h.jobRepo.publishJob(jobInput(queue.id));

    const pulled = await h.jobRepo.pullJob(queue);
    const outcome = await h.jobRepo.failJob(pulled!.id, pulled!.lockSeq!, queue);

    expect(outcome.status).toBe(JobStatus.PENDING);

    const row = await readJobRow(h.pool, published.id);
    expect(row.status).toBe(JobStatus.PENDING);
    expect(row.lease_expires_at).toBeNull();
    // lease_seq survives the retry — it is the fence token, not lease state.
    expect(row.lease_seq).not.toBeNull();
  });

  it('discards the job on the failure that spends the last attempt', async () => {
    const maxAttempts = 3;
    const queue = await h.queueService.createQueue(queueInput({ concurrency: 0, maxAttempts }));
    const published = await h.jobRepo.publishJob(jobInput(queue.id));

    const outcomes: JobStatus[] = [];
    for (let i = 0; i < maxAttempts; i++) {
      const pulled = await h.jobRepo.pullJob(queue);
      expect(pulled, `attempt ${i + 1} of ${maxAttempts} was not offered`).not.toBeNull();
      outcomes.push((await h.jobRepo.failJob(pulled!.id, pulled!.lockSeq!, queue)).status);
    }

    expect(outcomes).toEqual([JobStatus.PENDING, JobStatus.PENDING, JobStatus.FAILED]);
    expect(await readJobRow(h.pool, published.id)).toBeNull();
    expect(await h.jobRepo.pullJob(queue)).toBeNull();
  });

  it('gives the coordination slot back when a failed job is retried', async () => {
    const queue = await h.queueService.createQueue(
      queueInput({ concurrency: NUMBER_OF_SHARD * 2, maxAttempts: 3 })
    );
    const published = await h.jobRepo.publishJob(jobInput(queue.id));

    const pulled = await h.jobRepo.pullJob(queue);
    const outcome = await h.jobRepo.failJob(pulled!.id, pulled!.lockSeq!, queue);
    expect(outcome.status).toBe(JobStatus.PENDING);

    const shards = await readShardCounters(h.pool, queue.id);
    expect(shards.reduce((sum, s) => sum + s.running, 0)).toBe(0);

    const row = await readJobRow(h.pool, published.id);
    expect(row.queue_shard_no, 'a retried job must not keep claiming a shard').toBeNull();

    // ...and it can be picked up again.
    expect(await h.jobRepo.pullJob(queue)).not.toBeNull();
  });

  it('does not count a completed job against the budget of anything else', async () => {
    const queue = await h.queueService.createQueue(
      queueInput({ concurrency: 0, maxAttempts: 1 })
    );
    await h.jobRepo.publishJob(jobInput(queue.id));
    await h.jobRepo.publishJob(jobInput(queue.id));

    const first = await h.jobRepo.pullJob(queue);
    await h.jobRepo.completeJob(first!.id, first!.lockSeq!, queue);

    const second = await h.jobRepo.pullJob(queue);
    expect(second).not.toBeNull();
    expect(second!.attempts).toBe(1);
  });
});

describe('job lifecycle — coordination path (concurrency > 0 / groups)', () => {
  it('assigns a shard and increments that shard on pull, releasing it on complete', async () => {
    const queue = await h.queueService.createQueue(
      queueInput({ concurrency: NUMBER_OF_SHARD * 2 })
    );
    await h.jobRepo.publishJob(jobInput(queue.id));

    const pulled = await h.jobRepo.pullJob(queue);
    expect(pulled).not.toBeNull();

    const row = await readJobRow(h.pool, pulled!.id);
    expect(row.queue_shard_no).not.toBeNull();

    const held = await readShardCounters(h.pool, queue.id);
    const runningWhileHeld = held.reduce((sum, s) => sum + s.running, 0);
    expect(
      runningWhileHeld,
      'exactly one slot must be accounted for while a job is held'
    ).toBe(1);

    await h.jobRepo.completeJob(pulled!.id, pulled!.lockSeq!, queue);

    const released = await readShardCounters(h.pool, queue.id);
    expect(released.reduce((sum, s) => sum + s.running, 0)).toBe(0);
  });

  it('reports the lease it just issued back to the caller', async () => {
    const queue = await h.queueService.createQueue(
      queueInput({ concurrency: NUMBER_OF_SHARD * 2, leaseDuration: 30_000 })
    );
    await h.jobRepo.publishJob(jobInput(queue.id));

    const pulled = await h.jobRepo.pullJob(queue);
    expect(pulled).not.toBeNull();

    const row = await readJobRow(h.pool, pulled!.id);
    expect(row.lease_expires_at, 'no lease was written to the row').not.toBeNull();
    expect(
      pulled!.leaseExpiresAt,
      'the returned Job carries no lease even though one was written'
    ).not.toBeNull();
  });

  it('honours a sub-second lease duration', async () => {
    // The fast path stamps the lease in milliseconds; the coordination path
    // converts it to a whole-second interval. Both must produce a lease that is
    // still in the future at the moment it is issued.
    const queue = await h.queueService.createQueue(
      queueInput({ concurrency: NUMBER_OF_SHARD * 2, leaseDuration: 500 })
    );
    await h.jobRepo.publishJob(jobInput(queue.id));

    const issuedAt = Date.now();
    const pulled = await h.jobRepo.pullJob(queue);
    expect(pulled).not.toBeNull();

    // Read the row rather than the returned object, so this test isolates the
    // interval arithmetic from how the lease is reported back.
    const row = await readJobRow(h.pool, pulled!.id);
    expect(
      new Date(row.lease_expires_at).getTime(),
      'the lease was already expired at the moment it was issued'
    ).toBeGreaterThan(issuedAt);
  });

  it('registers a group limit when a job is published with a group', async () => {
    const queue = await h.queueService.createQueue(
      queueInput({ concurrency: 0, requiresGroupId: true })
    );
    await h.jobRepo.publishJob(
      jobInput(queue.id, { group: { id: 'tenant-a', concurrency: 3 } })
    );

    const groups = await readGroupCounters(h.pool, queue.id);
    expect(groups).toEqual([{ groupId: 'tenant-a', maxRunning: 3, running: 0 }]);
  });

  it('accounts a group slot on pull and releases it on complete', async () => {
    const queue = await h.queueService.createQueue(
      queueInput({ concurrency: 0, requiresGroupId: true })
    );
    await h.jobRepo.publishJob(
      jobInput(queue.id, { group: { id: 'tenant-a', concurrency: 2 } })
    );

    const pulled = await h.jobRepo.pullJob(queue);
    expect(pulled).not.toBeNull();
    expect(pulled!.groupId).toBe('tenant-a');
    expect((await readGroupCounters(h.pool, queue.id))[0].running).toBe(1);

    await h.jobRepo.completeJob(pulled!.id, pulled!.lockSeq!, queue);
    expect((await readGroupCounters(h.pool, queue.id))[0].running).toBe(0);
  });

  it('releases a group slot when a job fails', async () => {
    const queue = await h.queueService.createQueue(
      queueInput({ concurrency: 0, requiresGroupId: true })
    );
    await h.jobRepo.publishJob(
      jobInput(queue.id, { group: { id: 'tenant-a', concurrency: 2 } })
    );

    const pulled = await h.jobRepo.pullJob(queue);
    await h.jobRepo.failJob(pulled!.id, pulled!.lockSeq!, queue);

    expect((await readGroupCounters(h.pool, queue.id))[0].running).toBe(0);
  });

  it('refuses to over-admit a group beyond its declared concurrency', async () => {
    const queue = await h.queueService.createQueue(
      queueInput({ concurrency: 0, requiresGroupId: true })
    );
    for (let i = 0; i < 3; i++) {
      await h.jobRepo.publishJob(
        jobInput(queue.id, { group: { id: 'tenant-a', concurrency: 1 } })
      );
    }

    expect(await h.jobRepo.pullJob(queue)).not.toBeNull();
    // Cap of 1 is taken; the next pull must be refused rather than over-admit.
    expect(await h.jobRepo.pullJob(queue)).toBeNull();
    expect((await readGroupCounters(h.pool, queue.id))[0].running).toBe(1);
  });
});

describe('domain mapping', () => {
  it('maps 64-bit identity columns to numbers, not strings', async () => {
    const queue = await h.queueService.createQueue(queueInput({ concurrency: 0 }));
    expect(typeof queue.id, 'Queue.id').toBe('number');

    await h.jobRepo.publishJob(jobInput(queue.id));
    const pulled = await h.jobRepo.pullJob(queue);

    expect(typeof pulled!.id, 'Job.id').toBe('number');
    expect(typeof pulled!.lockSeq, 'Job.lockSeq').toBe('number');
  });

  it('round-trips JSON metadata and normalises snake_case keys', async () => {
    const queue = await h.queueService.createQueue(queueInput({ concurrency: 0 }));
    const published = await h.jobRepo.publishJob(
      jobInput(queue.id, { metadata: { consumer_id: 'c-1', trace: { span: 'abc' } } })
    );

    const fetched = await h.jobRepo.getById(published.id);
    expect(fetched.metadata.consumerId).toBe('c-1');
    expect(fetched.metadata.consumer_id).toBeUndefined();
    expect(fetched.metadata.trace).toEqual({ span: 'abc' });
  });

  it('exposes pending jobs through pullJobs without leasing them', async () => {
    const queue = await h.queueService.createQueue(queueInput({ concurrency: 0 }));
    await h.jobRepo.publishJob(jobInput(queue.id));
    await h.jobRepo.publishJob(jobInput(queue.id));

    const listed = await h.jobRepo.pullJobs(JobStatus.PENDING, 10);
    expect(listed).toHaveLength(2);
    expect(listed.every((j) => j.leaseExpiresAt === null)).toBe(true);
  });
});
