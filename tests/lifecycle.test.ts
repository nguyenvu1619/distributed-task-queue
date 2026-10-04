import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { JobStatus } from '../src/domain/job';
import { CreateQueueInput, NUMBER_OF_SHARD } from '../src/domain/queue';
import { JobNotFoundError, LeaseLostError, QueueNotFoundError } from '../src/domain/errors';
import { Logger, silentLogger } from '../src/domain/logger';
import { QueueRepository } from '../src/repository/postgresql/queue.repository';
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

/** A logger that swallows everything but collects `warn` lines for assertions. */
function captureWarnings(sink: string[]): Logger {
  return { ...silentLogger, warn: (message: string) => void sink.push(message) };
}

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

  it('returns the queue that already owns the name instead of failing on the unique index', async () => {
    const warnings: string[] = [];
    const repo = new QueueRepository(h.pool, captureWarnings(warnings));
    const name = uniqueName('dup');

    const first = await repo.createQueue(
      queueInput({ name, maxAttempts: 3, concurrency: NUMBER_OF_SHARD })
    );
    // Deliberately different config: the existing queue wins, it is not patched.
    const second = await repo.createQueue(
      queueInput({ name, maxAttempts: 9, concurrency: NUMBER_OF_SHARD * 4 })
    );

    expect(second.id, 'a duplicate name produced a second queue').toBe(first.id);
    expect(second.maxAttempts).toBe(3);
    expect(second.concurrency).toBe(NUMBER_OF_SHARD);
    expect(
      warnings.some((w) => w.includes(name)),
      'the losing create returned silently instead of warning'
    ).toBe(true);

    // The loser must not have topped up the winner's shards on its way out.
    const shards = await readShardCounters(h.pool, first.id);
    expect(shards).toHaveLength(NUMBER_OF_SHARD);
    expect(shards.reduce((sum, s) => sum + s.maxRunning, 0)).toBe(NUMBER_OF_SHARD);
  });

  it('collapses a concurrent race on one name onto a single queue and one set of shards', async () => {
    const RACERS = 8;
    const concurrency = NUMBER_OF_SHARD * 2;
    const warnings: string[] = [];
    const repo = new QueueRepository(h.pool, captureWarnings(warnings));
    const name = uniqueName('race');

    const created = await Promise.all(
      Array.from({ length: RACERS }, () => repo.createQueue(queueInput({ name, concurrency })))
    );

    expect(new Set(created.map((q) => q.id)).size, 'the racers disagreed on which queue is theirs')
      .toBe(1);
    expect(warnings, 'every loser but none of the winners must warn').toHaveLength(RACERS - 1);

    const { rows } = await h.pool.query(
      'SELECT count(*)::int AS n FROM queues WHERE name = $1',
      [name]
    );
    expect(rows[0].n).toBe(1);

    // The decisive one: shard rows are the queue's capacity, so a loser writing
    // its own set would silently multiply the cap by the number of racers.
    const shards = await readShardCounters(h.pool, created[0].id);
    expect(shards).toHaveLength(NUMBER_OF_SHARD);
    expect(shards.reduce((sum, s) => sum + s.maxRunning, 0)).toBe(concurrency);
  });

  it('rejects a lookup for an unknown queue', async () => {
    await expect(h.queueService.getQueue(999_999)).rejects.toBeInstanceOf(QueueNotFoundError);
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
    expect(job.lockSeq).toBe(0);
    expect(job.queueShardNo).toBeNull();
    expect(job.attempts).toBe(0);
  });

  it('returns the live job for a duplicate idempotency key instead of raising', async () => {
    const queue = await fastQueue();
    const key = uniqueName('dupe');

    const first = await h.jobRepo.publishJob(jobInput(queue.id, { idempotencyKey: key }));
    const second = await h.jobRepo.publishJob(jobInput(queue.id, { idempotencyKey: key }));

    expect(first.deduplicated).toBe(false);
    expect(second.deduplicated).toBe(true);
    expect(second.id).toBe(first.id);

    const { rows } = await h.pool.query(
      'SELECT count(*)::int AS n FROM jobs WHERE queue_id = $1',
      [queue.id]
    );
    expect(rows[0].n).toBe(1);
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

    await h.jobRepo.completeJob(pulled!.id, pulled!.lockSeq, queue);

    expect(await readJobRow(h.pool, published.id)).toBeNull();
  });

  it('removes the row from `jobs` when a job exhausts its attempts', async () => {
    const queue = await fastQueue({ maxAttempts: 1 });
    const published = await h.jobRepo.publishJob(jobInput(queue.id));
    const pulled = await h.jobRepo.pullJob(queue);

    const failed = await h.jobRepo.failJob(pulled!.id, pulled!.lockSeq, queue);

    expect(failed.status).toBe(JobStatus.FAILED);
    expect(await readJobRow(h.pool, published.id)).toBeNull();
  });

  it('rejects a settle carrying the wrong lease_seq', async () => {
    const queue = await fastQueue();
    await h.jobRepo.publishJob(jobInput(queue.id));
    const pulled = await h.jobRepo.pullJob(queue);

    await expect(
      h.jobRepo.completeJob(pulled!.id, Number(pulled!.lockSeq) + 99, queue)
    ).rejects.toBeInstanceOf(LeaseLostError);

    // ...and the job is untouched by the rejected attempt.
    const row = await readJobRow(h.pool, pulled!.id);
    expect(row.status).toBe(JobStatus.PROCESSING);
  });

  it('rejects completing the same job twice', async () => {
    const queue = await fastQueue();
    await h.jobRepo.publishJob(jobInput(queue.id));
    const pulled = await h.jobRepo.pullJob(queue);

    await h.jobRepo.completeJob(pulled!.id, pulled!.lockSeq, queue);
    await expect(
      h.jobRepo.completeJob(pulled!.id, pulled!.lockSeq, queue)
    ).rejects.toBeInstanceOf(LeaseLostError);
  });

  it('rejects settling a job that was never pulled', async () => {
    const queue = await fastQueue();
    const published = await h.jobRepo.publishJob(jobInput(queue.id));

    await expect(h.jobRepo.completeJob(published.id, 1, queue)).rejects.toBeInstanceOf(
      LeaseLostError
    );
  });

  it('does not retain terminal jobs — getById after completion is a miss', async () => {
    const queue = await fastQueue();
    await h.jobRepo.publishJob(jobInput(queue.id));
    const pulled = await h.jobRepo.pullJob(queue);
    await h.jobRepo.completeJob(pulled!.id, pulled!.lockSeq, queue);

    // Characterisation, not a requirement: there is no `job_status` archive in
    // the current schema, so terminal jobs are simply gone. See test report.
    await expect(h.jobRepo.getById(pulled!.id)).rejects.toBeInstanceOf(JobNotFoundError);
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

    await h.jobRepo.failJob(first!.id, first!.lockSeq, queue);
    const second = await h.jobRepo.pullJob(queue);
    expect(second!.attempts).toBe(2);
  });

  it('returns a failed job to the queue while it still has attempts left', async () => {
    const queue = await h.queueService.createQueue(
      queueInput({ concurrency: 0, maxAttempts: 3 })
    );
    const published = await h.jobRepo.publishJob(jobInput(queue.id));

    const pulled = await h.jobRepo.pullJob(queue);
    const outcome = await h.jobRepo.failJob(pulled!.id, pulled!.lockSeq, queue);

    expect(outcome.status).toBe(JobStatus.PENDING);

    const row = await readJobRow(h.pool, published.id);
    expect(row.status).toBe(JobStatus.PENDING);
    expect(row.lease_expires_at).toBeNull();
    // lease_seq survives the retry — it is the fence token, not lease state.
    expect(String(row.lease_seq)).toBe(String(pulled!.lockSeq));
  });

  it('discards the job on the failure that spends the last attempt', async () => {
    const maxAttempts = 3;
    const queue = await h.queueService.createQueue(queueInput({ concurrency: 0, maxAttempts }));
    const published = await h.jobRepo.publishJob(jobInput(queue.id));

    const outcomes: JobStatus[] = [];
    for (let i = 0; i < maxAttempts; i++) {
      const pulled = await h.jobRepo.pullJob(queue);
      expect(pulled, `attempt ${i + 1} of ${maxAttempts} was not offered`).not.toBeNull();
      outcomes.push((await h.jobRepo.failJob(pulled!.id, pulled!.lockSeq, queue)).status);
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
    const outcome = await h.jobRepo.failJob(pulled!.id, pulled!.lockSeq, queue);
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
    await h.jobRepo.completeJob(first!.id, first!.lockSeq, queue);

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

    await h.jobRepo.completeJob(pulled!.id, pulled!.lockSeq, queue);

    const released = await readShardCounters(h.pool, queue.id);
    expect(released.reduce((sum, s) => sum + s.running, 0)).toBe(0);
  });

  it('gives a queue below NUMBER_OF_SHARD its full capacity, one slot per shard', async () => {
    // Floor division would hand every shard max_running = 0 here and the queue
    // would admit nothing. Shards that carry no slot are not written at all, so
    // the row count is the capacity and the pull gate never scans a dead row.
    const concurrency = 5;
    expect(concurrency, 'this test only means anything below the shard count').toBeLessThan(
      NUMBER_OF_SHARD
    );
    const queue = await h.queueService.createQueue(queueInput({ concurrency }));

    const shards = await readShardCounters(h.pool, queue.id);
    expect(shards).toHaveLength(concurrency);
    expect(shards.every((s) => s.maxRunning === 1)).toBe(true);
    expect(shards.reduce((sum, s) => sum + s.maxRunning, 0)).toBe(concurrency);

    for (let i = 0; i < concurrency + 1; i++) {
      await h.jobRepo.publishJob(jobInput(queue.id));
    }

    const pulled = [];
    for (let i = 0; i < concurrency; i++) {
      const job = await h.jobRepo.pullJob(queue);
      expect(job, `slot ${i} was refused even though the queue still had capacity`).not.toBeNull();
      pulled.push(job!);
    }
    expect(
      await h.jobRepo.pullJob(queue),
      'the queue admitted more jobs than its configured concurrency'
    ).toBeNull();

    await h.jobRepo.completeJob(pulled[0].id, pulled[0].lockSeq, queue);
    expect(
      await h.jobRepo.pullJob(queue),
      'a released slot was not handed back out'
    ).not.toBeNull();
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

    await h.jobRepo.completeJob(pulled!.id, pulled!.lockSeq, queue);
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
    await h.jobRepo.failJob(pulled!.id, pulled!.lockSeq, queue);

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

describe('queue backlog', () => {
  it('reports zero for a queue nothing was published to', async () => {
    const queue = await h.queueService.createQueue(queueInput());
    expect(await h.jobRepo.countByStatus(queue.id)).toEqual({ pending: 0, processing: 0 });
  });

  it('moves a job from pending to processing and drops it once terminal', async () => {
    const queue = await h.queueService.createQueue(queueInput());
    await h.jobService.publishJobs([jobInput(queue.id), jobInput(queue.id), jobInput(queue.id)]);

    expect(await h.jobRepo.countByStatus(queue.id)).toEqual({ pending: 3, processing: 0 });

    const leased = await h.jobRepo.pullJob(queue);
    expect(await h.jobRepo.countByStatus(queue.id)).toEqual({ pending: 2, processing: 1 });

    // Terminal jobs are deleted, so the backlog is the whole story — a settled
    // job leaves no residue in either count.
    await h.jobRepo.completeJob(leased!.id, leased!.lockSeq, queue);
    expect(await h.jobRepo.countByStatus(queue.id)).toEqual({ pending: 2, processing: 0 });
  });

  it('counts only the queue it was asked about', async () => {
    const first = await h.queueService.createQueue(queueInput());
    const second = await h.queueService.createQueue(queueInput());
    await h.jobService.publishJobs([jobInput(first.id), jobInput(first.id), jobInput(second.id)]);

    expect(await h.jobRepo.countByStatus(first.id)).toEqual({ pending: 2, processing: 0 });
    expect(await h.jobRepo.countByStatus(second.id)).toEqual({ pending: 1, processing: 0 });
  });
});
