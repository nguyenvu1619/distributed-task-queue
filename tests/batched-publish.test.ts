import { PoolClient } from 'pg';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { ConflictError } from '../src/domain/errors';
import { Executor } from '../src/domain/executor';
import { CreateJobInput, JobStatus } from '../src/domain/job';
import {
  Harness,
  createHarness,
  jobInput,
  queueInput,
  resetDatabase,
  uniqueName,
} from './support/harness';
import { countByStatus, readGroupCounters, readJobRow } from './support/invariants';

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

/** An Executor that records every statement it is asked to run. */
function countingExecutor(): Executor & { statements: string[] } {
  const statements: string[] = [];
  return {
    statements,
    query: async (text: string, values?: any[]) => {
      statements.push(text);
      return h.pool.query(text, values);
    },
  };
}

describe('batched publish', () => {
  it('lands the whole batch in a single round trip', async () => {
    const queue = await h.queueService.createQueue(queueInput());
    const inputs = Array.from({ length: 50 }, (_, i) =>
      jobInput(queue.id, { payload: JSON.stringify({ n: i }) })
    );

    const exec = countingExecutor();
    const jobs = await h.jobService.publishJobs(inputs, exec);

    expect(exec.statements).toHaveLength(1);
    expect(jobs).toHaveLength(50);
    expect(await countByStatus(h.pool, queue.id)).toMatchObject({ PENDING: 50 });
  });

  it('returns the jobs in the order they were given, not the order the database returned them', async () => {
    const queue = await h.queueService.createQueue(queueInput());
    const inputs = Array.from({ length: 25 }, (_, i) =>
      jobInput(queue.id, { payload: JSON.stringify({ n: i }) })
    );

    const jobs = await h.jobService.publishJobs(inputs);

    expect(jobs.map((j) => j.idempotencyKey)).toEqual(inputs.map((i) => i.idempotencyKey));
    expect(jobs.map((j) => JSON.parse(j.payload).n)).toEqual(inputs.map((_, i) => i));
  });

  it('is not bounded by the 65535-parameter ceiling a VALUES list would hit', async () => {
    const queue = await h.queueService.createQueue(queueInput());
    // 10_000 jobs is 70_000 bind parameters as a generated VALUES list; as seven
    // arrays through unnest it stays at seven.
    const inputs = Array.from({ length: 10_000 }, (_, i) =>
      jobInput(queue.id, { payload: JSON.stringify({ n: i }) })
    );

    const exec = countingExecutor();
    const jobs = await h.jobService.publishJobs(inputs, exec);

    expect(exec.statements).toHaveLength(1);
    expect(jobs).toHaveLength(10_000);
    expect(await countByStatus(h.pool, queue.id)).toMatchObject({ PENDING: 10_000 });
  });

  it('issues no statement at all for an empty batch', async () => {
    const exec = countingExecutor();
    expect(await h.jobService.publishJobs([], exec)).toEqual([]);
    expect(exec.statements).toHaveLength(0);
  });

  it('seeds one group-limit row per (queue, group), with the declared cap', async () => {
    const queue = await h.queueService.createQueue(queueInput({ concurrency: 4 }));
    const groupA = uniqueName('grp-a');
    const groupB = uniqueName('grp-b');

    await h.jobService.publishJobs([
      jobInput(queue.id, { group: { id: groupA, concurrency: 2 } }),
      jobInput(queue.id, { group: { id: groupB, concurrency: 3 } }),
      jobInput(queue.id, { group: { id: groupA, concurrency: 2 } }),
      jobInput(queue.id, { group: { id: groupA, concurrency: 2 } }),
    ]);

    const counters = await readGroupCounters(h.pool, queue.id);
    expect(counters).toEqual([
      { groupId: groupA, maxRunning: 2, running: 0 },
      { groupId: groupB, maxRunning: 3, running: 0 },
    ].sort((x, y) => (x.groupId < y.groupId ? -1 : 1)));
  });

  it('leaves an existing group cap alone rather than reseeding it', async () => {
    const queue = await h.queueService.createQueue(queueInput({ concurrency: 4 }));
    const group = uniqueName('grp');

    await h.jobService.publishJob(jobInput(queue.id, { group: { id: group, concurrency: 1 } }));
    await h.jobService.publishJobs([
      jobInput(queue.id, { group: { id: group, concurrency: 9 } }),
      jobInput(queue.id, { group: { id: group, concurrency: 9 } }),
    ]);

    // The cap belongs to whoever seeded the row first — ON CONFLICT DO NOTHING,
    // same as the single-job path.
    expect(await readGroupCounters(h.pool, queue.id)).toEqual([
      { groupId: group, maxRunning: 1, running: 0 },
    ]);
  });

  it('spreads one batch across different queues', async () => {
    const first = await h.queueService.createQueue(queueInput());
    const second = await h.queueService.createQueue(queueInput());

    const jobs = await h.jobService.publishJobs([
      jobInput(first.id),
      jobInput(second.id),
      jobInput(first.id),
    ]);

    expect(jobs.map((j) => j.queueId)).toEqual([first.id, second.id, first.id]);
    expect(await countByStatus(h.pool, first.id)).toMatchObject({ PENDING: 2 });
    expect(await countByStatus(h.pool, second.id)).toMatchObject({ PENDING: 1 });
  });

  it('rejects a duplicate idempotency key inside the batch before touching the database', async () => {
    const queue = await h.queueService.createQueue(queueInput());
    const shared = uniqueName('dup');
    const inputs: CreateJobInput[] = [
      jobInput(queue.id, { idempotencyKey: shared }),
      jobInput(queue.id),
      jobInput(queue.id, { idempotencyKey: shared }),
    ];

    const exec = countingExecutor();
    await expect(h.jobService.publishJobs(inputs, exec)).rejects.toBeInstanceOf(ConflictError);
    await expect(h.jobService.publishJobs(inputs, exec)).rejects.toThrow(shared);

    expect(exec.statements).toHaveLength(0);
    expect(await countByStatus(h.pool, queue.id)).toMatchObject({ PENDING: 0 });
  });

  it('discards the whole batch when one key already exists', async () => {
    const queue = await h.queueService.createQueue(queueInput());
    const taken = await h.jobService.publishJob(jobInput(queue.id));

    await expect(
      h.jobService.publishJobs([
        jobInput(queue.id),
        jobInput(queue.id, { idempotencyKey: taken.idempotencyKey }),
        jobInput(queue.id),
      ])
    ).rejects.toThrow();

    // One statement is one transaction: the two valid jobs must not have landed.
    expect(await countByStatus(h.pool, queue.id)).toMatchObject({ PENDING: 1 });
  });

  it('commits with the caller transaction, and vanishes with it on rollback', async () => {
    const queue = await h.queueService.createQueue(queueInput({ concurrency: 4 }));
    const group = uniqueName('grp');
    let tx: PoolClient = await h.pool.connect();

    try {
      await tx.query('BEGIN');
      const jobs = await h.jobService.publishJobs(
        [
          jobInput(queue.id, { group: { id: group, concurrency: 2 } }),
          jobInput(queue.id, { group: { id: group, concurrency: 2 } }),
        ],
        tx
      );
      await tx.query('ROLLBACK');

      for (const job of jobs) {
        expect(await readJobRow(h.pool, job.id)).toBeNull();
      }
      expect(await readGroupCounters(h.pool, queue.id)).toHaveLength(0);
    } finally {
      tx.release();
    }

    tx = await h.pool.connect();
    try {
      await tx.query('BEGIN');
      const jobs = await h.jobService.publishJobs([jobInput(queue.id), jobInput(queue.id)], tx);
      await tx.query('COMMIT');

      for (const job of jobs) {
        expect(await readJobRow(h.pool, job.id)).toMatchObject({ status: JobStatus.PENDING });
      }
    } finally {
      tx.release();
    }
  });

  it('publishes jobs a worker can actually pull', async () => {
    const queue = await h.queueService.createQueue(queueInput());
    const published = await h.jobService.publishJobs([jobInput(queue.id), jobInput(queue.id)]);

    const keys = new Set<string>();
    for (let i = 0; i < 2; i++) {
      const pulled = await h.jobRepo.pullJob(queue);
      expect(pulled).not.toBeNull();
      keys.add(pulled!.idempotencyKey);
    }

    expect(keys).toEqual(new Set(published.map((j) => j.idempotencyKey)));
    expect(await h.jobRepo.pullJob(queue)).toBeNull();
  });
});
