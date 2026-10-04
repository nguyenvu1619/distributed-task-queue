import { PoolClient } from 'pg';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

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

  it('inserts a key repeated inside the batch once, and the FIRST occurrence is the fresh one', async () => {
    const queue = await h.queueService.createQueue(queueInput());
    const shared = uniqueName('dup');
    const inputs: CreateJobInput[] = [
      jobInput(queue.id, { idempotencyKey: shared, payload: 'occurrence-1' }),
      jobInput(queue.id),
      jobInput(queue.id, { idempotencyKey: shared, payload: 'occurrence-3' }),
    ];

    const results = await h.jobService.publishJobs(inputs);

    // Which occurrence wins is recorded here rather than left incidental: the
    // first one in input order owns the insert, matching what ON CONFLICT
    // DO NOTHING does with a repeated key inside one statement.
    expect(results.map((r) => r.deduplicated)).toEqual([false, false, true]);
    expect(results[2].id).toBe(results[0].id);
    expect(results[0].payload).toBe('occurrence-1');
    expect(results[2].payload).toBe('occurrence-1');

    expect(await countByStatus(h.pool, queue.id)).toMatchObject({ PENDING: 2 });
  });

  it('deduplicates against keys already in the table and still lands the rest', async () => {
    const queue = await h.queueService.createQueue(queueInput());
    const taken = await h.jobService.publishJob(jobInput(queue.id));

    const results = await h.jobService.publishJobs([
      jobInput(queue.id),
      jobInput(queue.id, { idempotencyKey: taken.idempotencyKey }),
      jobInput(queue.id),
    ]);

    expect(results.map((r) => r.deduplicated)).toEqual([false, true, false]);
    expect(results[1].id).toBe(taken.id);
    // The two fresh jobs land — a duplicate no longer takes the batch with it.
    expect(await countByStatus(h.pool, queue.id)).toMatchObject({ PENDING: 3 });
  });

  it('keeps the same key independent on different queues', async () => {
    const first = await h.queueService.createQueue(queueInput());
    const second = await h.queueService.createQueue(queueInput());
    const shared = uniqueName('order');

    const results = await h.jobService.publishJobs([
      jobInput(first.id, { idempotencyKey: shared, payload: 'email' }),
      jobInput(second.id, { idempotencyKey: shared, payload: 'invoice' }),
    ]);

    // Uniqueness is scoped to the queue: one business id backing a job on two
    // queues is the point, not a collision.
    expect(results.map((r) => r.deduplicated)).toEqual([false, false]);
    expect(results[0].id).not.toBe(results[1].id);
    expect(results.map((r) => r.queueId)).toEqual([first.id, second.id]);
    expect(await countByStatus(h.pool, first.id)).toMatchObject({ PENDING: 1 });
    expect(await countByStatus(h.pool, second.id)).toMatchObject({ PENDING: 1 });
  });

  it('frees a key once its job settles', async () => {
    const queue = await h.queueService.createQueue(queueInput());
    const key = uniqueName('reuse');

    const first = await h.jobService.publishJob(jobInput(queue.id, { idempotencyKey: key }));
    const pulled = await h.jobRepo.pullJob(queue);
    await h.jobRepo.completeJob(pulled!.id, pulled!.lockSeq, queue);

    // Terminal jobs are deleted, so the key is no longer held by anything live.
    const again = await h.jobService.publishJob(jobInput(queue.id, { idempotencyKey: key }));
    expect(again.deduplicated).toBe(false);
    expect(again.id).not.toBe(first.id);
  });

  it('resolves a duplicate in one statement, leaving no window for a read-back race', async () => {
    const queue = await h.queueService.createQueue(queueInput());
    const taken = await h.jobService.publishJob(jobInput(queue.id));

    const exec = countingExecutor();
    const [result] = await h.jobService.publishJobs(
      [jobInput(queue.id, { idempotencyKey: taken.idempotencyKey })],
      exec
    );

    // One statement is the whole point: a second one to ask who holds the key
    // could find that job already settled and deleted. There is no second one.
    expect(exec.statements).toHaveLength(1);
    expect(result.deduplicated).toBe(true);
    expect(result.id).toBe(taken.id);
  });

  it('leaves the lease of a job a worker is holding alone', async () => {
    const queue = await h.queueService.createQueue(queueInput());
    const published = await h.jobService.publishJob(jobInput(queue.id));
    const leased = await h.jobRepo.pullJob(queue);
    const before = await readJobRow(h.pool, leased!.id);

    // A duplicate publish has to write the existing row for RETURNING to give it
    // back. It writes `revision` and nothing else: clobbering status, attempts
    // or lease_seq would break the worker mid-flight.
    const dup = await h.jobService.publishJob(
      jobInput(queue.id, { idempotencyKey: published.idempotencyKey, attempts: 99 })
    );

    expect(dup.deduplicated).toBe(true);
    expect(await readJobRow(h.pool, leased!.id)).toEqual(before);

    // And the worker can still settle it — the settle is accepted (no
    // LeaseLostError from a clobbered lease_seq) and the row is gone.
    await h.jobRepo.completeJob(leased!.id, leased!.lockSeq, queue);
    expect(await readJobRow(h.pool, leased!.id)).toBeNull();
  });

  it('counts republishes in revision, starting from zero on a fresh insert', async () => {
    const queue = await h.queueService.createQueue(queueInput());
    const key = uniqueName('rev');
    const revision = async () =>
      Number(
        (
          await h.pool.query('SELECT revision FROM jobs WHERE queue_id = $1 AND idempotency_key = $2', [
            queue.id,
            key,
          ])
        ).rows[0].revision
      );

    const first = await h.jobService.publishJob(jobInput(queue.id, { idempotencyKey: key }));
    expect(first.deduplicated).toBe(false);
    expect(await revision()).toBe(0);

    for (const expected of [1, 2, 3]) {
      const again = await h.jobService.publishJob(jobInput(queue.id, { idempotencyKey: key }));
      expect(again.deduplicated).toBe(true);
      expect(again.id).toBe(first.id);
      expect(await revision()).toBe(expected);
    }

    // A batch counts once per distinct key, not once per occurrence: repeats are
    // collapsed before the statement runs.
    await h.jobService.publishJobs([
      jobInput(queue.id, { idempotencyKey: key }),
      jobInput(queue.id, { idempotencyKey: key }),
    ]);
    expect(await revision()).toBe(4);
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
