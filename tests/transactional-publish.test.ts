import { PoolClient, types } from 'pg';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';

import { Executor } from '../src/domain/executor';
import { JobStatus } from '../src/domain/job';
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

/**
 * Stands in for the caller's business table — the writes a job is supposed to
 * commit alongside. Created once, outside any transaction: PostgreSQL rolls DDL
 * back with everything else, so a CREATE inside a case under test would vanish
 * along with the rows it was meant to hold.
 */
const ORDERS = 'transactional_publish_orders';

beforeAll(async () => {
  const setup = createHarness({ maxConnections: 2 });
  await setup.pool.query(`CREATE TABLE IF NOT EXISTS ${ORDERS} (id TEXT PRIMARY KEY)`);
  await setup.close();
});

afterAll(async () => {
  const teardown = createHarness({ maxConnections: 2 });
  await teardown.pool.query(`DROP TABLE IF EXISTS ${ORDERS}`);
  await teardown.close();
});

// A fresh harness per test: QueueRepository memoises queues in-process, so a
// reused instance would keep serving rows that TRUNCATE has already removed.
beforeEach(async () => {
  h = createHarness({ maxConnections: 10 });
  await resetDatabase(h.pool);
  await h.pool.query(`TRUNCATE ${ORDERS}`);
});

afterEach(async () => {
  await h.close();
});

/** Runs `fn` on a dedicated client inside BEGIN, then commits or rolls back. */
async function inTransaction<T>(
  outcome: 'commit' | 'rollback',
  fn: (tx: PoolClient) => Promise<T>
): Promise<T> {
  const tx = await h.pool.connect();
  try {
    await tx.query('BEGIN');
    const value = await fn(tx);
    await tx.query(outcome === 'commit' ? 'COMMIT' : 'ROLLBACK');
    return value;
  } catch (err) {
    await tx.query('ROLLBACK');
    throw err;
  } finally {
    tx.release();
  }
}

describe('transactional publish', () => {
  it('commits the job with the business write that justifies it', async () => {
    const queue = await h.queueService.createQueue(queueInput());
    const orderId = uniqueName('order');

    const job = await inTransaction('commit', async (tx) => {
      await tx.query(`INSERT INTO ${ORDERS} (id) VALUES ($1)`, [orderId]);
      return h.jobService.publishJob(jobInput(queue.id, { payload: orderId }), tx);
    });

    const { rows } = await h.pool.query(`SELECT id FROM ${ORDERS} WHERE id = $1`, [orderId]);
    expect(rows).toHaveLength(1);
    expect(await readJobRow(h.pool, job.id)).toMatchObject({ status: JobStatus.PENDING });
  });

  it('drops the job when the caller rolls back — no orphan work for a write that never landed', async () => {
    const queue = await h.queueService.createQueue(queueInput());
    const orderId = uniqueName('order');

    const job = await inTransaction('rollback', async (tx) => {
      await tx.query(`INSERT INTO ${ORDERS} (id) VALUES ($1)`, [orderId]);
      return h.jobService.publishJob(jobInput(queue.id, { payload: orderId }), tx);
    });

    // The id was handed back from inside the transaction, so it existed; what
    // matters is that nothing survived the rollback on either side.
    expect(job.id).toBeGreaterThan(0);
    const { rows } = await h.pool.query(`SELECT id FROM ${ORDERS} WHERE id = $1`, [orderId]);
    expect(rows).toHaveLength(0);
    expect(await readJobRow(h.pool, job.id)).toBeNull();
    expect(await h.jobRepo.pullJob(queue)).toBeNull();
  });

  it('keeps the job invisible to workers until the caller commits', async () => {
    const queue = await h.queueService.createQueue(queueInput());
    const tx = await h.pool.connect();

    try {
      await tx.query('BEGIN');
      const job = await h.jobService.publishJob(jobInput(queue.id), tx);

      // Another session, mid-transaction: the row is simply not there yet.
      expect(await h.jobRepo.pullJob(queue)).toBeNull();

      await tx.query('COMMIT');

      const pulled = await h.jobRepo.pullJob(queue);
      expect(pulled?.id).toBe(job.id);
    } finally {
      tx.release();
    }
  });

  it('rolls back the group-limit seeding with the job it was seeded for', async () => {
    const queue = await h.queueService.createQueue(queueInput({ concurrency: 4 }));
    const group = { id: uniqueName('grp'), concurrency: 2 };

    await inTransaction('rollback', (tx) =>
      h.jobService.publishJob(jobInput(queue.id, { group }), tx)
    );

    // The limit row travels in the same statement as the insert, so a rollback
    // must take both: a stranded limit row would cap a group that has no jobs.
    expect(await readGroupCounters(h.pool, queue.id)).toHaveLength(0);
    expect(await countByStatus(h.pool, queue.id)).toEqual({ PENDING: 0, PROCESSING: 0 });
  });

  it('still commits on its own when no executor is passed', async () => {
    const queue = await h.queueService.createQueue(queueInput());
    const job = await h.jobService.publishJob(jobInput(queue.id));

    expect(await readJobRow(h.pool, job.id)).toMatchObject({ status: JobStatus.PENDING });
  });

  it('deduplicates inside a caller transaction without aborting it', async () => {
    const queue = await h.queueService.createQueue(queueInput());
    const key = uniqueName('dupe');
    const orderId = uniqueName('order');
    const first = await h.jobService.publishJob(jobInput(queue.id, { idempotencyKey: key }));

    const tx = await h.pool.connect();
    try {
      await tx.query('BEGIN');
      await tx.query(`INSERT INTO ${ORDERS} (id) VALUES ($1)`, [orderId]);

      const second = await h.jobService.publishJob(
        jobInput(queue.id, { idempotencyKey: key }),
        tx
      );
      expect(second.deduplicated).toBe(true);
      expect(second.id).toBe(first.id);

      // The whole point of not raising: a unique violation here would have
      // aborted the transaction and taken the order row with it. Prove the
      // transaction is still usable rather than merely un-thrown.
      const probe = await tx.query('SELECT 1 AS ok');
      expect(probe.rows[0].ok).toBe(1);
      await tx.query('COMMIT');
    } finally {
      tx.release();
    }

    const { rows } = await h.pool.query(`SELECT id FROM ${ORDERS} WHERE id = $1`, [orderId]);
    expect(rows).toHaveLength(1);
  });

  it('coerces bigint columns even on a client whose pg has no INT8 parser', async () => {
    const queue = await h.queueService.createQueue(queueInput({ concurrency: 4 }));

    // A caller handing us a client from their own copy of pg has not run this
    // package's types.setTypeParser, so BIGINT arrives as a string. Simulated by
    // restoring the driver default for the duration of the publish.
    const restore = types.getTypeParser(types.builtins.INT8, 'text');
    types.setTypeParser(types.builtins.INT8, (value: string) => value);

    let job;
    const tx = await h.pool.connect();
    try {
      await tx.query('BEGIN');
      job = await h.jobService.publishJob(jobInput(queue.id), tx);
      await tx.query('COMMIT');
    } finally {
      tx.release();
      types.setTypeParser(types.builtins.INT8, restore);
    }

    // lockSeq feeds `lease_seq + 1` in the fencing path; a string there turns
    // that arithmetic into concatenation.
    expect(typeof job.id).toBe('number');
    expect(typeof job.queueId).toBe('number');
    expect(job.queueId).toBe(queue.id);
    expect(typeof job.lockSeq).toBe('number');
  });

  it('accepts any handle that can run a query, not just a pg client', async () => {
    const queue = await h.queueService.createQueue(queueInput());
    const seen: string[] = [];

    // The structural Executor is the point: a Knex/Kysely/Drizzle transaction
    // reaches this repository through an adapter this thin.
    const adapter: Executor = {
      query: async (text, values) => {
        seen.push(text);
        return h.pool.query(text, values);
      },
    };

    const job = await h.jobService.publishJob(jobInput(queue.id), adapter);

    expect(seen).toHaveLength(1);
    expect(await readJobRow(h.pool, job.id)).toMatchObject({ status: JobStatus.PENDING });
  });
});
