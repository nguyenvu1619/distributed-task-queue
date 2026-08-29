/**
 * Harness for tests that drive the public `TaskQueue` facade, as opposed to
 * `harness.ts`, which wires the repositories and services up directly.
 *
 * The pool is owned here and the TaskQueue *borrows* it, so a test can still
 * query the database after `tq.close()` — which is exactly what assertions
 * about shutdown need to do.
 */
import { Pool, PoolClient } from 'pg';

import { TaskQueue } from '../../src/client/task-queue';
import { Worker } from '../../src/client/queue-handle';
import { TaskHandler } from '../../src/client/options';
import { Logger, silentLogger } from '../../src/domain/logger';
import { createPool } from '../../src/repository/postgresql/connection';
import { pgConfig, resetDatabase, sleep, uniqueName, waitFor } from './harness';

export interface TaskQueueHarnessOptions {
  maxConnections?: number;
  /** Defaults to a silent logger so a passing run stays readable. */
  logger?: Logger;
  /** Skip the TRUNCATE — for tests that manage their own fixtures. */
  skipReset?: boolean;
}

export interface JobSnapshot {
  id: number;
  idempotencyKey: string;
  payload: string;
  status: string;
  groupId: string | null;
  attempts: number;
  leaseSeq: number | null;
  leaseExpiresAt: Date | null;
  queueShardNo: number | null;
}

export interface TaskQueueHarness {
  /** Owned by the harness; outlives `tq.close()`. */
  pool: Pool;
  tq: TaskQueue;
  /** Registers a worker so `close()` stops it even if the test throws. */
  track<T extends Worker>(worker: T): T;
  /** A queue name unique to this process and call. */
  name(prefix: string): string;
  jobCount(queueId: number): Promise<number>;
  jobs(queueId: number): Promise<JobSnapshot[]>;
  job(id: number): Promise<JobSnapshot | null>;
  /** Waits until the queue holds no rows at all — every job reached a terminal state. */
  waitForDrain(queueId: number, options?: { timeout?: number }): Promise<void>;
  close(): Promise<void>;
}

export async function createTaskQueueHarness(
  options: TaskQueueHarnessOptions = {}
): Promise<TaskQueueHarness> {
  const pool = createPool(
    pgConfig({
      max: options.maxConnections ?? 10,
      connectionTimeoutMillis: 30_000,
      idleTimeoutMillis: 10_000,
    })
  );

  if (!options.skipReset) {
    // The pool exists but the harness does not yet, so nothing downstream can
    // close it. A TRUNCATE that fails here would otherwise leak the pool and
    // resurface as an unrelated connection error in a later file.
    try {
      await resetDatabase(pool);
    } catch (error) {
      await pool.end().catch(() => undefined);
      throw error;
    }
  }

  const tq = TaskQueue.create({ pool, logger: options.logger ?? silentLogger });
  const workers: Worker[] = [];

  const snapshot = (row: any): JobSnapshot => ({
    id: Number(row.id),
    idempotencyKey: row.idempotency_key,
    payload: row.payload,
    status: row.status,
    groupId: row.group_id,
    attempts: Number(row.attempts),
    leaseSeq: row.lease_seq === null ? null : Number(row.lease_seq),
    leaseExpiresAt: row.lease_expires_at,
    queueShardNo: row.queue_shard_no === null ? null : Number(row.queue_shard_no),
  });

  const JOB_SELECT = `SELECT id, idempotency_key, payload, status, group_id, attempts,
                             lease_seq, lease_expires_at, queue_shard_no
                      FROM jobs`;

  const harness: TaskQueueHarness = {
    pool,
    tq,

    track(worker) {
      workers.push(worker);
      return worker;
    },

    name(prefix) {
      return uniqueName(prefix);
    },

    async jobCount(queueId) {
      const { rows } = await pool.query(
        'SELECT count(*)::int AS n FROM jobs WHERE queue_id = $1',
        [queueId]
      );
      return rows[0].n;
    },

    async jobs(queueId) {
      const { rows } = await pool.query(
        `${JOB_SELECT} WHERE queue_id = $1 ORDER BY id`,
        [queueId]
      );
      return rows.map(snapshot);
    },

    async job(id) {
      const { rows } = await pool.query(`${JOB_SELECT} WHERE id = $1`, [id]);
      return rows[0] ? snapshot(rows[0]) : null;
    },

    async waitForDrain(queueId, waitOptions = {}) {
      await waitFor(async () => (await harness.jobCount(queueId)) === 0, {
        timeout: waitOptions.timeout ?? 20_000,
        message: `queue ${queueId} to drain`,
      });
    },

    async close() {
      // Stop workers first and independently: one wedged worker must not stop
      // the others, or the pool, from being cleaned up.
      await Promise.all(
        workers.map((worker) => worker.stop({ timeout: '5s' }).catch(() => undefined))
      );
      workers.length = 0;
      await tq.close({ timeout: '5s' }).catch(() => undefined);
      await pool.end();
    },
  };

  return harness;
}

// ---------------------------------------------------------------------------
// Business-table fixtures — the "other half" of a transactional publish
// ---------------------------------------------------------------------------

/**
 * A stand-in for the caller's own tables. Transaction tests need a real write
 * to commit or roll back *alongside* the job, otherwise they only prove the job
 * table behaves — which is not the claim being made.
 */
export interface ScratchTable {
  name: string;
  insert(executor: Executorish, id: string, amount?: number): Promise<void>;
  ids(): Promise<string[]>;
  count(): Promise<number>;
  drop(): Promise<void>;
}

interface Executorish {
  query(text: string, values?: any[]): Promise<any>;
}

export async function createScratchTable(
  pool: Pool,
  prefix = 'scratch_orders'
): Promise<ScratchTable> {
  // Unique per call so parallel-ish tests never collide on DDL, and so a
  // leftover table from a crashed run cannot silently satisfy an assertion.
  const name = `${prefix}_${process.pid}_${Math.abs(hash(uniqueName(prefix)))}`;

  await pool.query(
    `CREATE TABLE ${name} (
       id     TEXT PRIMARY KEY,
       amount NUMERIC NOT NULL DEFAULT 0
     )`
  );

  return {
    name,
    async insert(executor, id, amount = 0) {
      await executor.query(`INSERT INTO ${name} (id, amount) VALUES ($1, $2)`, [id, amount]);
    },
    async ids() {
      const { rows } = await pool.query(`SELECT id FROM ${name} ORDER BY id`);
      return rows.map((row) => row.id as string);
    },
    async count() {
      const { rows } = await pool.query(`SELECT count(*)::int AS n FROM ${name}`);
      return rows[0].n as number;
    },
    async drop() {
      await pool.query(`DROP TABLE IF EXISTS ${name}`);
    },
  };
}

function hash(value: string): number {
  let out = 0;
  for (let i = 0; i < value.length; i++) {
    out = (out * 31 + value.charCodeAt(i)) | 0;
  }
  return out;
}

/**
 * Runs `fn` on a client with an open transaction: COMMIT on success, ROLLBACK on
 * error, always.
 *
 * Hand-rolling this is a trap in tests. node-postgres does not roll back on
 * release, so a failed assertion between BEGIN and COMMIT hands the pool back a
 * connection still holding row locks — and the next DDL in teardown then blocks
 * until Postgres reaps it at `idleTimeoutMillis`. A genuine failure ends up
 * looking like a ten-second hang.
 */
export async function inTransaction<R>(
  pool: Pool,
  fn: (tx: PoolClient) => Promise<R>
): Promise<R> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const result = await fn(client);
    await client.query('COMMIT');
    return result;
  } catch (error) {
    await client.query('ROLLBACK').catch(() => undefined);
    throw error;
  } finally {
    client.release();
  }
}

/** Runs `fn` on a client with an open transaction and always rolls back. */
export async function inRolledBackTransaction<R>(
  pool: Pool,
  fn: (tx: PoolClient) => Promise<R>
): Promise<R> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    return await fn(client);
  } finally {
    await client.query('ROLLBACK').catch(() => undefined);
    client.release();
  }
}

// ---------------------------------------------------------------------------
// Handler helpers
// ---------------------------------------------------------------------------

export interface Collector<T> {
  /** Payloads seen, in the order the handler received them. */
  seen: T[];
  /** How many times the handler was entered — including runs that then threw. */
  calls: number;
  handler: TaskHandler<T>;
  /** Resolves once the handler has been entered at least `n` times. */
  waitForCalls(n: number, options?: { timeout?: number }): Promise<void>;
}

/**
 * Builds a recording handler. `behaviour` runs after the payload is recorded,
 * so a throwing handler still shows up in `seen` — tests about retries need to
 * count deliveries, not successes.
 */
export function collect<T>(
  behaviour?: (payload: T, index: number) => Promise<void> | void
): Collector<T> {
  const collector: Collector<T> = {
    seen: [],
    calls: 0,
    handler: async (payload, ctx) => {
      const index = collector.calls;
      collector.calls += 1;
      collector.seen.push(payload);
      if (behaviour) {
        await behaviour(payload, index);
      }
      void ctx;
    },
    waitForCalls(n, waitOptions = {}) {
      return waitFor(() => collector.calls >= n, {
        timeout: waitOptions.timeout ?? 20_000,
        message: `${n} handler call(s) (saw ${collector.calls})`,
      });
    },
  };
  return collector;
}

export { sleep, uniqueName, waitFor };
