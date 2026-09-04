/**
 * Transactional publish — the headline feature, and the reason to put the queue
 * in the same database as the data it is about.
 *
 * Every test here commits or rolls back a *real* business write alongside the
 * job (via `createScratchTable`). A test that only watched the `jobs` table
 * would prove that publishing works, not that it is atomic with the caller's
 * own writes, which is the actual claim.
 *
 * Where a test asserts something that is arguably wrong — the first-use
 * self-deadlock, the NUL in metadata — the comment says so and names the fix.
 * Pinning the current behaviour is what makes a change to it visible.
 */
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { Pool, PoolClient } from 'pg';

import { TaskQueue } from '../src/client/task-queue';
import { QueueHandle } from '../src/client/queue-handle';
import { ErrorCodes } from '../src/domain/errors';
import { Executor } from '../src/domain/executor';
import { silentLogger } from '../src/domain/logger';
import { createPool } from '../src/repository/postgresql/connection';
import { JobRepository } from '../src/repository/postgresql/job.repository';
import { pgConfig } from './support/harness';
import { readGroupCounters } from './support/invariants';
import {
  ScratchTable,
  TaskQueueHarness,
  collect,
  createScratchTable,
  createTaskQueueHarness,
  inRolledBackTransaction,
  inTransaction,
  sleep,
  uniqueName,
} from './support/task-queue';

interface Confirmation {
  orderId: string;
}

// Undefined between tests on purpose. If `beforeEach` throws part-way, teardown
// must not close the *previous* test's harness — "Called end on pool more than
// once" would then be the only error anyone ever sees.
let h: TaskQueueHarness | undefined;
let orders: ScratchTable | undefined;

beforeEach(async () => {
  h = await createTaskQueueHarness();
  orders = await createScratchTable(h.pool, 'tx_orders');
});

afterEach(async () => {
  const currentOrders = orders;
  const current = h;
  orders = undefined;
  h = undefined;

  // The scratch table is outside the migrated schema, so the next test's
  // TRUNCATE would not clean it up — drop it while the pool is still open, and
  // say so if that fails: a swallowed failure here leaks a table for the rest
  // of the run.
  if (currentOrders) {
    await currentOrders.drop().catch((error) => {
      console.error(`failed to drop scratch table ${currentOrders.name}:`, error);
    });
  }
  await current?.close();
});

/**
 * A queue plus its resolved id.
 *
 * Resolving up front matters. `publish` creates the queue on first use over a
 * connection it checks out of the pool itself — not over the caller's `tx` — so
 * a first-use publish inside a transaction needs a *second* free connection.
 * Pre-resolving keeps most tests below about the publish rather than about
 * queue creation; the "first use inside the transaction" suite is the one that
 * deliberately stays on that edge.
 */
async function confirmationQueue(
  prefix = 'confirmations'
): Promise<{ handle: QueueHandle<Confirmation>; queueId: number }> {
  const handle = h!.tq.defineQueue<Confirmation>(h!.name(prefix));
  return { handle, queueId: await handle.id() };
}

/** Counts the statements a publish runs on the executor it was handed. */
function countingExecutor(target: Executor): { executor: Executor; calls: () => number } {
  let calls = 0;
  return {
    executor: {
      query: (text, values) => {
        calls += 1;
        return target.query(text, values);
      },
    },
    calls: () => calls,
  };
}

describe('tq.transaction()', () => {
  it('commits the business row and the job together', async () => {
    const { handle, queueId } = await confirmationQueue();

    const published = await h!.tq.transaction(async (tx) => {
      await orders!.insert(tx, 'order-1', 42);
      const job = await handle.publish({ orderId: 'order-1' }, { tx });

      // Mid-transaction, from another session: neither row may exist yet. Both
      // post-commit assertions below hold just as well for a publish that never
      // joined the transaction, so this is the half that has teeth.
      expect(await h!.jobCount(queueId)).toBe(0);
      expect(await orders!.count()).toBe(0);
      return job;
    });

    expect(published.deduplicated).toBe(false);
    expect(await orders!.ids()).toEqual(['order-1']);

    const jobs = await h!.jobs(queueId);
    expect(jobs).toHaveLength(1);
    expect(jobs[0].id).toBe(published.id);
    expect(JSON.parse(jobs[0].payload)).toEqual({ orderId: 'order-1' });
    expect(jobs[0].status).toBe('PENDING');
  });

  it('returns the callback result to the caller', async () => {
    const { handle } = await confirmationQueue();

    const result = await h!.tq.transaction(async (tx) => {
      await orders!.insert(tx, 'order-2');
      await handle.publish({ orderId: 'order-2' }, { tx });
      return { receipt: 'ok' as const };
    });

    expect(result).toEqual({ receipt: 'ok' });
  });

  it('persists neither the business row nor the job when the callback throws, and rethrows that same error', async () => {
    const { handle, queueId } = await confirmationQueue();

    class PaymentDeclined extends Error {}
    const declined = new PaymentDeclined('payment declined');

    // Identity, not just message: the facade must not wrap or replace the
    // caller's error on its way out of the rollback.
    await expect(
      h!.tq.transaction(async (tx) => {
        await orders!.insert(tx, 'order-3');
        await handle.publish({ orderId: 'order-3' }, { tx });
        throw declined;
      })
    ).rejects.toBe(declined);

    expect(await orders!.count()).toBe(0);
    expect(await h!.jobCount(queueId)).toBe(0);
  });

  it('persists nothing when a business statement after the publish fails', async () => {
    const { handle, queueId } = await confirmationQueue();

    // The publish itself succeeds and is sandwiched between two business
    // writes; the second one violates the primary key. Everything must go.
    await expect(
      h!.tq.transaction(async (tx) => {
        await orders!.insert(tx, 'order-4');
        await handle.publish({ orderId: 'order-4' }, { tx });
        await orders!.insert(tx, 'order-4');
      })
    ).rejects.toThrow(/duplicate key/i);

    expect(await orders!.count()).toBe(0);
    expect(await h!.jobCount(queueId)).toBe(0);
  });

  it('returns the connection to the pool after a failed transaction', async () => {
    const { handle, queueId } = await confirmationQueue();

    // More failures than the pool has connections (max 10): a client leaked on
    // the error path would exhaust the pool long before the loop ends.
    for (let i = 0; i < 15; i++) {
      await expect(
        h!.tq.transaction(async (tx) => {
          await orders!.insert(tx, `doomed-${i}`);
          await handle.publish({ orderId: `doomed-${i}` }, { tx });
          throw new Error('boom');
        })
      ).rejects.toThrow('boom');
    }

    await h!.tq.transaction(async (tx) => {
      await orders!.insert(tx, 'survivor');
      await handle.publish({ orderId: 'survivor' }, { tx });
    });

    expect(await orders!.ids()).toEqual(['survivor']);
    expect(await h!.jobCount(queueId)).toBe(1);
  });
});

describe('isolation from other sessions', () => {
  it('hides both rows from another session until the transaction commits', async () => {
    const { handle, queueId } = await confirmationQueue();

    await h!.tq.transaction(async (tx) => {
      await orders!.insert(tx, 'order-5');
      await handle.publish({ orderId: 'order-5' }, { tx });

      // `orders.count()` and `h.jobCount()` both run on the harness pool, i.e.
      // a different session than `tx` — neither may see uncommitted work.
      expect(await orders!.count()).toBe(0);
      expect(await h!.jobCount(queueId)).toBe(0);
    });

    expect(await orders!.count()).toBe(1);
    expect(await h!.jobCount(queueId)).toBe(1);
  });

  it('does not let a worker running throughout consume the job before the commit', async () => {
    const { handle, queueId } = await confirmationQueue();

    const collector = collect<Confirmation>();
    h!.track(await handle.work(collector.handler, { pollInterval: '10ms' }));

    await h!.tq.transaction(async (tx) => {
      await orders!.insert(tx, 'order-6');
      await handle.publish({ orderId: 'order-6' }, { tx });

      // Proving a negative needs real time: 300ms at a 10ms poll interval gives
      // the worker ~30 chances to wrongly pick up the uncommitted job.
      await sleep(300);
      expect(collector.calls).toBe(0);
    });

    await collector.waitForCalls(1);
    expect(collector.seen).toEqual([{ orderId: 'order-6' }]);
    await h!.waitForDrain(queueId);
  });
});

describe('a transaction the caller owns', () => {
  it('joins a BEGIN/COMMIT the caller issued itself', async () => {
    const { handle, queueId } = await confirmationQueue();

    const published = await inTransaction(h!.pool, async (client) => {
      await orders!.insert(client, 'order-7');
      const job = await handle.publish({ orderId: 'order-7' }, { tx: client });

      // Still invisible elsewhere — the library added no COMMIT of its own.
      expect(await h!.jobCount(queueId)).toBe(0);
      return job;
    });

    expect(await h!.job(published.id)).not.toBeNull();
    expect(await orders!.ids()).toEqual(['order-7']);
    expect(await h!.jobCount(queueId)).toBe(1);
  });

  it('discards the job when the caller issues an explicit ROLLBACK', async () => {
    const { handle, queueId } = await confirmationQueue();

    await inRolledBackTransaction(h!.pool, async (client) => {
      await orders!.insert(client, 'order-8');
      await handle.publish({ orderId: 'order-8' }, { tx: client });
    });

    expect(await orders!.count()).toBe(0);
    expect(await h!.jobCount(queueId)).toBe(0);
  });

  it('accepts a client checked out from a completely separate pool', async () => {
    const { handle, queueId } = await confirmationQueue();

    // An application that already has its own pool — the common case — should
    // not have to hand it to the library or borrow the library's.
    const foreign: Pool = createPool(pgConfig({ max: 2 }));
    try {
      const published = await inTransaction(foreign, async (client) => {
        await orders!.insert(client, 'order-9');
        const job = await handle.publish({ orderId: 'order-9' }, { tx: client });

        // id / queue_id are BIGINT, and callers type them as `number`. Note
        // what this does *not* establish: `connection.ts` registers the INT8
        // parser process-globally, and this "foreign" pool comes from the same
        // copy of `pg`, so these hold even with the defensive coercion in
        // `deserializeJob` removed. producer.test.ts owns that coercion.
        expect(typeof job.id).toBe('number');
        expect(Number.isInteger(job.id)).toBe(true);
        expect(typeof job.queueId).toBe('number');
        expect(job.queueId).toBe(queueId);
        return job;
      });

      expect((await h!.job(published.id))?.id).toBe(published.id);
    } finally {
      await foreign.end();
    }

    expect(await orders!.ids()).toEqual(['order-9']);
    expect(await h!.jobCount(queueId)).toBe(1);
  });

  it('joins a transaction reached through a hand-written Executor adapter', async () => {
    const { handle, queueId } = await confirmationQueue();

    await inTransaction(h!.pool, async (client) => {
      // What an ORM user writes: not the PoolClient, but a thin object that
      // forwards to whatever their transaction handle happens to be.
      const adapter: Executor = {
        query: (text, values) => client.query(text, values),
      };

      await orders!.insert(adapter, 'order-10');
      const published = await handle.publish({ orderId: 'order-10' }, { tx: adapter });
      expect(published.deduplicated).toBe(false);

      expect(await h!.jobCount(queueId)).toBe(0);
    });

    expect(await orders!.ids()).toEqual(['order-10']);
    expect(await h!.jobCount(queueId)).toBe(1);
  });

  it('accepts an adapter that rebuilds the query result by hand', async () => {
    const { handle, queueId } = await confirmationQueue();

    let calls = 0;
    await inTransaction(h!.pool, async (client) => {
      // A driver wrapper rarely returns pg's own QueryResult. Only `rows` and
      // `rowCount` are part of the Executor contract, so an adapter that
      // supplies exactly those — and nothing else — has to work. `rowCount:
      // null` is what the README's Knex snippet hands back, so a future
      // `rowCount === 0` shortcut in `insertJobs` must not rely on a number.
      const adapter: Executor = {
        async query(text, values) {
          calls += 1;
          const result = await client.query(text, values);
          return {
            rows: result.rows.map((row) => ({ ...row })),
            rowCount: null,
          };
        },
      };

      await orders!.insert(adapter, 'order-11');
      const published = await handle.publish({ orderId: 'order-11' }, { tx: adapter });
      expect(published.id).toBeGreaterThan(0);
      expect(JSON.parse(published.payload)).toEqual({ orderId: 'order-11' });
    });

    // The insert and the publish both went through the adapter, so the library
    // never reached around it to a pooled connection of its own.
    expect(calls).toBe(2);
    expect(await orders!.ids()).toEqual(['order-11']);
    expect(await h!.jobCount(queueId)).toBe(1);
  });
});

describe('a queue used for the first time inside the transaction', () => {
  it('creates the queue and still commits the job with the business row', async () => {
    // No `await handle.id()` first: `defineQueue` at module scope and the very
    // first publish inside a request transaction is the natural way to write
    // this, and it takes the queue-creation path the other suites avoid.
    const handle = h!.tq.defineQueue<Confirmation>(h!.name('first-use'));

    const published = await h!.tq.transaction(async (tx) => {
      await orders!.insert(tx, 'order-fu1');
      return handle.publish({ orderId: 'order-fu1' }, { tx });
    });

    const queueId = await handle.id();
    expect(published.queueId).toBe(queueId);
    expect(await orders!.ids()).toEqual(['order-fu1']);
    expect(await h!.jobCount(queueId)).toBe(1);
  });

  it('keeps the queue row after the caller rolls back, but not the job', async () => {
    const name = h!.name('first-use-rollback');
    const handle = h!.tq.defineQueue<Confirmation>(name);

    await expect(
      h!.tq.transaction(async (tx) => {
        await orders!.insert(tx, 'order-fu2');
        await handle.publish({ orderId: 'order-fu2' }, { tx });
        throw new Error('checkout abandoned');
      })
    ).rejects.toThrow('checkout abandoned');

    expect(await orders!.count()).toBe(0);
    expect(await h!.jobCount(await handle.id())).toBe(0);

    // Deliberate asymmetry: queue creation runs on a connection of the
    // library's own — it has to, because it issues SET TRANSACTION ISOLATION
    // LEVEL, which Postgres rejects once the caller's transaction has run a
    // statement — so the queue outlives the rollback and the handle stays
    // resolved. Only the job is transactional.
    const { rows } = await h!.pool.query('SELECT 1 FROM queues WHERE name = $1', [name]);
    expect(rows).toHaveLength(1);
    expect(await handle.id()).toBeGreaterThan(0);
  });

  it('starves itself of connections against a pool of one, failing on the connection timeout', async () => {
    // KNOWN DEFECT, pinned as it stands rather than fixed here.
    //
    // Resolving the queue checks out a *second* connection while the caller's
    // transaction is holding the first. Given a single-connection pool nothing
    // can release it, so the publish waits until the checkout times out. This
    // is not a contrived setup: it bites every request on a max:1 serverless /
    // pgbouncer deployment, and any pool momentarily saturated by concurrent
    // first-use transactions.
    //
    // The fix is for `QueueHandle.resolve()` to run on `options.tx` when one is
    // supplied (or for the publish to fail fast with a typed error naming the
    // real problem, instead of surfacing as a generic pool timeout).
    //
    // Small pool + short connectionTimeoutMillis so a regression cannot turn
    // this into a hung suite.
    const tiny = createPool(pgConfig({ max: 1, connectionTimeoutMillis: 1000 }));
    try {
      const tq = TaskQueue.create({ pool: tiny, logger: silentLogger });
      const handle = tq.defineQueue<Confirmation>(h!.name('starved'));

      const startedAt = Date.now();
      await expect(
        tq.transaction(async (tx) => handle.publish({ orderId: 'order-fu3' }, { tx }))
      ).rejects.toThrow(/timeout exceeded when trying to connect/i);
      expect(Date.now() - startedAt).toBeLessThan(10_000);

      // The queue was never created, so nothing about the failure is partial.
      const { rows } = await h!.pool.query('SELECT 1 FROM queues WHERE name = $1', [handle.name]);
      expect(rows).toHaveLength(0);

      await tq.close({ timeout: '1s' });
    } finally {
      // `tq` borrowed this pool, so `close()` leaves it open — end it here or
      // the run keeps a connection (and a timer) alive.
      await tiny.end();
    }
  });
});

describe('publishMany inside a transaction', () => {
  it('commits the whole batch alongside the business row', async () => {
    const { handle, queueId } = await confirmationQueue();

    const published = await h!.tq.transaction(async (tx) => {
      await orders!.insert(tx, 'order-12');
      const batch = await handle.publishMany(
        [{ orderId: 'order-12' }, { orderId: 'order-12-b' }, { orderId: 'order-12-c' }],
        { tx }
      );

      // Nothing of the batch may be visible to another session yet — the
      // post-commit counts below would look identical if it had autocommitted.
      expect(await h!.jobCount(queueId)).toBe(0);
      return batch;
    });

    expect(published).toHaveLength(3);
    expect(published.every((job) => !job.deduplicated)).toBe(true);

    const jobs = await h!.jobs(queueId);
    expect(jobs).toHaveLength(3);
    expect(jobs.map((job) => JSON.parse(job.payload).orderId)).toEqual([
      'order-12',
      'order-12-b',
      'order-12-c',
    ]);
    expect(await orders!.ids()).toEqual(['order-12']);
  });

  it('discards the whole batch when the transaction rolls back', async () => {
    const { handle, queueId } = await confirmationQueue();

    await expect(
      h!.tq.transaction(async (tx) => {
        await orders!.insert(tx, 'order-13');
        await handle.publishMany(
          [{ orderId: 'a' }, { orderId: 'b' }, { orderId: 'c' }],
          { tx }
        );
        throw new Error('shipment rejected');
      })
    ).rejects.toThrow('shipment rejected');

    // All-or-nothing spans the batch and the business row alike: no partial
    // batch may survive.
    expect(await h!.jobCount(queueId)).toBe(0);
    expect(await orders!.count()).toBe(0);
  });
});

/**
 * A grouped publish writes a *second* statement — the `group_queue_limits`
 * upsert — and it has to go through the caller's executor too. A regression
 * sending it to the pool instead would commit a job whose limit row was rolled
 * away, and such a job is never delivered and never errors: the queue simply
 * goes quiet.
 *
 * `requiresGroupId` also puts these queues on the coordination pull, so this is
 * the only place where "published in a transaction, consumed by a worker" is
 * proven off the fast path.
 */
describe('grouped publish inside a transaction', () => {
  async function groupedQueue(
    prefix = 'grouped-confirmations'
  ): Promise<{ handle: QueueHandle<Confirmation>; queueId: number }> {
    const handle = h!.tq.defineQueue<Confirmation>(h!.name(prefix), { requiresGroupId: true });
    return { handle, queueId: await handle.id() };
  }

  it('commits the job and its group limit row together', async () => {
    const { handle, queueId } = await groupedQueue();

    const published = await h!.tq.transaction(async (tx) => {
      await orders!.insert(tx, 'order-g1');
      return handle.publish(
        { orderId: 'order-g1' },
        { tx, group: { id: 'tenant-1', concurrency: 2 } }
      );
    });

    expect((await h!.job(published.id))?.groupId).toBe('tenant-1');
    expect(await readGroupCounters(h!.pool, queueId)).toEqual([
      { groupId: 'tenant-1', maxRunning: 2, running: 0 },
    ]);
    expect(await orders!.ids()).toEqual(['order-g1']);
  });

  it('rolls the group limit row back with the job', async () => {
    const { handle, queueId } = await groupedQueue();

    await expect(
      h!.tq.transaction(async (tx) => {
        await orders!.insert(tx, 'order-g2');
        await handle.publish(
          { orderId: 'order-g2' },
          { tx, group: { id: 'tenant-2', concurrency: 3 } }
        );
        throw new Error('tenant suspended');
      })
    ).rejects.toThrow('tenant suspended');

    expect(await h!.jobCount(queueId)).toBe(0);
    expect(await orders!.count()).toBe(0);
    // The limit row is the half most likely to escape: it is a separate
    // statement, written after the insert.
    expect(await readGroupCounters(h!.pool, queueId)).toEqual([]);
  });

  it('hides the group limit row from another session until the commit', async () => {
    const { handle, queueId } = await groupedQueue();

    await h!.tq.transaction(async (tx) => {
      await orders!.insert(tx, 'order-g3');
      await handle.publish(
        { orderId: 'order-g3' },
        { tx, group: { id: 'tenant-3', concurrency: 1 } }
      );

      // Read on the harness pool, a different session: a limit row already
      // visible here would have been written outside the caller's transaction.
      expect(await readGroupCounters(h!.pool, queueId)).toEqual([]);
    });

    expect(await readGroupCounters(h!.pool, queueId)).toEqual([
      { groupId: 'tenant-3', maxRunning: 1, running: 0 },
    ]);
  });

  it('lets a worker consume a job whose limit row was created inside the transaction', async () => {
    const { handle, queueId } = await groupedQueue();
    const collector = collect<Confirmation>();

    await h!.tq.transaction(async (tx) => {
      await orders!.insert(tx, 'order-g4');
      await handle.publish(
        { orderId: 'order-g4' },
        { tx, group: { id: 'tenant-4', concurrency: 1 } }
      );
    });

    // The coordination pull admits a job only by incrementing its group's
    // `running` under `running < max_running`. No limit row, no delivery — so
    // this is the assertion that a committed job with a rolled-away limit row
    // would fail on, where a plain jobCount would not.
    h!.track(await handle.work(collector.handler, { pollInterval: '10ms' }));
    await collector.waitForCalls(1);
    expect(collector.seen).toEqual([{ orderId: 'order-g4' }]);

    await h!.waitForDrain(queueId);
    expect(await readGroupCounters(h!.pool, queueId)).toEqual([
      { groupId: 'tenant-4', maxRunning: 1, running: 0 },
    ]);
  });

  it('writes one limit row per group when a batch inside the transaction spans two', async () => {
    const { queueId } = await groupedQueue('grouped-batch');
    // `publishMany` takes a single PublishOptions, so the facade cannot express
    // a multi-group batch; drive the same repository call the facade uses, with
    // the caller's tx, to reach the per-group loop.
    const repo = new JobRepository(h!.pool, silentLogger);

    await h!.tq.transaction(async (tx) => {
      await orders!.insert(tx, 'order-g5');
      await repo.publishJobs(
        [
          {
            idempotencyKey: uniqueName('mg-tx'),
            payload: JSON.stringify({ orderId: 'order-g5' }),
            queueId,
            group: { id: 'alpha', concurrency: 2 },
          },
          {
            idempotencyKey: uniqueName('mg-tx'),
            payload: JSON.stringify({ orderId: 'order-g5-b' }),
            queueId,
            group: { id: 'beta', concurrency: 5 },
          },
        ],
        tx
      );

      expect(await readGroupCounters(h!.pool, queueId)).toEqual([]);
    });

    expect(await readGroupCounters(h!.pool, queueId)).toEqual([
      { groupId: 'alpha', maxRunning: 2, running: 0 },
      { groupId: 'beta', maxRunning: 5, running: 0 },
    ]);
    expect(await h!.jobCount(queueId)).toBe(2);
    expect(await orders!.ids()).toEqual(['order-g5']);
  });
});

describe('a duplicate idempotency key inside the caller transaction', () => {
  it('leaves the transaction usable for the statements that follow it', async () => {
    const { handle, queueId } = await confirmationQueue();

    const first = await handle.publish({ orderId: 'order-14' }, { idempotencyKey: 'confirm-14' });

    // A raw 23505 here would abort the caller's transaction and take their
    // business writes with it — hence ON CONFLICT DO NOTHING plus a read-back.
    const second = await h!.tq.transaction(async (tx) => {
      const counted = countingExecutor(tx);
      const duplicate = await handle.publish(
        { orderId: 'order-14' },
        { tx: counted.executor, idempotencyKey: 'confirm-14' }
      );
      expect(duplicate.deduplicated).toBe(true);
      expect(duplicate.id).toBe(first.id);

      // The claim above is only worth anything if both statements ran on the
      // caller's connection: the ON CONFLICT insert, then the read-back of the
      // row it skipped. A publish that quietly used a pooled connection of its
      // own would leave this at 0 and every other assertion here unchanged.
      expect(counted.calls()).toBe(2);

      await orders!.insert(tx, 'written-after-duplicate');
      return duplicate;
    });

    expect(second.id).toBe(first.id);
    expect(await orders!.ids()).toEqual(['written-after-duplicate']);
    expect(await h!.jobCount(queueId)).toBe(1);
  });

  it('does not disturb business writes on either side of it', async () => {
    const { handle, queueId } = await confirmationQueue();

    await handle.publish({ orderId: 'order-15' }, { idempotencyKey: 'confirm-15' });

    await h!.tq.transaction(async (tx) => {
      await orders!.insert(tx, 'before-duplicate');

      const counted = countingExecutor(tx);
      const duplicate = await handle.publish(
        { orderId: 'order-15' },
        { tx: counted.executor, idempotencyKey: 'confirm-15' }
      );
      expect(duplicate.deduplicated).toBe(true);
      expect(counted.calls()).toBe(2);

      await orders!.insert(tx, 'after-duplicate');
    });

    expect(await orders!.ids()).toEqual(['after-duplicate', 'before-duplicate']);
    expect(await h!.jobCount(queueId)).toBe(1);
  });

  it('reads back its own uncommitted row when the same key repeats in one transaction', async () => {
    const { handle, queueId } = await confirmationQueue();
    const key = 'confirm-22';

    const [first, second] = await h!.tq.transaction(async (tx) => {
      const a = await handle.publish({ orderId: 'order-22' }, { tx, idempotencyKey: key });
      const b = await handle.publish({ orderId: 'order-22' }, { tx, idempotencyKey: key });

      // The row `a` inserted is visible to nobody but this transaction. A
      // read-back that moved to the pool would still pass the committed-row
      // case above and raise PUBLISH_CONFLICT here, on perfectly ordinary code.
      expect(b.deduplicated).toBe(true);
      expect(b.id).toBe(a.id);

      await orders!.insert(tx, 'after-self-duplicate');
      return [a, b];
    });

    expect(second.id).toBe(first.id);
    expect(await h!.jobCount(queueId)).toBe(1);
    expect(await orders!.ids()).toEqual(['after-self-duplicate']);
  });

  it('raises PUBLISH_CONFLICT when the row the insert skipped is gone before the read-back', async () => {
    const { handle, queueId } = await confirmationQueue();
    const key = 'confirm-vanished';

    const first = await handle.publish({ orderId: 'order-20' }, { idempotencyKey: key });

    const b = await h!.pool.connect();
    try {
      await b.query('BEGIN');
      await orders!.insert(b, 'before-conflict');

      // `insertJobs` runs two statements: ON CONFLICT DO NOTHING, then a
      // read-back of the keys it skipped. Deleting the conflicting row in
      // between — from another session, which is exactly what a worker
      // completing that job does — leaves the read-back with nothing to find.
      // That window is the only route to PUBLISH_CONFLICT, the publish path's one
      // typed error, so without this the branch could quietly become `row!.id`
      // (a bare TypeError) or let the 23505 fly.
      const intercepted: Executor = {
        async query(text, values) {
          const result = await b.query(text, values);
          if (text.includes('INSERT INTO jobs')) {
            await inTransaction(h!.pool, async (killer) => {
              // Bounded on purpose: were the skipped insert ever to start
              // locking the conflicting row, this would fail rather than hang.
              await killer.query("SET LOCAL statement_timeout = '3s'");
              await killer.query('DELETE FROM jobs WHERE idempotency_key = $1', [key]);
            });
          }
          return result;
        },
      };

      const error = await handle
        .publish({ orderId: 'order-20' }, { tx: intercepted, idempotencyKey: key })
        .then(() => null)
        .catch((err: unknown) => err);

      expect(error).toMatchObject({ code: ErrorCodes.PUBLISH_CONFLICT });
      expect((error as Error).message).toContain(key);

      // The typed error's whole purpose: B is untouched and carries on.
      await orders!.insert(b, 'after-conflict');
      await b.query('COMMIT');
    } finally {
      await b.query('ROLLBACK').catch(() => undefined);
      b.release();
    }

    expect(await orders!.ids()).toEqual(['after-conflict', 'before-conflict']);
    expect(await h!.job(first.id)).toBeNull();
    expect(await h!.jobCount(queueId)).toBe(0);
  });

  it('is aborted by a duplicate under REPEATABLE READ, where ON CONFLICT cannot save it', async () => {
    const { handle, queueId } = await confirmationQueue();
    const key = 'confirm-repeatable-read';

    const b = await h!.pool.connect();
    try {
      await b.query('BEGIN');
      await b.query('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ');
      // REPEATABLE READ pins the snapshot at the first statement, so take one
      // now, before the row below exists.
      await b.query('SELECT 1');

      const first = await handle.publish({ orderId: 'order-20' }, { idempotencyKey: key });
      expect(first.deduplicated).toBe(false);

      // KNOWN DEFECT, pinned as it stands. ON CONFLICT DO NOTHING keeps a
      // duplicate publish from poisoning the caller's transaction only at READ
      // COMMITTED. Against a row committed after a REPEATABLE READ snapshot,
      // Postgres raises 40001 on the INSERT itself — before the read-back gets
      // a chance — so the caller's transaction is aborted after all, which is
      // the exact outcome the ON CONFLICT design exists to prevent. Callers
      // running at REPEATABLE READ or SERIALIZABLE get no protection.
      //
      // The fix is either to document the isolation level publishing supports,
      // or to run the insert on a savepoint so a serialization failure can be
      // caught and turned into the typed PublishConflictError.
      const publishError = await handle
        .publish({ orderId: 'order-20' }, { tx: b, idempotencyKey: key })
        .then(() => null)
        .catch((err: unknown) => err);
      expect((publishError as { code?: string } | null)?.code).toBe('40001');

      const followUpError = await orders!
        .insert(b, 'after-serialization-failure')
        .then(() => null)
        .catch((err: unknown) => err);
      expect((followUpError as { code?: string } | null)?.code).toBe('25P02');
    } finally {
      await b.query('ROLLBACK').catch(() => undefined);
      b.release();
    }

    expect(await orders!.count()).toBe(0);
    expect(await h!.jobCount(queueId)).toBe(1);
  });
});

describe('metadata that Postgres rejects', () => {
  it('aborts the caller transaction on a NUL, taking their business writes with it', async () => {
    const { handle, queueId } = await confirmationQueue();

    const client = await h!.pool.connect();
    try {
      await client.query('BEGIN');
      await orders!.insert(client, 'order-nul');

      // KNOWN DEFECT, pinned as it stands rather than fixed here. `metadata` is
      // bound as `$N::jsonb` and `JSON.stringify` escapes a NUL as \u0000,
      // which jsonb rejects with 22P05. Inside `{ tx }` that error lands on the
      // caller's connection and aborts their transaction — precisely the
      // failure mode ON CONFLICT DO NOTHING exists to avoid for duplicate keys,
      // and the one the README promises publishing cannot cause.
      //
      // The fix is to validate metadata in `QueueHandle.toInput` and throw
      // InvalidInputError before any statement reaches the connection.
      const publishError = await handle
        .publish({ orderId: 'order-nul' }, { tx: client, metadata: { note: 'a\u0000b' } })
        .then(() => null)
        .catch((err: unknown) => err);
      expect((publishError as { code?: string } | null)?.code).toBe('22P05');

      const followUpError = await orders!
        .insert(client, 'order-nul-2')
        .then(() => null)
        .catch((err: unknown) => err);
      // 25P02: current transaction is aborted, commands ignored until end of
      // transaction block. The caller's own write is collateral damage.
      expect((followUpError as { code?: string } | null)?.code).toBe('25P02');

      await client.query('ROLLBACK');
    } finally {
      await client.query('ROLLBACK').catch(() => undefined);
      client.release();
    }

    expect(await orders!.count()).toBe(0);
    expect(await h!.jobCount(queueId)).toBe(0);
  });
});

describe('no BEGIN/COMMIT of its own', () => {
  it('does not commit the job when the caller rolls the transaction back', async () => {
    const { handle, queueId } = await confirmationQueue();

    // The sharpest proof available: if `publish` emitted its own COMMIT, or its
    // own BEGIN over the top of the caller's, the job would outlive this
    // rollback. It must not.
    const published = await inRolledBackTransaction(h!.pool, async (tx) => {
      await orders!.insert(tx, 'order-16');
      return handle.publish({ orderId: 'order-16' }, { tx });
    });

    expect(published.id).toBeGreaterThan(0);
    expect(await h!.job(published.id)).toBeNull();
    expect(await h!.jobCount(queueId)).toBe(0);
    expect(await orders!.count()).toBe(0);
  });

  it('leaves a publish made without tx untouched by the caller rollback', async () => {
    const { handle, queueId } = await confirmationQueue();

    let published = 0;
    await expect(
      h!.tq.transaction(async (tx) => {
        await orders!.insert(tx, 'order-17');
        // No `tx`: this publish runs on its own connection and commits on its
        // own. Opting out of the transaction has to actually opt out.
        published = (await handle.publish({ orderId: 'order-17' })).id;
        throw new Error('order cancelled');
      })
    ).rejects.toThrow('order cancelled');

    expect(await orders!.count()).toBe(0);
    expect(await h!.job(published)).not.toBeNull();
    expect(await h!.jobCount(queueId)).toBe(1);
  });
});

describe('several queues in one transaction', () => {
  it('commits publishes to two different queues with the business row', async () => {
    const confirmations = await confirmationQueue('confirmations');
    const analytics = await confirmationQueue('analytics');

    // One order, two downstream jobs — the README's pitch, extended past a
    // single queue.
    await h!.tq.transaction(async (tx) => {
      await orders!.insert(tx, 'order-21');
      await confirmations.handle.publish({ orderId: 'order-21' }, { tx });
      await analytics.handle.publish({ orderId: 'order-21' }, { tx });

      expect(await h!.jobCount(confirmations.queueId)).toBe(0);
      expect(await h!.jobCount(analytics.queueId)).toBe(0);
    });

    expect(await orders!.ids()).toEqual(['order-21']);
    expect(await h!.jobCount(confirmations.queueId)).toBe(1);
    expect(await h!.jobCount(analytics.queueId)).toBe(1);
  });

  it('discards both when the transaction rolls back', async () => {
    const confirmations = await confirmationQueue('confirmations');
    const analytics = await confirmationQueue('analytics');

    await expect(
      h!.tq.transaction(async (tx) => {
        await orders!.insert(tx, 'order-21b');
        await confirmations.handle.publish({ orderId: 'order-21b' }, { tx });
        await analytics.handle.publish({ orderId: 'order-21b' }, { tx });
        throw new Error('fraud check failed');
      })
    ).rejects.toThrow('fraud check failed');

    expect(await orders!.count()).toBe(0);
    expect(await h!.jobCount(confirmations.queueId)).toBe(0);
    expect(await h!.jobCount(analytics.queueId)).toBe(0);
  });
});

describe('savepoint interop', () => {
  it('discards only the job when the caller rolls back to a savepoint', async () => {
    const { handle, queueId } = await confirmationQueue();

    // Hand-rolled on purpose: the whole test is about the statements the caller
    // issues. The ROLLBACK in `finally` is the safety net — node-postgres does
    // not roll back on release, so a failed assertion above would otherwise
    // hand back a connection still holding row locks, and `orders.drop()` in
    // teardown would block on it for a full idle timeout.
    const client = await h!.pool.connect();
    try {
      await client.query('BEGIN');
      await orders!.insert(client, 'order-18');
      await client.query('SAVEPOINT before_publish');
      const published = await handle.publish({ orderId: 'order-18' }, { tx: client });
      await client.query('ROLLBACK TO SAVEPOINT before_publish');
      await client.query('COMMIT');

      expect(await h!.job(published.id)).toBeNull();
    } finally {
      await client.query('ROLLBACK').catch(() => undefined);
      client.release();
    }

    // The business write predates the savepoint, so it survives; the job does not.
    expect(await orders!.ids()).toEqual(['order-18']);
    expect(await h!.jobCount(queueId)).toBe(0);
  });

  it('keeps both when the savepoint is released', async () => {
    const { handle, queueId } = await confirmationQueue();

    const client = await h!.pool.connect();
    try {
      await client.query('BEGIN');
      await orders!.insert(client, 'order-19');
      await client.query('SAVEPOINT before_publish');
      await handle.publish({ orderId: 'order-19' }, { tx: client });

      // Releasing a savepoint commits nothing: still invisible elsewhere until
      // the COMMIT below.
      expect(await h!.jobCount(queueId)).toBe(0);

      await client.query('RELEASE SAVEPOINT before_publish');
      await client.query('COMMIT');
    } finally {
      await client.query('ROLLBACK').catch(() => undefined);
      client.release();
    }

    expect(await orders!.ids()).toEqual(['order-19']);
    expect(await h!.jobCount(queueId)).toBe(1);
  });
});

describe('placing an order, end to end', () => {
  it('never delivers the confirmation job before the order row it describes', async () => {
    const { handle, queueId } = await confirmationQueue('order-confirmations');

    // What the handler could see at the moment it ran. Asserting inside the
    // handler would only turn a violation into a silent retry.
    const observed: Array<{ orderId: string; orderWasVisible: boolean }> = [];

    h!.track(
      await handle.work(
        async (payload) => {
          const ids = await orders!.ids();
          observed.push({
            orderId: payload.orderId,
            orderWasVisible: ids.includes(payload.orderId),
          });
        },
        { pollInterval: '10ms' }
      )
    );

    const placeOrder = (id: string, total: number) =>
      h!.tq.transaction(async (tx) => {
        await orders!.insert(tx, id, total);
        await handle.publish({ orderId: id }, { tx, idempotencyKey: `confirm-${id}` });

        // Hold the transaction open. Without this the gap between an escaped
        // publish and the COMMIT is about a millisecond and the worker never
        // gets to look; 200ms at a 10ms poll gives it ~20 chances, so a publish
        // that left the transaction is seen here rather than passing silently.
        await sleep(200);
        expect(observed.some((entry) => entry.orderId === id)).toBe(false);

        return id;
      });

    await placeOrder('ord-a', 19.99);
    await placeOrder('ord-b', 5.5);

    await h!.waitForDrain(queueId);

    expect(observed).toHaveLength(2);
    expect(observed.map((entry) => entry.orderId).sort()).toEqual(['ord-a', 'ord-b']);
    // The whole point: the job cannot exist while its data does not.
    expect(observed.every((entry) => entry.orderWasVisible)).toBe(true);
    expect(await orders!.ids()).toEqual(['ord-a', 'ord-b']);
  });
});

describe('two transactions racing on one idempotency key', () => {
  it('produces exactly one job and aborts neither caller', async () => {
    const { handle, queueId } = await confirmationQueue();
    const key = 'confirm-race';

    const a: PoolClient = await h!.pool.connect();
    const b: PoolClient = await h!.pool.connect();

    try {
      await a.query('BEGIN');
      await b.query('BEGIN');
      await orders!.insert(a, 'race-a');
      await orders!.insert(b, 'race-b');

      const first = await handle.publish({ orderId: 'race-a' }, { tx: a, idempotencyKey: key });
      expect(first.deduplicated).toBe(false);

      // ON CONFLICT DO NOTHING cannot decide against an *uncommitted* duplicate,
      // so B's insert waits on A rather than erroring. Waiting is the desired
      // outcome: it is what lets B still read the row back afterwards.
      const secondPublish = handle.publish({ orderId: 'race-b' }, { tx: b, idempotencyKey: key });
      let settled = false;
      void secondPublish.then(
        () => {
          settled = true;
        },
        () => {
          settled = true;
        }
      );
      await sleep(300);
      expect(settled, 'B resolved while A still held the uncommitted duplicate').toBe(false);

      await a.query('COMMIT');

      const second = await secondPublish;
      expect(second.deduplicated).toBe(true);
      expect(second.id).toBe(first.id);

      // B's transaction must still be alive — a 23505 would have poisoned it
      // and this statement would fail with "current transaction is aborted".
      await orders!.insert(b, 'race-b2');
      await b.query('COMMIT');
    } finally {
      // Both clients stay hand-rolled (the test drives their COMMITs in a
      // specific order), so guard the release: a failed assertion must not hand
      // the pool back a connection still holding locks.
      await a.query('ROLLBACK').catch(() => undefined);
      await b.query('ROLLBACK').catch(() => undefined);
      a.release();
      b.release();
    }

    expect(await h!.jobCount(queueId)).toBe(1);
    expect(await orders!.ids()).toEqual(['race-a', 'race-b', 'race-b2']);
  });

  it('lets the loser insert its own row when the winner rolls back', async () => {
    const { handle, queueId } = await confirmationQueue();
    const key = 'confirm-race-rollback';

    const a: PoolClient = await h!.pool.connect();
    const b: PoolClient = await h!.pool.connect();

    try {
      await a.query('BEGIN');
      await b.query('BEGIN');

      const first = await handle.publish({ orderId: 'race-c' }, { tx: a, idempotencyKey: key });
      expect(first.deduplicated).toBe(false);

      const secondPublish = handle.publish({ orderId: 'race-d' }, { tx: b, idempotencyKey: key });
      let settled = false;
      void secondPublish.then(
        () => {
          settled = true;
        },
        () => {
          settled = true;
        }
      );
      await sleep(300);
      expect(settled, 'B resolved while A still held the uncommitted duplicate').toBe(false);

      // A gives up. Its speculative insertion is released, so B's ON CONFLICT
      // actually inserts instead of skipping — the other side of the
      // freshKeys/rowsByKey reconciliation, and the realistic shape of "the
      // first attempt failed, retry it".
      await a.query('ROLLBACK');

      const second = await secondPublish;
      expect(second.deduplicated).toBe(false);
      expect(second.id).not.toBe(first.id);

      await orders!.insert(b, 'race-d');
      await b.query('COMMIT');

      expect((await h!.job(second.id))?.id).toBe(second.id);
    } finally {
      await a.query('ROLLBACK').catch(() => undefined);
      await b.query('ROLLBACK').catch(() => undefined);
      a.release();
      b.release();
    }

    const jobs = await h!.jobs(queueId);
    expect(jobs).toHaveLength(1);
    expect(JSON.parse(jobs[0].payload)).toEqual({ orderId: 'race-d' });
    expect(await orders!.ids()).toEqual(['race-d']);
  });
});
