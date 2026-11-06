import * as net from 'net';
import { Pool } from 'pg';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { JobStatus } from '../src/domain/job';
import { NUMBER_OF_SHARD } from '../src/domain/queue';
import { JobSnooze, LeaseLostError, NonRetryable } from '../src/domain/errors';
import { SettleRetryOptions } from '../src/domain/worker';
import { createPool } from '../src/repository/postgresql/connection';
import { JobRepository } from '../src/repository/postgresql/job.repository';
import { QueueRepository } from '../src/repository/postgresql/queue.repository';
import { JobService } from '../src/services/job.service';
import { WorkerService } from '../src/services/worker.service';
import { isTransientSettleError, retrySettle } from '../src/services/settle-retry';
import {
  Harness,
  createHarness,
  jobInput,
  pgConfig,
  queueInput,
  resetDatabase,
  sleep,
  waitFor,
} from './support/harness';
import { readJobRow, readShardCounters } from './support/invariants';
import { FlakyProxy, startFlakyProxy } from './support/flaky-proxy';

let h: Harness;
let proxy: FlakyProxy;
/** Reaches Postgres only through `proxy`. `h.pool` stays direct, for setup and assertions. */
let flakyPool: Pool;
let flakyJobService: JobService;

beforeEach(async () => {
  h = createHarness({ maxConnections: 10 });
  await resetDatabase(h.pool);

  const target = pgConfig();
  proxy = await startFlakyProxy({ host: target.host!, port: target.port! });
  flakyPool = createPool(pgConfig({ host: '127.0.0.1', port: proxy.port, max: 4 }));
  // A cut reaches idle clients as an 'error' event on the pool, which Node
  // treats as fatal when nothing listens. A production pool needs one too.
  flakyPool.on('error', () => {});
  flakyJobService = new JobService(new JobRepository(flakyPool), new QueueRepository(flakyPool));
});

afterEach(async () => {
  await flakyPool.end();
  await proxy.close();
  await h.close();
});

const FAST_RETRY: SettleRetryOptions = { baseDelayMs: 10, maxDelayMs: 50 };

function flakyWorker(
  queueId: number,
  handler: () => Promise<void>,
  settleRetry: SettleRetryOptions = FAST_RETRY
): WorkerService {
  return new WorkerService(flakyJobService, { queueId, pollInterval: 20, settleRetry, handler });
}

/** Settles `promise` into the error it rejects with. */
async function errorOf(promise: Promise<unknown>): Promise<any> {
  try {
    await promise;
  } catch (error) {
    return error;
  }
  throw new Error('expected the operation to fail');
}

/** A local port with nothing listening on it. */
async function closedPort(): Promise<number> {
  const server = net.createServer();
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as net.AddressInfo;
  await new Promise<void>((resolve) => server.close(() => resolve()));
  return port;
}

describe.each([
  ['fast path', { concurrency: 0 }],
  ['coordination path', { concurrency: NUMBER_OF_SHARD * 2 }],
])('settling through an outage — %s', (_path, queueOverrides) => {
  /**
   * Runs one job whose handler takes the link down on its way out: the work is
   * done, its outcome not yet recorded. The outage is held until the worker has
   * been turned away three times — proof that the settle failed and is being
   * retried, not that it slipped through before the cut — then lifted.
   */
  async function settleThroughOutage(outcome: () => void): Promise<{ jobId: number; runs: number }> {
    const queue = await h.queueService.createQueue(queueInput(queueOverrides));
    const published = await h.jobRepo.publishJob(jobInput(queue.id));

    let runs = 0;
    const worker = flakyWorker(queue.id, async () => {
      runs += 1;
      proxy.cut();
      outcome();
    });

    await worker.start();
    try {
      await waitFor(() => proxy.refused >= 3, { message: 'the settle to be retried during the outage' });
      expect((await readJobRow(h.pool, published.id)).status).toBe(JobStatus.PROCESSING);

      proxy.restore();
      await waitFor(
        async () => (await readJobRow(h.pool, published.id))?.status !== JobStatus.PROCESSING,
        { message: 'the settle to land once the link is back' }
      );
    } finally {
      await worker.stop();
    }

    const shards = await readShardCounters(h.pool, queue.id);
    expect(shards.reduce((sum, s) => sum + s.running, 0), 'the slot must be given back').toBe(0);
    return { jobId: published.id, runs };
  }

  it('completes the job, so it is not run a second time', async () => {
    const { jobId, runs } = await settleThroughOutage(() => {});
    expect(await readJobRow(h.pool, jobId)).toBeNull();
    expect(runs).toBe(1);
  });

  it('discards a NonRetryable job instead of leaving the reaper to hand it out again', async () => {
    const { jobId, runs } = await settleThroughOutage(() => {
      throw new NonRetryable();
    });
    expect(await readJobRow(h.pool, jobId)).toBeNull();
    expect(runs).toBe(1);
  });

  it('snoozes the job with its delay and its attempt refund intact', async () => {
    const { jobId, runs } = await settleThroughOutage(() => {
      throw new JobSnooze(60_000);
    });
    const row = await readJobRow(h.pool, jobId);
    expect(row.status).toBe(JobStatus.PENDING);
    expect(row.deferred, 'the snooze delay must survive the outage').toBe(true);
    expect(row.attempts, 'the snooze must still refund the attempt').toBe(0);
    expect(runs).toBe(1);
  });
});

describe('settle retry — limits', () => {
  it('keeps retrying through stop(), which waits for the settle to land', async () => {
    const queue = await h.queueService.createQueue(queueInput());
    const published = await h.jobRepo.publishJob(jobInput(queue.id));
    const worker = flakyWorker(queue.id, async () => proxy.cut());

    await worker.start();
    await waitFor(() => proxy.refused >= 1, { message: 'the settle to hit the outage' });

    let stopped = false;
    const stopping = worker.stop().then(() => {
      stopped = true;
    });
    const seen = proxy.refused;
    await waitFor(() => proxy.refused >= seen + 3, { message: 'retries to carry on after stop()' });
    expect(stopped, 'stop() must wait for the settle').toBe(false);

    proxy.restore();
    await stopping;
    expect(await readJobRow(h.pool, published.id)).toBeNull();
  });

  it('gives up at the deadline and leaves the job to the reaper', async () => {
    const queue = await h.queueService.createQueue(queueInput());
    const published = await h.jobRepo.publishJob(jobInput(queue.id));
    const worker = flakyWorker(queue.id, async () => proxy.cut(), { ...FAST_RETRY, deadlineMs: 300 });

    await worker.start();
    await waitFor(() => proxy.refused >= 1, { message: 'the settle to hit the outage' });

    // The link stays down. Without the deadline the settle would retry for as
    // long as the outage lasts, and stop() would not return.
    const outcome = await Promise.race([
      worker.stop().then(() => 'stopped'),
      sleep(10_000).then(() => 'still retrying'),
    ]);
    expect(outcome).toBe('stopped');

    proxy.restore();
    const row = await readJobRow(h.pool, published.id);
    expect(row.status, 'an abandoned settle leaves the job for the reaper').toBe(JobStatus.PROCESSING);
  });
});

describe('retrySettle', () => {
  it('stops at LeaseLostError: the fence has answered, and asking again gets the same answer', async () => {
    const queue = await h.queueService.createQueue(queueInput());
    await h.jobRepo.publishJob(jobInput(queue.id));
    const pulled = await h.jobRepo.pullJob(queue);

    let attempts = 0;
    const settle = retrySettle(async (timeoutMs) => {
      attempts += 1;
      await h.jobRepo.completeJob(pulled!.id, pulled!.lockSeq + 1, queue, timeoutMs);
    }, FAST_RETRY);

    await expect(settle).rejects.toBeInstanceOf(LeaseLostError);
    expect(attempts).toBe(1);
  });

  it('abandons an attempt that hangs — which still commits later, so the retry finds the lease gone', async () => {
    const queue = await h.queueService.createQueue(queueInput());
    const published = await h.jobRepo.publishJob(jobInput(queue.id));
    const pulled = await h.jobRepo.pullJob(queue);

    // Hold the row lock, so every complete blocks until its timeout abandons it.
    const blocker = await h.pool.connect();
    await blocker.query('BEGIN');
    await blocker.query('SELECT 1 FROM jobs WHERE id = $1 FOR UPDATE', [published.id]);

    let attempts = 0;
    const settled = retrySettle(async (timeoutMs) => {
      attempts += 1;
      await h.jobRepo.completeJob(pulled!.id, pulled!.lockSeq, queue, timeoutMs);
    }, { ...FAST_RETRY, attemptTimeoutMs: 200 }).then(() => 'settled', (error: unknown) => error);

    try {
      await waitFor(() => attempts >= 2, { message: 'the hung attempt to be abandoned and retried' });
    } finally {
      await blocker.query('ROLLBACK');
      blocker.release();
    }

    // Abandoning an attempt only stops the client waiting: on the server the
    // first DELETE is still queued for the lock, and it commits once the lock
    // is free. The live attempt finds the lease gone — what a lost reply looks
    // like from the worker's side. The job is completed exactly once.
    expect(await settled).toBeInstanceOf(LeaseLostError);
    expect(await readJobRow(h.pool, published.id)).toBeNull();
  });
});

describe('isTransientSettleError — on errors a real server and driver raise', () => {
  it('a dropped link: the connection ends before the server answers', async () => {
    proxy.cut();
    const error = await errorOf(flakyPool.query('SELECT 1'));
    expect(error.severity, 'the server never answered').toBeUndefined();
    expect(isTransientSettleError(error)).toBe(true);
  });

  it('a refused connection: nothing listens where the database was', async () => {
    const pool = createPool(pgConfig({ host: '127.0.0.1', port: await closedPort(), max: 1 }));
    try {
      const error = await errorOf(pool.query('SELECT 1'));
      expect(error.code).toBe('ECONNREFUSED');
      expect(isTransientSettleError(error)).toBe(true);
    } finally {
      await pool.end();
    }
  });

  it('57P01 terminated backend: what a restart or a failover does to open sessions', async () => {
    const client = await h.pool.connect();
    client.on('error', () => {}); // the dead connection is reported as an event too
    try {
      const { rows } = await client.query('SELECT pg_backend_pid() AS pid');
      const victim = errorOf(client.query('SELECT pg_sleep(30)'));
      await waitFor(
        async () =>
          (
            await h.pool.query(`SELECT 1 FROM pg_stat_activity WHERE pid = $1 AND state = 'active'`, [
              rows[0].pid,
            ])
          ).rows.length > 0,
        { message: 'the statement to be running' }
      );
      await h.pool.query('SELECT pg_terminate_backend($1)', [rows[0].pid]);

      const error = await victim;
      expect(error.code).toBe('57P01');
      expect(isTransientSettleError(error)).toBe(true);
    } finally {
      client.release(true);
    }
  });

  it('25006 read-only transaction: what a node demoted to standby answers after a failover', async () => {
    const client = await h.pool.connect();
    try {
      await client.query('BEGIN READ ONLY');
      const error = await errorOf(client.query('DELETE FROM jobs WHERE false'));
      expect(error.code).toBe('25006');
      expect(isTransientSettleError(error)).toBe(true);
    } finally {
      client.release(true);
    }
  });

  it('42601 syntax error: permanent, since a retry sends the same statement', async () => {
    const error = await errorOf(h.pool.query('SELEC 1'));
    expect(error.code).toBe('42601');
    expect(isTransientSettleError(error)).toBe(false);
  });
});
