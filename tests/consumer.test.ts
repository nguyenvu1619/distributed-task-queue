/**
 * Consuming jobs through `QueueHandle.work()`, plus the reaper that puts
 * abandoned leases back.
 *
 * Everything here drives the public facade. Claims about what happened to a job
 * are checked against the `jobs` table rather than against in-process
 * bookkeeping: the row is the only state two worker processes actually share,
 * and a worker that *thinks* it completed a job proves nothing.
 */
import { Pool } from 'pg';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { JobStatus } from '../src/domain/job';
import { WorkerErrorEvent } from '../src/domain/worker';
import { JobContext } from '../src/client/options';
import { Serializer } from '../src/client/serializer';
import {
  ProcessingSampler,
  ConcurrencyTracker,
  countProcessingInGroup,
  expectNoNegativeCounters,
  readGroupCounters,
  readShardCounters,
} from './support/invariants';
import {
  Collector,
  TaskQueueHarness,
  collect,
  createTaskQueueHarness,
  sleep,
  waitFor,
} from './support/task-queue';

/**
 * Two names for one harness, on purpose.
 *
 * `live` is what teardown owns and `h` is what the tests read. If the beforeEach
 * hook throws, `live` is still undefined, so afterEach closes nothing instead of
 * ending the PREVIOUS test's already-ended pool and burying the real failure
 * under "Called end on pool more than once".
 */
let live: TaskQueueHarness | undefined;
let h!: TaskQueueHarness;

beforeEach(async () => {
  live = undefined;
  // Roomier than the default: the concurrency cases run up to eight worker
  // slots, and every coordinated pull holds a connection for its transaction.
  h = await createTaskQueueHarness({ maxConnections: 25 });
  live = h;
});

afterEach(async () => {
  const finished = live;
  live = undefined;
  await finished?.close();
  vi.restoreAllMocks();
});

// ---------------------------------------------------------------------------
// Local helpers
// ---------------------------------------------------------------------------

interface Deferred<T = void> {
  promise: Promise<T>;
  resolve(value: T): void;
}

function deferred<T = void>(): Deferred<T> {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((r) => {
    resolve = r as (value: T) => void;
  });
  return { promise, resolve };
}

interface Barrier {
  /** True once `size` handlers were inside the gate at the same time. */
  opened: boolean;
  wait(): Promise<void>;
}

/**
 * Holds every arriving handler until `size` of them are in flight together.
 *
 * This is how "N slots really do run N handlers at once" is proved without a
 * sleep: if the parallelism is not there, nobody is released. The timeout is a
 * deadlock guard only — it releases everyone so the assertion on the observed
 * peak reports the real number instead of the suite hanging.
 */
function barrier(size: number, options: { timeout?: number; hold?: number } = {}): Barrier {
  const { timeout = 15_000, hold = 0 } = options;
  let arrived = 0;
  let open!: () => void;
  const gate = new Promise<void>((resolve) => {
    open = resolve;
  });
  const timer = setTimeout(() => open(), timeout);
  timer.unref?.();

  const instance: Barrier = {
    opened: false,
    async wait() {
      arrived += 1;
      if (arrived >= size) {
        instance.opened = true;
        clearTimeout(timer);
        open();
      }
      await gate;
      // Keeps the job PROCESSING a little longer once the gate is open, so a
      // sampler polling the database has a window wide enough to see it.
      if (hold > 0) {
        await sleep(hold);
      }
    },
  };
  return instance;
}

interface GroupSampler {
  peaks: Map<string, number>;
  /**
   * Polls that came back. Every failed poll is swallowed below, so a sampler
   * can silently observe nothing at all — `samples` is what tells the two
   * apart, and a peak assertion is only worth reading next to it.
   */
  samples: number;
  stop(): void;
}

/** Per-group twin of ProcessingSampler: database ground truth for group caps. */
function sampleGroupConcurrency(
  pool: Pool,
  queueId: number,
  groups: string[],
  intervalMs = 5
): GroupSampler {
  const peaks = new Map<string, number>(groups.map((group) => [group, 0]));
  let inFlight = false;
  let timer: NodeJS.Timeout;

  const sampler: GroupSampler = { peaks, samples: 0, stop: () => clearInterval(timer) };

  timer = setInterval(() => {
    if (inFlight) return; // never queue samples up behind a slow one
    inFlight = true;
    Promise.all(groups.map((group) => countProcessingInGroup(pool, queueId, group)))
      .then((counts) => {
        sampler.samples += 1;
        counts.forEach((n, index) => {
          const group = groups[index];
          if (n > peaks.get(group)!) peaks.set(group, n);
        });
      })
      .catch(() => {
        /* pool saturated mid-race — drop the sample rather than fail the run */
      })
      .finally(() => {
        inFlight = false;
      });
  }, intervalMs);
  timer.unref?.();

  return sampler;
}

/**
 * Moves a row's `created_at` into the past.
 *
 * Both the pull and the reaper order by `created_at`, and rows inserted in a
 * loop come out in insertion order under every plausible ordering key — so a
 * test that wants to pin the key itself has to make the two disagree.
 */
async function backdateCreatedAt(id: number, seconds: number): Promise<void> {
  await h.pool.query(
    `UPDATE jobs SET created_at = now() - ($2 || ' seconds')::interval WHERE id = $1`,
    [id, seconds]
  );
}

/** Puts a lease in the past without waiting for wall-clock time. */
async function expireLease(id: number): Promise<void> {
  await h.pool.query(
    `UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1`,
    [id]
  );
}

/** Rewrites a live job into the state a crashed worker would have left behind. */
async function simulateCrashedWorker(
  id: number,
  { attempts, leaseSeq = 1 }: { attempts: number; leaseSeq?: number }
): Promise<void> {
  await h.pool.query(
    `UPDATE jobs
     SET status = 'PROCESSING', attempts = $2, lease_seq = $3,
         lease_expires_at = now() - interval '1 second'
     WHERE id = $1`,
    [id, attempts, leaseSeq]
  );
}

// ---------------------------------------------------------------------------

describe('delivery', () => {
  it('hands the handler the decoded payload and a fully populated context', async () => {
    const queue = h.tq.defineQueue<{ order: number }>(h.name('deliver'), { maxAttempts: 5 });
    const published = await queue.publish({ order: 7 });

    const payloads: Array<{ order: number }> = [];
    let seen: JobContext | undefined;
    let abortedOnEntry: boolean | undefined;

    h.track(
      await queue.work(
        async (payload, ctx) => {
          payloads.push(payload);
          seen = ctx;
          abortedOnEntry = ctx.signal.aborted;
        },
        { pollInterval: '10ms' }
      )
    );

    await h.waitForDrain(await queue.id());

    expect(payloads).toEqual([{ order: 7 }]);
    expect(seen!.id).toBe(published.id);
    expect(seen!.queue).toBe(queue.name);
    expect(seen!.attempt).toBe(1);
    expect(seen!.maxAttempts).toBe(5);
    expect(seen!.groupId).toBeNull();
    expect(seen!.job.idempotencyKey).toBe(published.idempotencyKey);
    // The raw record is the escape hatch — it must carry the live lease.
    expect(seen!.job.payload).toBe(JSON.stringify({ order: 7 }));
    expect(seen!.job.lockSeq).toBe(1);
    expect(abortedOnEntry).toBe(false);

    // Terminal jobs are deleted; there is no archive table to look in.
    expect(await h.job(published.id)).toBeNull();
  });

  it('surfaces the group id of a grouped job on the context', async () => {
    const queue = h.tq.defineQueue<{ n: number }>(h.name('grouped-ctx'), {
      requiresGroupId: true,
    });
    await queue.publish({ n: 1 }, { group: { id: 'tenant-a', concurrency: 1 } });

    let groupId: string | null | undefined;
    h.track(
      await queue.work(
        async (_payload, ctx) => {
          groupId = ctx.groupId;
        },
        { pollInterval: '10ms' }
      )
    );

    await h.waitForDrain(await queue.id());
    expect(groupId).toBe('tenant-a');
  });

  it('delivers jobs to a single-slot worker oldest created_at first', async () => {
    const queue = h.tq.defineQueue<{ n: number }>(h.name('fifo'));

    // Each row is back-dated so that insertion order is the exact REVERSE of
    // the delivery order being claimed. Publishing in order would leave
    // `ORDER BY created_at`, `ORDER BY id` and `ORDER BY updated_at` producing
    // the same sequence, and the test would characterise a query plan rather
    // than the ordering key.
    //
    // Honest limit: deleting the ORDER BY outright is still undetectable here.
    // `idx_job_pending_queue` is a partial index on (queue_id, created_at), so
    // an unordered scan may well hand rows back in created_at order anyway.
    for (let n = 0; n < 8; n++) {
      const published = await queue.publish({ n });
      await backdateCreatedAt(published.id, n + 1);
    }

    const worked = collect<{ n: number }>();
    h.track(await queue.work(worked.handler, { pollInterval: '10ms' }));

    await h.waitForDrain(await queue.id());
    expect(worked.seen.map((payload) => payload.n)).toEqual([7, 6, 5, 4, 3, 2, 1, 0]);
  });
});

describe('worker concurrency', () => {
  it('runs as many handlers at once as it has slots', async () => {
    const queue = h.tq.defineQueue<{ n: number }>(h.name('slots'));
    await queue.publishMany([{ n: 0 }, { n: 1 }, { n: 2 }, { n: 3 }]);

    const gate = barrier(4);
    const tracker = new ConcurrencyTracker();

    h.track(
      await queue.work(
        async () => {
          tracker.enter();
          try {
            await gate.wait();
          } finally {
            tracker.exit();
          }
        },
        { concurrency: 4, pollInterval: '10ms' }
      )
    );

    // Longer than the barrier's own 15s deadlock guard, so a missing slot shows
    // up as "peak was 3" rather than as an opaque drain timeout.
    await h.waitForDrain(await queue.id(), { timeout: 30_000 });

    expect(gate.opened).toBe(true);
    expect(tracker.peak).toBe(4);
    expect(tracker.entered).toBe(4);
  });

  it('never runs two handlers at once on a single-slot worker', async () => {
    const queue = h.tq.defineQueue<{ n: number }>(h.name('one-slot'));
    await queue.publishMany([0, 1, 2, 3, 4, 5].map((n) => ({ n })));

    const tracker = new ConcurrencyTracker();
    h.track(
      await queue.work(
        async () => {
          tracker.enter();
          try {
            // Long enough that an accidental second slot would overlap here.
            await sleep(25);
          } finally {
            tracker.exit();
          }
        },
        { pollInterval: '10ms' }
      )
    );

    await h.waitForDrain(await queue.id());
    expect(tracker.entered).toBe(6);
    expect(tracker.peak).toBe(1);
  });

  it('honours the queue concurrency cap even with more worker slots than the cap', async () => {
    const CAP = 4;
    const queue = h.tq.defineQueue<{ n: number }>(h.name('capped'), { concurrency: CAP });
    const queueId = await queue.id();
    await queue.publishMany(Array.from({ length: 12 }, (_, n) => ({ n })));

    const sampler = new ProcessingSampler(h.pool, queueId, 5);
    sampler.start();

    // In a finally: a failed drain must not leave an interval polling the pool
    // while the harness is tearing it down.
    try {
      const gate = barrier(CAP, { hold: 60 });
      h.track(
        await queue.work(
          async () => {
            await gate.wait();
          },
          { concurrency: 8, pollInterval: '10ms' }
        )
      );

      await h.waitForDrain(queueId, { timeout: 30_000 });
      expect(gate.opened).toBe(true);
    } finally {
      sampler.stop();
    }

    // The gate above only opens once CAP handlers are inside it together, so
    // the cap is shown to admit its full width and not merely to stay under it.
    expect(sampler.samples).toBeGreaterThan(0);
    expect(sampler.peak).toBeLessThanOrEqual(CAP);
    // A regression that serialised the queue would still satisfy `<= CAP`, so
    // the cap has to be shown to admit real parallelism as well.
    expect(sampler.peak).toBeGreaterThan(1);

    // Every shard slot handed out was handed back. Length first: `.every()` is
    // vacuously true on an empty array, so a regression that stopped creating
    // queue_shards rows at all would otherwise satisfy this.
    const capShards = await readShardCounters(h.pool, queueId);
    expect(capShards).toHaveLength(32);
    expect(capShards.every((s) => s.running === 0)).toBe(true);
    await expectNoNegativeCounters(h.pool, queueId);
  });
});

describe('retries', () => {
  it('retries a permanently failing job exactly maxAttempts times, then deletes it', async () => {
    const queue = h.tq.defineQueue<{ n: number }>(h.name('retry-forever'), { maxAttempts: 3 });
    const published = await queue.publish({ n: 1 });

    const boom = new Error('handler always fails');
    const attempts: number[] = [];
    const errors: WorkerErrorEvent[] = [];

    h.track(
      await queue.work(
        async (_payload, ctx) => {
          attempts.push(ctx.attempt);
          throw boom;
        },
        { pollInterval: '10ms', onError: (event) => errors.push(event) }
      )
    );

    await h.waitForDrain(await queue.id());
    // The row is gone, so a fourth delivery is impossible — but a worker that
    // kept a stale copy could still re-enter the handler. Prove it does not.
    await sleep(150);

    expect(attempts).toEqual([1, 2, 3]);
    const handlerErrors = errors.filter((event) => event.phase === 'handler');
    expect(handlerErrors).toHaveLength(3);
    expect(handlerErrors.every((event) => event.error === boom)).toBe(true);
    expect(await h.job(published.id)).toBeNull();
  });

  it('completes a job that throws once and then succeeds, after exactly two deliveries', async () => {
    const queue = h.tq.defineQueue<{ n: number }>(h.name('retry-once'), { maxAttempts: 3 });
    const published = await queue.publish({ n: 1 });

    const worked = collect<{ n: number }>((_payload, index) => {
      if (index === 0) {
        throw new Error('transient failure');
      }
    });

    h.track(await queue.work(worked.handler, { pollInterval: '10ms' }));

    await h.waitForDrain(await queue.id());
    await sleep(150);

    expect(worked.calls).toBe(2);
    expect(worked.seen).toEqual([{ n: 1 }, { n: 1 }]);
    expect(await h.job(published.id)).toBeNull();
  });

  it('reports a handler failure through onError with the phase, error and job', async () => {
    const queue = h.tq.defineQueue<{ n: number }>(h.name('on-error'), { maxAttempts: 1 });
    const published = await queue.publish({ n: 1 });

    const boom = new Error('nope');
    const errors: WorkerErrorEvent[] = [];

    h.track(
      await queue.work(
        async () => {
          throw boom;
        },
        { pollInterval: '10ms', onError: (event) => errors.push(event) }
      )
    );

    await waitFor(() => errors.length > 0, { message: 'onError to fire' });
    // maxAttempts is 1, so the job is discarded on that first failure. Settling
    // first means a second event here would be a redelivery, not a slow report.
    await sleep(150);

    expect(errors).toHaveLength(1);
    expect(errors[0].phase).toBe('handler');
    expect(errors[0].error).toBe(boom);
    expect(errors[0].job?.id).toBe(published.id);
    // The single slot of a default worker is slot 0. `typeof === 'number'` was
    // satisfied by -1 and by NaN.
    expect(errors[0].slot).toBe(0);
  });

  it('does not let an onError hook that throws break the worker', async () => {
    const queue = h.tq.defineQueue<{ n: number }>(h.name('on-error-throws'), { maxAttempts: 1 });
    await queue.publish({ n: 1 });

    const worked = collect<{ n: number }>((_payload, index) => {
      if (index === 0) throw new Error('first job fails');
    });

    h.track(
      await queue.work(worked.handler, {
        pollInterval: '10ms',
        onError: () => {
          throw new Error('the hook itself is broken');
        },
      })
    );

    await h.waitForDrain(await queue.id());

    // The slot survived the broken hook and is still consuming.
    await queue.publish({ n: 2 });
    await waitFor(() => worked.calls >= 2, { message: 'the worker to keep consuming' });
    await h.waitForDrain(await queue.id());
  });
});

describe('poison payloads', () => {
  it('discards an undecodable payload on the first look instead of retrying it', async () => {
    const queue = h.tq.defineQueue<{ n: number }>(h.name('poison'), { maxAttempts: 5 });
    const queueId = await queue.id();

    // Planted directly: the serializer would never produce this, but another
    // producer (or an older schema) can.
    const { rows } = await h.pool.query(
      `INSERT INTO jobs (idempotency_key, payload, status, queue_id, attempts, metadata)
       VALUES ($1, $2, 'PENDING', $3, 0, '{}'::jsonb)
       RETURNING id`,
      [h.name('poison-key'), '{ this is not json', queueId]
    );
    const id = Number(rows[0].id);

    const errors: WorkerErrorEvent[] = [];
    const worked = collect();

    h.track(
      await queue.work(worked.handler, {
        pollInterval: '10ms',
        onError: (event) => errors.push(event),
      })
    );

    await waitFor(async () => (await h.job(id)) === null, {
      message: 'the poison job to be discarded',
    });
    // maxAttempts is 5: routing poison through failJob would redeliver it four
    // more times. At a 10ms poll that would all have happened by now.
    await sleep(250);

    expect(errors.map((event) => event.phase)).toEqual(['deserialize']);
    expect(errors[0].job?.id).toBe(id);
    // Discarded before the handler was ever entered.
    expect(worked.calls).toBe(0);
  });

  it('keeps consuming healthy jobs after discarding a poison one', async () => {
    const queue = h.tq.defineQueue<{ n: number }>(h.name('poison-then-good'));
    const queueId = await queue.id();

    await h.pool.query(
      `INSERT INTO jobs (idempotency_key, payload, status, queue_id, attempts, metadata)
       VALUES ($1, $2, 'PENDING', $3, 0, '{}'::jsonb)`,
      [h.name('poison-key'), 'definitely-not-json', queueId]
    );
    await queue.publish({ n: 42 });

    const worked = collect<{ n: number }>();
    h.track(await queue.work(worked.handler, { pollInterval: '10ms' }));

    await h.waitForDrain(queueId);
    expect(worked.seen).toEqual([{ n: 42 }]);
  });

  it('gives the shard slot back when it discards a poison job on a capped queue', async () => {
    // discardJob has two branches. The tests above only ever reach the bare
    // DELETE; this one is the transactional branch, where the slot has to be
    // released before the row goes. A leak there is permanent and silent: every
    // poison job keeps a shard slot until the counters hit max and the queue
    // stops admitting anything at all.
    const queue = h.tq.defineQueue<{ n: number }>(h.name('poison-capped'), {
      concurrency: 4,
      maxAttempts: 5,
    });
    const queueId = await queue.id();

    const { rows } = await h.pool.query(
      `INSERT INTO jobs (idempotency_key, payload, status, queue_id, attempts, metadata)
       VALUES ($1, $2, 'PENDING', $3, 0, '{}'::jsonb)
       RETURNING id`,
      [h.name('poison-key'), '{ this is not json', queueId]
    );
    const poisonId = Number(rows[0].id);
    const healthy = await queue.publish({ n: 42 });

    const errors: WorkerErrorEvent[] = [];
    const worked = collect<{ n: number }>();

    h.track(
      await queue.work(worked.handler, {
        pollInterval: '10ms',
        onError: (event) => errors.push(event),
      })
    );

    await h.waitForDrain(queueId, { timeout: 30_000 });
    // maxAttempts is 5: a poison job routed through failJob would be back four
    // more times, and at a 10ms poll that would land inside this window.
    await sleep(250);

    expect(errors.map((event) => event.phase)).toEqual(['deserialize']);
    expect(errors[0].job?.id).toBe(poisonId);
    expect(worked.seen).toEqual([{ n: 42 }]);
    expect(await h.job(healthy.id)).toBeNull();

    // This test exists to prove the discard released its shard slot, so the
    // count has to be pinned before `.every()` — which is true of an empty array.
    const shards = await readShardCounters(h.pool, queueId);
    expect(shards).toHaveLength(32);
    expect(shards.every((s) => s.running === 0)).toBe(true);
    await expectNoNegativeCounters(h.pool, queueId);
  });

  it('gives the group slot back when it discards a poison job on a grouped queue', async () => {
    const queue = h.tq.defineQueue<{ n: number }>(h.name('poison-grouped'), {
      requiresGroupId: true,
      maxAttempts: 5,
    });
    const queueId = await queue.id();
    const group = 'tenant-poison';

    // The group row is planted alongside the job on purpose: the coordinated
    // pull increments group_queue_limits and rolls the whole pull back when
    // there is no row to increment, so a grouped job without one wedges the
    // queue for ever instead of ever reaching the discard being tested.
    await h.pool.query(
      `INSERT INTO group_queue_limits (group_id, queue_id, max_running, running, created_at, updated_at)
       VALUES ($1, $2, 1, 0, now(), now())
       ON CONFLICT DO NOTHING`,
      [group, queueId]
    );
    const { rows } = await h.pool.query(
      `INSERT INTO jobs (idempotency_key, payload, status, queue_id, group_id, attempts, metadata)
       VALUES ($1, $2, 'PENDING', $3, $4, 0, '{}'::jsonb)
       RETURNING id`,
      [h.name('poison-key'), '{ this is not json', queueId, group]
    );
    const poisonId = Number(rows[0].id);
    const healthy = await queue.publish({ n: 42 }, { group: { id: group, concurrency: 1 } });

    const errors: WorkerErrorEvent[] = [];
    const worked = collect<{ n: number }>();

    h.track(
      await queue.work(worked.handler, {
        pollInterval: '10ms',
        onError: (event) => errors.push(event),
      })
    );

    await h.waitForDrain(queueId, { timeout: 30_000 });
    await sleep(250);

    expect(errors.map((event) => event.phase)).toEqual(['deserialize']);
    expect(errors[0].job?.id).toBe(poisonId);
    expect(worked.seen).toEqual([{ n: 42 }]);
    expect(await h.job(healthy.id)).toBeNull();

    // The group is capped at one: a slot still held here would make the next
    // job on this group undeliverable.
    expect(await readGroupCounters(h.pool, queueId)).toEqual([
      { groupId: group, maxRunning: 1, running: 0 },
    ]);
    await expectNoNegativeCounters(h.pool, queueId);
  });
});

describe('custom serializers', () => {
  interface Point {
    x: number;
    y: number;
  }

  // Deliberately not JSON, so nothing here can pass by accident. `work()` wires
  // the queue's serializer into the worker; WorkerService falls back to
  // JSON.parse when it is missing. If that wiring regressed, every payload from
  // a non-JSON codec would be classified as poison and DELETED without a retry
  // — total, silent data loss that the producer-side tests cannot see.
  const pipeSerializer: Serializer<Point> = {
    serialize: (value) => `${value.x}|${value.y}`,
    deserialize: (raw) => {
      const [x, y] = raw.split('|');
      return { x: Number(x), y: Number(y) };
    },
  };

  it('hands the handler the value the queue codec decoded, not JSON.parse output', async () => {
    const queue = h.tq.defineQueue<Point>(h.name('consume-custom-ser'), {
      serializer: pipeSerializer,
    });
    const published = await queue.publish({ x: 3, y: 4 });

    const worked = collect<Point>();
    const errors: WorkerErrorEvent[] = [];
    h.track(
      await queue.work(worked.handler, {
        pollInterval: '10ms',
        onError: (event) => errors.push(event),
      })
    );

    await h.waitForDrain(await queue.id());

    expect(worked.seen).toEqual([{ x: 3, y: 4 }]);
    // Not a discard: '3|4' is not JSON, so the fallback would have reported
    // 'deserialize' and deleted the job.
    expect(errors).toEqual([]);
    expect(await h.job(published.id)).toBeNull();
  });

  it('classifies poison by the queue codec even when the payload is valid JSON', async () => {
    // '13' and '7' both parse as JSON, so only this codec can reject one and
    // turn the other into an object rather than a number. That is what makes
    // the two assertions below specific to the queue's serializer.
    const tagSerializer: Serializer<{ tag: string }> = {
      serialize: (value) => value.tag,
      deserialize: (raw) => {
        if (raw === '13') {
          throw new Error(`unsupported tag ${raw}`);
        }
        return { tag: raw };
      },
    };

    const queue = h.tq.defineQueue<{ tag: string }>(h.name('codec-poison'), {
      serializer: tagSerializer,
      maxAttempts: 5,
    });
    const poison = await queue.publish({ tag: '13' });
    const healthy = await queue.publish({ tag: '7' });

    const errors: WorkerErrorEvent[] = [];
    const worked = collect<{ tag: string }>();
    h.track(
      await queue.work(worked.handler, {
        pollInterval: '10ms',
        onError: (event) => errors.push(event),
      })
    );

    await h.waitForDrain(await queue.id());
    // maxAttempts is 5: a poison job routed through failJob would come back
    // four more times inside this window.
    await sleep(250);

    expect(errors.map((event) => event.phase)).toEqual(['deserialize']);
    expect(errors[0].job?.id).toBe(poison.id);
    expect(worked.seen).toEqual([{ tag: '7' }]);
    expect(await h.job(poison.id)).toBeNull();
    expect(await h.job(healthy.id)).toBeNull();
  });
});

describe('polling', () => {
  it('keeps the slot alive when a pull throws', async () => {
    const queue = h.tq.defineQueue<{ n: number }>(h.name('pull-throws'));
    const published = await queue.publish({ n: 1 });

    const boom = new Error('connection reset while pulling');
    const spy = vi.spyOn(h.tq['jobService'], 'pullJobDirect').mockRejectedValueOnce(boom);

    const errors: WorkerErrorEvent[] = [];
    const worked = collect<{ n: number }>();
    h.track(
      await queue.work(worked.handler, {
        pollInterval: '10ms',
        onError: (event) => errors.push(event),
      })
    );

    await h.waitForDrain(await queue.id());

    const pullErrors = errors.filter((event) => event.phase === 'pull');
    expect(pullErrors).toHaveLength(1);
    expect(pullErrors[0].error).toBe(boom);
    // A pull that never returned a job has no job to report.
    expect(pullErrors[0].job).toBeUndefined();
    expect(pullErrors[0].slot).toBe(0);
    // The single slot polled again rather than dying with the failed pull.
    expect(spy.mock.calls.length).toBeGreaterThan(1);
    expect(worked.seen).toEqual([{ n: 1 }]);
    expect(await h.job(published.id)).toBeNull();
  });

  it('sleeps between polls on an empty queue instead of spinning', async () => {
    const queue = h.tq.defineQueue<{ n: number }>(h.name('idle-poll'));
    // Resolve the queue up front so the spy below only ever sees poll traffic.
    await queue.id();

    const spy = vi.spyOn(h.tq['jobService'], 'pullJobDirect');
    h.track(await queue.work(async () => {}, { pollInterval: '100ms' }));

    // Proving a negative, so a real wait is unavoidable: ~5 polls belong in this
    // window. Dropping the poll sleep would turn an idle worker into an
    // unthrottled UPDATE … SKIP LOCKED loop, which every other test survives.
    await sleep(500);

    expect(spy.mock.calls.length).toBeGreaterThan(0);
    expect(spy.mock.calls.length, 'polls in 500ms at a 100ms interval').toBeLessThanOrEqual(10);
  });
});

describe('settling failures', () => {
  it('does not re-run a job when recording success fails', async () => {
    const queue = h.tq.defineQueue<{ n: number }>(h.name('complete-fails'), {
      maxAttempts: 3,
      // Long enough that only the worker itself could redeliver the job.
      leaseDuration: '60s',
    });
    const published = await queue.publish({ n: 1 });

    const completeFailure = new Error('connection reset while completing');
    // Reaching into `h.tq['jobService']` is the coupling this file depends on:
    // TaskQueue builds exactly ONE JobService and shares it across every handle
    // and every worker, so a spy here is what this queue's slots really call. A
    // failure on this line means that wiring changed, not that the mock is off.
    const spy = vi
      .spyOn(h.tq['jobService'], 'completeJobDirect')
      .mockImplementationOnce(async () => {
        throw completeFailure;
      });

    const errors: WorkerErrorEvent[] = [];
    const worked = collect<{ n: number }>();
    const worker = h.track(
      await queue.work(worked.handler, {
        pollInterval: '10ms',
        onError: (event) => errors.push(event),
      })
    );

    await waitFor(() => errors.some((event) => event.phase === 'complete'), {
      message: "the 'complete' failure to be reported",
    });
    // ~25 poll cycles in which a buggy worker could redeliver the job.
    await sleep(250);
    await worker.stop({ timeout: '5s' });

    expect(spy).toHaveBeenCalledTimes(1);
    expect(worked.calls).toBe(1);
    expect(errors.filter((event) => event.phase === 'complete')).toHaveLength(1);
    // Routing a completion error into failJob would put an already-successful
    // job back to PENDING and run it again.
    expect(errors.filter((event) => event.phase === 'fail')).toEqual([]);

    const row = await h.job(published.id);
    expect(row?.status).toBe(JobStatus.PROCESSING);
    expect(row?.attempts).toBe(1);
    // Still leased — the reaper owns the decision from here. `not.toBeNull()`
    // alone was also satisfied by a lease that had already expired, which is
    // the opposite of the claim: this queue leases for 60s.
    expect(row!.leaseExpiresAt!.getTime()).toBeGreaterThan(Date.now());
  });

  it('does not re-enter the handler when recording a failure fails', async () => {
    const queue = h.tq.defineQueue<{ n: number }>(h.name('fail-fails'), {
      maxAttempts: 3,
      // Long enough that only the worker itself could redeliver the job.
      leaseDuration: '60s',
    });
    const published = await queue.publish({ n: 1 });

    // `settle` funnels complete / fail / discard through one try/catch. Only
    // the 'complete' arm was covered, so a mis-tagged phase — or a fail error
    // escaping the catch and killing the slot — went unnoticed on this one.
    const failFailure = new Error('connection reset while failing');
    const spy = vi.spyOn(h.tq['jobService'], 'failJobDirect').mockImplementationOnce(async () => {
      throw failFailure;
    });

    const errors: WorkerErrorEvent[] = [];
    let entered = 0;
    const worker = h.track(
      await queue.work(
        async () => {
          entered += 1;
          throw new Error('handler failed');
        },
        { pollInterval: '10ms', onError: (event) => errors.push(event) }
      )
    );

    await waitFor(() => errors.some((event) => event.phase === 'fail'), {
      message: "the 'fail' failure to be reported",
    });
    // ~25 poll cycles in which a buggy worker could redeliver the job.
    await sleep(250);
    await worker.stop({ timeout: '5s' });

    expect(spy).toHaveBeenCalledTimes(1);
    expect(entered).toBe(1);
    const failErrors = errors.filter((event) => event.phase === 'fail');
    expect(failErrors).toHaveLength(1);
    expect(failErrors[0].error).toBe(failFailure);
    expect(failErrors[0].job?.id).toBe(published.id);
    expect(errors.filter((event) => event.phase === 'complete')).toEqual([]);

    // The attempt was already spent at pull time; the row keeps its live lease
    // and the reaper decides what happens next.
    const row = await h.job(published.id);
    expect(row?.status).toBe(JobStatus.PROCESSING);
    expect(row?.attempts).toBe(1);
    expect(row!.leaseExpiresAt!.getTime()).toBeGreaterThan(Date.now());
  });

  it('keeps consuming when recording a discard fails', async () => {
    const queue = h.tq.defineQueue<{ n: number }>(h.name('discard-fails'), {
      maxAttempts: 3,
      leaseDuration: '60s',
    });
    const queueId = await queue.id();

    const { rows } = await h.pool.query(
      `INSERT INTO jobs (idempotency_key, payload, status, queue_id, attempts, metadata)
       VALUES ($1, $2, 'PENDING', $3, 0, '{}'::jsonb)
       RETURNING id`,
      [h.name('poison-key'), '{ this is not json', queueId]
    );
    const poisonId = Number(rows[0].id);

    const discardFailure = new Error('connection reset while discarding');
    const spy = vi
      .spyOn(h.tq['jobService'], 'discardJobDirect')
      .mockImplementationOnce(async () => {
        throw discardFailure;
      });

    const errors: WorkerErrorEvent[] = [];
    const worked = collect<{ n: number }>();
    h.track(
      await queue.work(worked.handler, {
        pollInterval: '10ms',
        onError: (event) => errors.push(event),
      })
    );

    await waitFor(() => errors.some((event) => event.phase === 'discard'), {
      message: "the 'discard' failure to be reported",
    });

    // Published only after the failed discard, so consuming it proves the slot
    // outlived the settle error rather than having raced ahead of it.
    const healthy = await queue.publish({ n: 7 });
    await waitFor(async () => (await h.job(healthy.id)) === null, {
      message: 'a job published after the failed discard to be consumed',
    });

    expect(spy).toHaveBeenCalledTimes(1);
    const discardErrors = errors.filter((event) => event.phase === 'discard');
    expect(discardErrors).toHaveLength(1);
    expect(discardErrors[0].error).toBe(discardFailure);
    expect(discardErrors[0].job?.id).toBe(poisonId);
    expect(worked.seen).toEqual([{ n: 7 }]);

    // Undeleted and still leased: the poison row is the reaper's problem now.
    const row = await h.job(poisonId);
    expect(row?.status).toBe(JobStatus.PROCESSING);
    expect(row!.leaseExpiresAt!.getTime()).toBeGreaterThan(Date.now());
  });
});

describe('stop', () => {
  it('waits for an in-flight handler to finish when given time', async () => {
    const queue = h.tq.defineQueue<{ n: number }>(h.name('stop-drain'));
    const published = await queue.publish({ n: 1 });

    const entered = deferred();
    const release = deferred();
    let finished = false;

    const worker = h.track(
      await queue.work(
        async () => {
          entered.resolve();
          await release.promise;
          finished = true;
        },
        { pollInterval: '10ms' }
      )
    );

    await entered.promise;
    const timer = setTimeout(() => release.resolve(), 100);
    timer.unref?.();

    const result = await worker.stop({ timeout: '10s' });

    expect(result.drained).toBe(true);
    expect(finished).toBe(true);
    expect(worker.isRunning()).toBe(false);
    // A drained handler still gets its success recorded before stop returns.
    expect(await h.job(published.id)).toBeNull();
  });

  it('reports drained: false when a handler outlives the stop timeout', async () => {
    const queue = h.tq.defineQueue<{ n: number }>(h.name('stop-stuck'));
    const published = await queue.publish({ n: 1 });

    const entered = deferred();
    const stuck = deferred();

    const worker = h.track(
      await queue.work(
        async () => {
          entered.resolve();
          await stuck.promise;
        },
        { pollInterval: '10ms' }
      )
    );

    await entered.promise;
    const startedAt = Date.now();
    const result = await worker.stop({ timeout: '300ms' });
    const elapsed = Date.now() - startedAt;

    expect(result.drained).toBe(false);
    // Both bounds matter. Without the lower one, a stop() that abandoned its
    // in-flight handlers the instant it was called would pass this test.
    expect(elapsed, 'stop waited for the deadline').toBeGreaterThanOrEqual(250);
    expect(elapsed, 'stop gave up at the deadline').toBeLessThan(1_500);
    expect((await h.job(published.id))?.status).toBe(JobStatus.PROCESSING);

    // Unwedge it so teardown is not left holding a half-finished job.
    stuck.resolve();
    await waitFor(async () => (await h.job(published.id)) === null, {
      message: 'the released handler to settle its job',
    });
  });

  it('returns promptly on an idle worker instead of waiting out the poll interval', async () => {
    const queue = h.tq.defineQueue<{ n: number }>(h.name('stop-idle'));
    const worker = h.track(await queue.work(async () => {}, { pollInterval: '30s' }));

    // Let the slot find an empty queue and park in its poll sleep.
    await sleep(150);

    const startedAt = Date.now();
    const result = await worker.stop({ timeout: '20s' });
    const elapsed = Date.now() - startedAt;

    expect(result.drained).toBe(true);
    // Anything near 30s means the poll sleep is not interruptible.
    expect(elapsed).toBeLessThan(2_000);
  });

  it('is safe to call twice', async () => {
    const queue = h.tq.defineQueue<{ n: number }>(h.name('stop-twice'));
    const worked = collect<{ n: number }>();
    const worker = h.track(await queue.work(worked.handler, { pollInterval: '10ms' }));

    expect(await worker.stop({ timeout: '5s' })).toEqual({ drained: true });
    expect(await worker.stop({ timeout: '5s' })).toEqual({ drained: true });
    expect(worker.isRunning()).toBe(false);

    // The return shape alone says nothing about what the second stop did to the
    // worker. ~25 poll cycles for something that must stay stopped.
    const published = await queue.publish({ n: 1 });
    await sleep(250);

    expect(worked.calls).toBe(0);
    expect((await h.job(published.id))?.status).toBe(JobStatus.PENDING);
  });

  it('stops pulling new jobs once stopped', async () => {
    const queue = h.tq.defineQueue<{ n: number }>(h.name('stop-pulling'));
    const first = await queue.publish({ n: 1 });

    const worked = collect<{ n: number }>();
    const worker = h.track(await queue.work(worked.handler, { pollInterval: '10ms' }));

    await worked.waitForCalls(1);
    await waitFor(async () => (await h.job(first.id)) === null, {
      message: 'the first job to be settled',
    });
    await worker.stop({ timeout: '5s' });

    const second = await queue.publish({ n: 2 });
    // ~25 poll cycles for a worker that is supposed to be stopped.
    await sleep(250);

    expect(worked.calls).toBe(1);
    expect((await h.job(second.id))?.status).toBe(JobStatus.PENDING);
  });

  it('waits for every slot of a multi-slot worker, not just the first', async () => {
    const queue = h.tq.defineQueue<{ n: number }>(h.name('stop-drain-many'));
    const published = await queue.publishMany([{ n: 0 }, { n: 1 }, { n: 2 }, { n: 3 }]);

    const gate = barrier(4);
    const release = deferred();
    const abortedAtExit: boolean[] = [];

    const worker = h.track(
      await queue.work(
        async (_payload, ctx) => {
          // All four have to be inside together, or `await Promise.all(slots)`
          // is only ever awaiting one promise and a regression that awaited
          // slots[0] would pass.
          await gate.wait();
          await release.promise;
          abortedAtExit.push(ctx.signal.aborted);
        },
        { concurrency: 4, pollInterval: '10ms' }
      )
    );

    await waitFor(() => gate.opened, { message: 'all four slots to be in flight' });
    // Released on a timer rather than by stop(), so the wait stop reports is
    // one it genuinely sat through.
    const timer = setTimeout(() => release.resolve(), 150);
    timer.unref?.();

    const result = await worker.stop({ timeout: '10s' });

    expect(result.drained).toBe(true);
    // One shared AbortController, observed by four handlers instead of one.
    expect(abortedAtExit).toEqual([true, true, true, true]);
    for (const job of published) {
      expect(await h.job(job.id)).toBeNull();
    }
  });

  it('reports drained: false when one slot of four outlives the timeout', async () => {
    const queue = h.tq.defineQueue<{ n: number }>(h.name('stop-stuck-many'));
    const published = await queue.publishMany([{ n: 0 }, { n: 1 }, { n: 2 }, { n: 3 }]);

    const gate = barrier(4);
    const stuck = deferred();
    let entered = 0;
    let hungId: number | undefined;

    const worker = h.track(
      await queue.work(
        async (_payload, ctx) => {
          const index = entered++;
          await gate.wait();
          // Whichever job the fourth slot happened to draw is the one that hangs.
          if (index === 3) {
            hungId = ctx.id;
            await stuck.promise;
          }
        },
        { concurrency: 4, pollInterval: '10ms' }
      )
    );

    await waitFor(() => gate.opened, { message: 'all four slots to be in flight' });
    const result = await worker.stop({ timeout: '1s' });

    expect(result.drained).toBe(false);
    expect(hungId).toBeDefined();

    // The three that returned still recorded their success before the deadline.
    const settled = published.filter((job) => job.id !== hungId);
    expect(settled).toHaveLength(3);
    for (const job of settled) {
      expect(await h.job(job.id)).toBeNull();
    }
    expect((await h.job(hungId!))?.status).toBe(JobStatus.PROCESSING);

    // Unwedge it so teardown is not left holding a half-finished job.
    stuck.resolve();
    await waitFor(async () => (await h.job(hungId!)) === null, {
      message: 'the released handler to settle its job',
    });
  });

  it('waits without a deadline when stop() is given no timeout', async () => {
    const queue = h.tq.defineQueue<{ n: number }>(h.name('stop-unbounded'));
    const published = await queue.publish({ n: 1 });

    const entered = deferred();
    const release = deferred();
    let finished = false;

    const worker = h.track(
      await queue.work(
        async () => {
          entered.resolve();
          await release.promise;
          finished = true;
        },
        { pollInterval: '10ms' }
      )
    );

    await entered.promise;
    // The no-timeout branch has no deadline of its own — documented in the
    // README, taken by nothing else here, not even harness teardown — so the
    // handler has to be released by something other than the stop call.
    const timer = setTimeout(() => release.resolve(), 150);
    timer.unref?.();

    const result = await worker.stop();

    expect(result).toEqual({ drained: true });
    expect(finished).toBe(true);
    expect(await h.job(published.id)).toBeNull();
  });
});

describe('start', () => {
  it('yields a single running worker when start() is called twice concurrently', async () => {
    const queue = h.tq.defineQueue<{ n: number }>(h.name('double-start'));
    await queue.publishMany([{ n: 0 }, { n: 1 }]);

    const tracker = new ConcurrencyTracker();
    const worker = h.track(
      await queue.work(
        async () => {
          tracker.enter();
          try {
            await sleep(60);
          } finally {
            tracker.exit();
          }
        },
        { concurrency: 1, pollInterval: '10ms', autoStart: false }
      )
    );

    await Promise.all([worker.start(), worker.start()]);
    expect(worker.isRunning()).toBe(true);

    await h.waitForDrain(await queue.id());

    // A second set of slots would show up as two handlers in flight at once.
    expect(tracker.peak).toBe(1);
    expect(tracker.entered).toBe(2);
  });

  it('leaves the worker stopped when stop() interleaves with an in-flight start()', async () => {
    const queue = h.tq.defineQueue<{ n: number }>(h.name('stop-during-start'));

    const worked = collect<{ n: number }>();
    const worker = h.track(
      await queue.work(worked.handler, { pollInterval: '10ms', autoStart: false })
    );

    // Deliberately not awaited: stop() has to cope with a start that has not
    // finished resolving the queue yet.
    const starting = worker.start();
    const result = await worker.stop({ timeout: '10s' });
    await starting;

    expect(result.drained).toBe(true);
    expect(worker.isRunning()).toBe(false);

    const published = await queue.publish({ n: 1 });
    await sleep(250);

    expect(worked.calls).toBe(0);
    expect((await h.job(published.id))?.status).toBe(JobStatus.PENDING);
    expect(worker.isRunning()).toBe(false);
  });

  it('consumes again after a successful stop and a restart', async () => {
    const queue = h.tq.defineQueue<{ n: number }>(h.name('restart'));
    const first = await queue.publish({ n: 1 });

    const seen: number[] = [];
    const abortedOnEntry: boolean[] = [];
    const worker = h.track(
      await queue.work(
        async (payload, ctx) => {
          seen.push(payload.n);
          abortedOnEntry.push(ctx.signal.aborted);
        },
        { pollInterval: '10ms' }
      )
    );

    await h.waitForDrain(await queue.id());
    expect(await worker.stop({ timeout: '5s' })).toEqual({ drained: true });
    expect(worker.isRunning()).toBe(false);

    await worker.start();
    expect(worker.isRunning()).toBe(true);

    const second = await queue.publish({ n: 2 });
    await waitFor(() => seen.length === 2, {
      message: 'the restarted worker to consume a second job',
    });
    await h.waitForDrain(await queue.id());

    expect(seen).toEqual([1, 2]);
    // The interesting half: stop() aborted the first controller, so doStart has
    // to build a new one. Reusing it would hand every later handler an
    // already-aborted signal AND make the poll sleep return synchronously — a
    // hot spin that nothing else in this file would notice.
    expect(abortedOnEntry).toEqual([false, false]);
    expect(await h.job(first.id)).toBeNull();
    expect(await h.job(second.id)).toBeNull();
  });
});

describe('ctx.signal', () => {
  it('aborts an in-flight handler on stop so it can bail out early', async () => {
    const queue = h.tq.defineQueue<{ n: number }>(h.name('signal'));
    const published = await queue.publish({ n: 1 });

    const entered = deferred();
    let abortedWhileWaiting = false;

    const worker = h.track(
      await queue.work(
        async (_payload, ctx) => {
          entered.resolve();
          // Would hang for ever without the abort — nothing else resolves it.
          await new Promise<void>((resolve) => {
            ctx.signal.addEventListener('abort', () => resolve(), { once: true });
          });
          abortedWhileWaiting = ctx.signal.aborted;
        },
        { pollInterval: '10ms' }
      )
    );

    await entered.promise;
    const result = await worker.stop({ timeout: '10s' });

    expect(result.drained).toBe(true);
    expect(abortedWhileWaiting).toBe(true);
    // The handler returned normally, so the job counts as done.
    expect(await h.job(published.id)).toBeNull();
  });
});

describe('autoStart', () => {
  it('consumes nothing until start() is called', async () => {
    const queue = h.tq.defineQueue<{ n: number }>(h.name('autostart'));
    const published = await queue.publish({ n: 1 });

    const worked = collect<{ n: number }>();
    const worker = h.track(
      await queue.work(worked.handler, { pollInterval: '10ms', autoStart: false })
    );

    expect(worker.isRunning()).toBe(false);
    // ~25 poll cycles for a worker that was never started.
    await sleep(250);
    expect(worked.calls).toBe(0);
    expect((await h.job(published.id))?.status).toBe(JobStatus.PENDING);

    await worker.start();
    expect(worker.isRunning()).toBe(true);

    await h.waitForDrain(await queue.id());
    expect(worked.seen).toEqual([{ n: 1 }]);
  });
});

describe('reaper', () => {
  it('returns an expired lease to PENDING and lets the job be picked up again', async () => {
    const queue = h.tq.defineQueue<{ n: number }>(h.name('reaper-recovers'), {
      maxAttempts: 3,
      leaseDuration: '300ms',
    });
    const published = await queue.publish({ n: 1 });

    const leaseSeqs: number[] = [];
    const errors: WorkerErrorEvent[] = [];
    let seqWhileReclaimed: number | null = null;

    h.track(
      await queue.work(
        async (_payload, ctx) => {
          leaseSeqs.push(ctx.job.lockSeq!);
          if (leaseSeqs.length === 1) {
            // Hold past the lease without touching the abort signal: this is
            // the "worker went dark" case the reaper exists for.
            await waitFor(async () => (await h.job(ctx.id))?.status === JobStatus.PENDING, {
              timeout: 20_000,
              message: 'the reaper to reclaim the expired lease',
            });
            seqWhileReclaimed = (await h.job(ctx.id))!.leaseSeq;
          }
        },
        { pollInterval: '10ms', onError: (event) => errors.push(event) }
      )
    );

    const reaper = h.tq.startReaper({ interval: '50ms' });
    await h.waitForDrain(await queue.id(), { timeout: 30_000 });
    await reaper.stop();

    expect(leaseSeqs).toHaveLength(2);
    // lease_seq is a fence token, not a lease counter: recovery must not reset
    // it, or the zombie worker's token would still be valid for the new owner.
    expect(seqWhileReclaimed).toBe(leaseSeqs[0]);
    expect(leaseSeqs[1]).toBeGreaterThan(leaseSeqs[0]);

    // The zombie's completion was fenced off rather than silently applied.
    expect(errors.some((event) => event.phase === 'complete')).toBe(true);
    expect(await h.job(published.id)).toBeNull();
  });

  it('discards an attempt-exhausted job instead of resetting it', async () => {
    const queue = h.tq.defineQueue<{ n: number }>(h.name('reaper-exhausted'), {
      maxAttempts: 2,
      leaseDuration: '10s',
    });
    const exhausted = await queue.publish({ n: 1 });
    const retryable = await queue.publish({ n: 2 });

    // Both leases are dead; only one still has budget left. A lease expiry has
    // already cost an attempt, so `attempts` is what decides.
    await simulateCrashedWorker(exhausted.id, { attempts: 2, leaseSeq: 2 });
    await simulateCrashedWorker(retryable.id, { attempts: 1, leaseSeq: 1 });

    const reaper = h.tq.startReaper({ interval: '50ms' });
    await waitFor(async () => (await h.job(exhausted.id)) === null, {
      message: 'the exhausted job to be discarded',
    });
    await waitFor(async () => (await h.job(retryable.id))?.status === JobStatus.PENDING, {
      message: 'the retryable job to be reset',
    });
    await reaper.stop();

    const survivor = await h.job(retryable.id);
    expect(survivor?.attempts).toBe(1);
    expect(survivor?.leaseExpiresAt).toBeNull();
    expect(survivor?.leaseSeq).toBe(1);
  });

  it('reclaims nothing while the lease is still live', async () => {
    const queue = h.tq.defineQueue<{ n: number }>(h.name('reaper-live-lease'), {
      leaseDuration: '60s',
    });
    const published = await queue.publish({ n: 1 });
    await h.pool.query(
      `UPDATE jobs SET status = 'PROCESSING', attempts = 1, lease_seq = 1,
                       lease_expires_at = now() + interval '60 seconds'
       WHERE id = $1`,
      [published.id]
    );

    const reaper = h.tq.startReaper({ interval: '10s' });
    expect(await reaper.runOnce()).toEqual([]);
    await reaper.stop();

    // The empty array on its own proves very little — recoverJobs never reports
    // the jobs it discards, and startReaper's own first pass races the runOnce
    // above. The row is the claim: untouched, still leased, still on attempt 1.
    const row = await h.job(published.id);
    expect(row).not.toBeNull();
    expect(row!.status).toBe(JobStatus.PROCESSING);
    expect(row!.attempts).toBe(1);
    expect(row!.leaseSeq).toBe(1);
    expect(row!.leaseExpiresAt!.getTime()).toBeGreaterThan(Date.now());
  });

  it('reclaims at most batchSize jobs per pass and never reports a discard', async () => {
    const queue = h.tq.defineQueue<{ n: number }>(h.name('reaper-batch'), {
      maxAttempts: 2,
      leaseDuration: '10s',
    });
    const queueId = await queue.id();

    // Started before anything is planted: startReaper fires a pass immediately,
    // and letting it find an empty table is what makes every reclaim below
    // attributable to an explicit runOnce(). The next scheduled pass is 10s out.
    const reaper = h.tq.startReaper({ interval: '10s', batchSize: 2 });
    await sleep(100);

    // recoverJobs takes its batch newest-first, so the exhausted job is planted
    // oldest: it lands in the LAST pass, where it can be seen to be deleted
    // rather than merely not reached yet.
    const exhausted = await queue.publish({ n: -1 });
    await simulateCrashedWorker(exhausted.id, { attempts: 2, leaseSeq: 2 });
    await backdateCreatedAt(exhausted.id, 60);

    const retryable: number[] = [];
    for (let n = 0; n < 5; n++) {
      const job = await queue.publish({ n });
      await simulateCrashedWorker(job.id, { attempts: 1 });
      await backdateCreatedAt(job.id, 50 - n * 10);
      retryable.push(job.id);
    }

    // RETURNING has no ordering guarantee, so batch membership is the claim.
    const sorted = (ids: number[]): number[] => [...ids].sort((a, b) => a - b);

    const first = await reaper.runOnce();
    expect(first, 'batchSize caps the pass at 2').toHaveLength(2);
    // Exactly the rows that flipped: an id reported without the row behind it
    // having been reset would pass a length check on its own.
    const pendingAfterFirst = (await h.jobs(queueId))
      .filter((row) => row.status === JobStatus.PENDING)
      .map((row) => row.id);
    expect(sorted(pendingAfterFirst)).toEqual(sorted(first));
    expect(sorted(first)).toEqual(sorted([retryable[3], retryable[4]]));

    const second = await reaper.runOnce();
    expect(sorted(second)).toEqual(sorted([retryable[1], retryable[2]]));

    // Two expired rows are left — the last retryable one and the exhausted one.
    // Both are in this pass; only one comes back.
    const third = await reaper.runOnce();
    expect(third).toEqual([retryable[0]]);
    expect(await reaper.runOnce()).toEqual([]);
    await reaper.stop();

    const reported = [...first, ...second, ...third];
    expect(sorted(reported)).toEqual(sorted(retryable));
    expect(reported).not.toContain(exhausted.id);
    expect(await h.job(exhausted.id)).toBeNull();

    for (const id of retryable) {
      const row = await h.job(id);
      expect(row?.status).toBe(JobStatus.PENDING);
      expect(row?.leaseExpiresAt).toBeNull();
    }
  });

  it('stops cleanly and flips isRunning', async () => {
    const queue = h.tq.defineQueue<{ n: number }>(h.name('reaper-stop'));
    const published = await queue.publish({ n: 1 });

    const reaper = h.tq.startReaper({ interval: '20ms' });
    expect(reaper.isRunning()).toBe(true);

    await reaper.stop();
    expect(reaper.isRunning()).toBe(false);

    // Only now is there something to reclaim. A stopped reaper must ignore it.
    await simulateCrashedWorker(published.id, { attempts: 1 });
    await expireLease(published.id);
    // ~12 intervals of a loop that is supposed to be gone.
    await sleep(250);

    expect((await h.job(published.id))?.status).toBe(JobStatus.PROCESSING);
  });
});

describe('multiple workers on one queue', () => {
  it('delivers every job exactly once across both workers', async () => {
    const queue = h.tq.defineQueue<{ n: number }>(h.name('two-workers'));
    const total = 16;

    const hold = (): Promise<void> => sleep(5);
    const first: Collector<{ n: number }> = collect(hold);
    const second: Collector<{ n: number }> = collect(hold);

    // Both workers are started against an EMPTY queue and the backlog is
    // published afterwards. Publishing first let whichever worker started
    // sooner drain the lot, so "both pulled" was decided by a start-up race
    // rather than by the queue actually being shared.
    h.track(
      await queue.work(first.handler, { concurrency: 2, pollInterval: '10ms', name: 'worker-a' })
    );
    h.track(
      await queue.work(second.handler, { concurrency: 2, pollInterval: '10ms', name: 'worker-b' })
    );

    await queue.publishMany(Array.from({ length: total }, (_, n) => ({ n })));
    await Promise.all([first.waitForCalls(1), second.waitForCalls(1)]);

    await h.waitForDrain(await queue.id(), { timeout: 30_000 });
    // A duplicate delivery would land after the drain, not before it.
    await sleep(150);

    const seen = [...first.seen, ...second.seen].map((payload) => payload.n).sort((a, b) => a - b);
    expect(seen).toEqual(Array.from({ length: total }, (_, n) => n));
    expect(first.calls).toBeGreaterThan(0);
    expect(second.calls).toBeGreaterThan(0);
  });
});

describe('group concurrency', () => {
  it('never runs more than one job per group when each group is capped at one', async () => {
    const queue = h.tq.defineQueue<{ group: string; n: number }>(h.name('group-cap'), {
      requiresGroupId: true,
    });
    const queueId = await queue.id();
    const groups = ['tenant-a', 'tenant-b'];

    // Interleaved so the two groups compete for the worker's slots.
    for (let n = 0; n < 4; n++) {
      for (const group of groups) {
        await queue.publish({ group, n }, { group: { id: group, concurrency: 1 } });
      }
    }

    const sampler = sampleGroupConcurrency(h.pool, queueId, groups, 5);
    // The queue-wide twin: the per-group peaks below stay at 1 whether the caps
    // work or the whole queue has been serialised by accident. Only this one
    // can tell those apart.
    const queueSampler = new ProcessingSampler(h.pool, queueId, 5);
    queueSampler.start();
    const seen: string[] = [];

    try {
      h.track(
        await queue.work(
          async (payload) => {
            seen.push(`${payload.group}:${payload.n}`);
            // Wide enough for the sampler to observe the overlap window.
            await sleep(40);
          },
          { concurrency: 4, pollInterval: '10ms' }
        )
      );

      await h.waitForDrain(queueId, { timeout: 30_000 });
    } finally {
      // In a finally: a failed drain must not leave two intervals polling the
      // pool while the harness tears it down.
      sampler.stop();
      queueSampler.stop();
    }

    // Every failed poll is swallowed, so peaks are only meaningful next to this.
    expect(sampler.samples).toBeGreaterThan(0);
    for (const group of groups) {
      expect(sampler.peaks.get(group)).toBe(1);
    }
    // Two groups capped at one each, on a four-slot worker, must still overlap.
    expect(queueSampler.peak, 'both groups ran at once').toBe(2);

    expect(seen).toHaveLength(8);
    const counters = await readGroupCounters(h.pool, queueId);
    expect(counters).toHaveLength(2);
    expect(counters.every((g) => g.running === 0)).toBe(true);
    await expectNoNegativeCounters(h.pool, queueId);
  });
});
