/**
 * Publishing: what `publish` / `publishMany` actually write, what they hand
 * back, and how they behave at the edges — serialization, idempotency keys,
 * metadata and group limits.
 *
 * Transactional publish has its own suite (transaction.test.ts); the only
 * transaction-shaped cases here are the "foreign executor" ones, which exist to
 * prove publishing survives a client that did not come from this library's pool.
 */
import type { Pool, PoolClient } from 'pg';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { TaskQueue } from '../src/client/task-queue';
import { Serializer } from '../src/client/serializer';
import { BadParamInputError } from '../src/domain/errors';
import { JobStatus } from '../src/domain/job';
import { silentLogger } from '../src/domain/logger';
import { createPool } from '../src/repository/postgresql/connection';
import { JobRepository } from '../src/repository/postgresql/job.repository';
import { pgConfig } from './support/harness';
import { readGroupCounters } from './support/invariants';
import {
  TaskQueueHarness,
  collect,
  createTaskQueueHarness,
  sleep,
  uniqueName,
} from './support/task-queue';

// Cleared by afterEach, so a beforeEach that throws cannot make teardown close
// the *previous* test's harness and bury the real error under "Called end on
// pool more than once".
let h: TaskQueueHarness | undefined;

beforeEach(async () => {
  h = await createTaskQueueHarness();
});

afterEach(async () => {
  const current = h;
  h = undefined;
  await current?.close();
});

/** What a consumer of the default serializer would see for this stored job. */
const decode = (raw: string): unknown => JSON.parse(raw);

/** The rejection reason, or null if the promise resolved. */
const rejection = async (promise: Promise<unknown>): Promise<any> =>
  promise.then(
    () => null,
    (error) => error
  );

/**
 * An `Executor` that runs everything on `pool` and keeps the first line of each
 * statement. Both the "bare object" test and the batch test need to assert on
 * what was actually sent, not only on the rows that ended up in the table.
 */
function recordingExecutor(pool: Pool): { statements: string[]; query: Pool['query'] } {
  const statements: string[] = [];
  return {
    statements,
    query: ((text: string, values?: any[]) => {
      statements.push(text.trim());
      return pool.query(text, values);
    }) as Pool['query'],
  };
}

// Matched anywhere in the statement, not anchored: the insert arrives inside
// a `WITH ins AS (INSERT INTO jobs …)` when group-limit seeding rides along.
const insertsIntoJobs = (statements: string[]): number =>
  statements.filter((sql) => /\bINSERT INTO jobs\b/i.test(sql)).length;

/**
 * `bytes` ASCII characters with no structure for pglz to find.
 *
 * Index entries are compressed before the btree length check, so `'k'.repeat(n)`
 * squeezes down to a few dozen bytes and inserts happily at any length — a key
 * built that way cannot show where the real ceiling is.
 */
function incompressibleKey(bytes: number): string {
  const alphabet = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_';
  let state = 0x9e3779b9;
  let out = '';
  while (out.length < bytes) {
    // xorshift32: identical on every run, but incompressible in practice.
    state ^= state << 13;
    state >>>= 0;
    state ^= state >>> 17;
    state ^= state << 5;
    state >>>= 0;
    out += alphabet[state % alphabet.length];
  }
  return out;
}

describe('publish', () => {
  it('writes exactly one PENDING row with no lease and no shard yet', async () => {
    const q = h!.tq.defineQueue<{ to: string }>(h!.name('one-row'));
    const queueId = await q.id();

    const published = await q.publish({ to: 'a@b.c' });

    expect(await h!.jobCount(queueId)).toBe(1);

    const [row] = await h!.jobs(queueId);
    expect(row.status).toBe(JobStatus.PENDING);
    expect(row.attempts).toBe(0);
    // A lease is issued at pull time; nothing about it may be set at publish.
    expect(row.leaseSeq).toBeNull();
    expect(row.leaseExpiresAt).toBeNull();
    // The shard is chosen by the puller, not the publisher.
    expect(row.queueShardNo).toBeNull();
    expect(row.groupId).toBeNull();
    expect(decode(row.payload)).toEqual({ to: 'a@b.c' });
    expect(row.id).toBe(published.id);
  });

  it('returns a job whose fields match the stored row', async () => {
    const q = h!.tq.defineQueue<{ n: number }>(h!.name('mirror'));
    const queueId = await q.id();

    const published = await q.publish({ n: 42 });
    const stored = await h!.job(published.id);

    expect(stored).not.toBeNull();
    expect(published.id).toBe(stored!.id);
    expect(published.idempotencyKey).toBe(stored!.idempotencyKey);
    expect(published.payload).toBe(stored!.payload);
    expect(published.status).toBe(stored!.status);
    expect(published.attempts).toBe(stored!.attempts);
    expect(published.queueId).toBe(queueId);
    expect(published.deduplicated).toBe(false);
    expect(published.lockSeq).toBeNull();
    expect(published.leaseExpiresAt).toBeNull();
    // Terminal jobs are deleted rather than stamped, so a live job never has one.
    expect(published.completedAt).toBeNull();
    expect(published.createdAt).toBeInstanceOf(Date);
  });

  it('treats tq.publish(name, payload) as a shorthand for the handle', async () => {
    const name = h!.name('shorthand');

    const viaClient = await h!.tq.publish(name, { n: 1 });
    const viaHandle = await h!.tq.defineQueue<{ n: number }>(name).publish({ n: 2 });

    // Same lazily-created queue, not two of them.
    const queueId = await h!.tq.defineQueue(name).id();
    expect(viaClient.queueId).toBe(queueId);
    expect(viaHandle.queueId).toBe(queueId);
    expect(await h!.jobCount(queueId)).toBe(2);

    const { rows } = await h!.pool.query('SELECT count(*)::int AS n FROM queues WHERE name = $1', [
      name,
    ]);
    expect(rows[0].n).toBe(1);
  });
});

describe('payload serialization', () => {
  it('round-trips nested objects, arrays and nulls', async () => {
    const q = h!.tq.defineQueue<Record<string, unknown>>(h!.name('nested'));
    const payload = {
      order: { id: 'o-1', lines: [{ sku: 'a', qty: 2 }, { sku: 'b', qty: 0 }] },
      tags: ['x', 'y', 'z'],
      cancelledAt: null,
      flags: { retryable: true, urgent: false },
      depth: [[1, [2, [3]]]],
    };

    const published = await q.publish(payload);
    const stored = await h!.job(published.id);

    expect(decode(stored!.payload)).toEqual(payload);
  });

  it('round-trips unicode and control characters', async () => {
    const q = h!.tq.defineQueue<{ text: string }>(h!.name('unicode'));
    const payload = {
      // Emoji (surrogate pair), CJK, RTL, a combining mark, and a NUL, which a
      // Postgres TEXT column cannot store raw but JSON escapes before it lands.
      text: '\u{1F642} \u6F22\u5B57 \u0645\u0631\u062D\u0628\u0627 e\u0301 tab:\t nul:\u0000 quote:" backslash:\\',
    };

    const published = await q.publish(payload);
    const stored = await h!.job(published.id);

    // The escape must survive the round trip through TEXT, not be stripped.
    expect(stored!.payload).toContain('\\u0000');
    expect(decode(stored!.payload)).toEqual(payload);
  });

  it('stores an empty object as an empty object', async () => {
    const q = h!.tq.defineQueue<Record<string, never>>(h!.name('empty'));

    const published = await q.publish({});
    const stored = await h!.job(published.id);

    expect(stored!.payload).toBe('{}');
    expect(decode(stored!.payload)).toEqual({});
  });

  it('carries a 100KB payload without truncation', async () => {
    const q = h!.tq.defineQueue<{ blob: string }>(h!.name('big'));
    // Mixed-width characters so a byte/char confusion anywhere would show up.
    const payload = { blob: 'ab\u{1F642}'.repeat(25_000) };
    expect(payload.blob.length).toBe(100_000);

    const published = await q.publish(payload);
    const stored = await h!.job(published.id);

    expect(decode(stored!.payload)).toEqual(payload);
  });

  it('rejects undefined before inserting anything into jobs', async () => {
    const q = h!.tq.defineQueue<unknown>(h!.name('undef'));
    const queueId = await q.id();

    await expect(q.publish(undefined)).rejects.toThrow(BadParamInputError);
    // A function also stringifies to `undefined` and must fail the same way.
    await expect(q.publish(() => 1)).rejects.toThrow(BadParamInputError);

    expect(await h!.jobCount(queueId)).toBe(0);
  });

  it('rejects values JSON cannot represent before inserting anything into jobs', async () => {
    const q = h!.tq.defineQueue<unknown>(h!.name('circular'));
    const queueId = await q.id();

    const circular: Record<string, unknown> = { name: 'loop' };
    circular.self = circular;

    // JSON.stringify throws for these, so the failure surfaces as a raw
    // TypeError rather than one of the library's own error types.
    await expect(q.publish(circular)).rejects.toThrow(TypeError);
    await expect(q.publish({ big: BigInt(1) })).rejects.toThrow(TypeError);

    expect(await h!.jobCount(queueId)).toBe(0);
  });

  it('uses the queue serializer instead of JSON when one is configured', async () => {
    interface Point {
      x: number;
      y: number;
    }
    // Deliberately not JSON, so the assertion cannot pass by accident.
    const pipeSerializer: Serializer<Point> = {
      serialize: (value) => `${value.x}|${value.y}`,
      deserialize: (raw) => {
        const [x, y] = raw.split('|');
        return { x: Number(x), y: Number(y) };
      },
    };

    const q = h!.tq.defineQueue<Point>(h!.name('custom-ser'), { serializer: pipeSerializer });
    const published = await q.publish({ x: 3, y: 4 });
    const stored = await h!.job(published.id);

    expect(stored!.payload).toBe('3|4');
    expect(pipeSerializer.deserialize(stored!.payload)).toEqual({ x: 3, y: 4 });
  });
});

describe('idempotency keys', () => {
  it('gives every publish a distinct key by default', async () => {
    const q = h!.tq.defineQueue<{ n: number }>(h!.name('auto-key'));
    const queueId = await q.id();

    // Identical payloads: only the generated key can distinguish them.
    const published = [
      await q.publish({ n: 1 }),
      await q.publish({ n: 1 }),
      await q.publish({ n: 1 }),
    ];

    expect(new Set(published.map((job) => job.idempotencyKey)).size).toBe(3);
    for (const job of published) {
      expect(job.deduplicated).toBe(false);
      expect(job.idempotencyKey).toMatch(
        /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/
      );
    }
    expect(await h!.jobCount(queueId)).toBe(3);
  });

  it('returns the original job and inserts nothing when a key repeats', async () => {
    const q = h!.tq.defineQueue<{ n: number }>(h!.name('dedup'));
    const queueId = await q.id();
    const key = uniqueName('key');

    const first = await q.publish({ n: 1 }, { idempotencyKey: key });
    const second = await q.publish({ n: 2 }, { idempotencyKey: key });

    expect(first.deduplicated).toBe(false);
    expect(second.deduplicated).toBe(true);
    expect(second.id).toBe(first.id);
    // The second payload is dropped, not merged in.
    expect(decode(second.payload)).toEqual({ n: 1 });
    expect(await h!.jobCount(queueId)).toBe(1);
    expect(decode((await h!.job(first.id))!.payload)).toEqual({ n: 1 });
  });

  it('deduplicates the same key across different queues, because the constraint is global', async () => {
    const a = h!.tq.defineQueue<{ n: number }>(h!.name('key-queue-a'));
    const b = h!.tq.defineQueue<{ n: number }>(h!.name('key-queue-b'));
    const [aId, bId] = [await a.id(), await b.id()];
    const key = uniqueName('shared-key');

    const first = await a.publish({ n: 1 }, { idempotencyKey: key });
    const second = await b.publish({ n: 2 }, { idempotencyKey: key });

    // Sharp edge: jobs.idempotency_key is UNIQUE over the whole table, so
    // publishing the same key to a *different* queue silently hands back the
    // other queue's job and enqueues nothing here.
    expect(second.deduplicated).toBe(true);
    expect(second.id).toBe(first.id);
    expect(second.queueId).toBe(aId);
    expect(await h!.jobCount(aId)).toBe(1);
    expect(await h!.jobCount(bId)).toBe(0);
  });

  it('frees the key once the job reaches a terminal state', async () => {
    const q = h!.tq.defineQueue<{ n: number }>(h!.name('key-reuse'));
    const queueId = await q.id();
    const key = uniqueName('reusable');

    const first = await q.publish({ n: 1 }, { idempotencyKey: key });

    const seen = collect<{ n: number }>();
    const worker = h!.track(await q.work(seen.handler, { pollInterval: '10ms' }));
    await h!.waitForDrain(queueId);
    // Stop before republishing so the worker cannot race in and delete the
    // second job before the assertions read it.
    await worker.stop({ timeout: '5s' });
    expect(seen.seen).toEqual([{ n: 1 }]);

    const second = await q.publish({ n: 2 }, { idempotencyKey: key });

    expect(second.deduplicated).toBe(false);
    expect(second.id).not.toBe(first.id);
    expect(decode(second.payload)).toEqual({ n: 2 });
    expect(await h!.jobCount(queueId)).toBe(1);
  });

  it('inserts exactly one row when two clients publish the same key at once', async () => {
    const other = TaskQueue.create({ pool: h!.pool, logger: silentLogger });
    try {
      const name = h!.name('race-key');
      const mine = h!.tq.defineQueue<{ from: string }>(name);
      const theirs = other.defineQueue<{ from: string }>(name);
      // Resolve up front so the race is purely about the insert, not about two
      // clients creating the queue.
      const [queue] = await Promise.all([mine.resolve(), theirs.resolve()]);
      const key = uniqueName('contended');

      const [a, b] = await Promise.all([
        mine.publish({ from: 'a' }, { idempotencyKey: key }),
        theirs.publish({ from: 'b' }, { idempotencyKey: key }),
      ]);

      expect(a.id).toBe(b.id);
      expect(await h!.jobCount(queue.id)).toBe(1);
      // Exactly one caller may claim it inserted the row. Promise.all makes the
      // two publishes overlap in practice but does not guarantee it, so this is
      // written to hold either way; the strictly sequential case is covered by
      // "returns the original job and inserts nothing when a key repeats".
      expect([a, b].filter((result) => !result.deduplicated)).toHaveLength(1);
    } finally {
      await other.close();
    }
  });
});

describe('metadata', () => {
  it('stores metadata and returns it on the published job', async () => {
    const q = h!.tq.defineQueue<{ n: number }>(h!.name('meta'));
    const metadata = {
      source: 'producer.test',
      attemptBudget: 3,
      nested: { region: 'eu-west-1', tags: ['a', 'b'] },
      nothing: null,
    };

    const published = await q.publish({ n: 1 }, { metadata });

    expect(published.metadata).toEqual(metadata);

    const { rows } = await h!.pool.query('SELECT metadata FROM jobs WHERE id = $1', [published.id]);
    expect(rows[0].metadata).toEqual(metadata);
  });

  it('defaults to an empty object', async () => {
    const q = h!.tq.defineQueue<{ n: number }>(h!.name('meta-default'));

    const published = await q.publish({ n: 1 });

    expect(published.metadata).toEqual({});
    const { rows } = await h!.pool.query('SELECT metadata FROM jobs WHERE id = $1', [published.id]);
    expect(rows[0].metadata).toEqual({});
  });
});

describe('groups', () => {
  it('creates the group limit row on first publish', async () => {
    const q = h!.tq.defineQueue<{ n: number }>(h!.name('grp'), { requiresGroupId: true });
    const queueId = await q.id();

    const published = await q.publish({ n: 1 }, { group: { id: 'tenant-1', concurrency: 4 } });

    expect((await h!.job(published.id))!.groupId).toBe('tenant-1');
    expect(await readGroupCounters(h!.pool, queueId)).toEqual([
      { groupId: 'tenant-1', maxRunning: 4, running: 0 },
    ]);
  });

  it('does not duplicate the limit row on a second publish for the same group', async () => {
    const q = h!.tq.defineQueue<{ n: number }>(h!.name('grp-twice'), { requiresGroupId: true });
    const queueId = await q.id();

    await q.publish({ n: 1 }, { group: { id: 'tenant-1', concurrency: 4 } });
    // A different cap on the same group must not create a second row either —
    // the primary key is (queue_id, group_id).
    await q.publish({ n: 2 }, { group: { id: 'tenant-1', concurrency: 9 } });

    const counters = await readGroupCounters(h!.pool, queueId);
    expect(counters).toHaveLength(1);
    // First writer wins: ON CONFLICT DO NOTHING keeps the original cap.
    expect(counters[0]).toEqual({ groupId: 'tenant-1', maxRunning: 4, running: 0 });
    expect(await h!.jobCount(queueId)).toBe(2);
  });

  it('keeps the same group id separate per queue', async () => {
    const a = h!.tq.defineQueue<{ n: number }>(h!.name('grp-q-a'), { requiresGroupId: true });
    const b = h!.tq.defineQueue<{ n: number }>(h!.name('grp-q-b'), { requiresGroupId: true });
    const [aId, bId] = [await a.id(), await b.id()];

    await a.publish({ n: 1 }, { group: { id: 'shared', concurrency: 2 } });
    await b.publish({ n: 2 }, { group: { id: 'shared', concurrency: 7 } });

    expect(await readGroupCounters(h!.pool, aId)).toEqual([
      { groupId: 'shared', maxRunning: 2, running: 0 },
    ]);
    expect(await readGroupCounters(h!.pool, bId)).toEqual([
      { groupId: 'shared', maxRunning: 7, running: 0 },
    ]);
  });

  it('creates no limit row for a group concurrency of zero, which strands the job', async () => {
    const stranded = h!.tq.defineQueue<{ n: number }>(h!.name('grp-zero'), {
      requiresGroupId: true,
    });
    const strandedId = await stranded.id();

    const published = await stranded.publish(
      { n: 1 },
      { group: { id: 'tenant-0', concurrency: 0 } }
    );

    // The job carries the group id...
    expect((await h!.job(published.id))!.groupId).toBe('tenant-0');
    // ...but the limit insert is guarded by a truthiness test on concurrency,
    // so a zero cap skips it and no row exists to admit the job.
    expect(await readGroupCounters(h!.pool, strandedId)).toEqual([]);

    // Positive control on a SEPARATE queue, whose group does have a limit row.
    // Without it, "the handler was never called" is equally what a worker that
    // never started, never resolved its queue, or swallowed every pull error
    // would produce. The control cannot share the stranded job's queue: a job
    // queued behind an unpullable one is merely head-of-line blocked.
    const control = h!.tq.defineQueue<{ n: number }>(h!.name('grp-zero-control'), {
      requiresGroupId: true,
    });
    const controlId = await control.id();
    await control.publish({ n: 2 }, { group: { id: 'tenant-ok', concurrency: 1 } });

    const strandedSeen = collect<{ n: number }>();
    const controlSeen = collect<{ n: number }>();
    h!.track(await stranded.work(strandedSeen.handler, { pollInterval: '10ms' }));
    h!.track(await control.work(controlSeen.handler, { pollInterval: '10ms' }));

    // The control delivery proves the coordination pull path really does run and
    // deliver in this process; the stranded queue is polled by a worker
    // configured identically, so silence there is the queue's doing, not the
    // worker's.
    await controlSeen.waitForCalls(1);
    await h!.waitForDrain(controlId);
    // Short and deliberate: this is a negative. The control has just completed a
    // whole poll/pull/complete cycle, and 10ms polling gives the stranded queue
    // dozens more chances inside this window.
    await sleep(200);

    expect(strandedSeen.calls).toBe(0);
    const row = (await h!.job(published.id))!;
    expect(row.status).toBe(JobStatus.PENDING);
    // attempts and lease_seq are stamped by the pull itself, so untouched values
    // separate "never picked up" from "picked up and later reaped back".
    expect(row.attempts).toBe(0);
    expect(row.leaseSeq).toBeNull();
    expect(await h!.jobCount(strandedId)).toBe(1);
  });

  it('rejects a publish with no group on a queue that requires one', async () => {
    const q = h!.tq.defineQueue<{ n: number }>(h!.name('grp-required'), { requiresGroupId: true });
    const queueId = await q.id();

    await expect(q.publish({ n: 1 })).rejects.toThrow(BadParamInputError);
    await expect(q.publishMany([{ n: 1 }, { n: 2 }])).rejects.toThrow(BadParamInputError);

    expect(await h!.jobCount(queueId)).toBe(0);
    expect(await readGroupCounters(h!.pool, queueId)).toEqual([]);
  });
});

/**
 * Inputs the client accepts and the database then refuses. Every case here pins
 * CURRENT behaviour that is arguably a defect — the caller gets a raw driver
 * error rather than a `BadParamInputError` — so that changing any of it is a
 * deliberate act with a failing test attached.
 */
describe('inputs that reach the database as raw errors', () => {
  it('lets a NUL inside metadata surface as a raw jsonb error', async () => {
    const q = h!.tq.defineQueue<{ n: number }>(h!.name('meta-nul'));
    const queueId = await q.id();

    // KNOWN DEFECT. metadata is bound as $N::jsonb and Postgres rejects an
    // escaped NUL in jsonb, so the driver's SQLSTATE escapes to the caller.
    // The asymmetry is the real trap: jobs.payload is TEXT and stores that very
    // same escape happily (see "round-trips unicode and control characters"),
    // so one string is fatal in one argument and fine in the other. Correct
    // behaviour would be a single validation error from the client for both.
    const error = await rejection(q.publish({ n: 1 }, { metadata: { note: 'a\u0000b' } }));
    expect(error).not.toBeNull();
    expect(error.code).toBe('22P05');

    expect(await h!.jobCount(queueId)).toBe(0);
  });

  it('takes a 2600-byte idempotency key but rejects a 4000-byte one', async () => {
    const q = h!.tq.defineQueue<{ n: number }>(h!.name('key-length'));
    const queueId = await q.id();

    const accepted = await q.publish({ n: 1 }, { idempotencyKey: incompressibleKey(2600) });
    expect(accepted.deduplicated).toBe(false);

    // KNOWN DEFECT. The ceiling is the btree index-tuple limit (2704 bytes) on
    // jobs_idempotency_key_key, not anything the client checks, so an over-long
    // key fails at INSERT time with SQLSTATE 54000 instead of being rejected up
    // front against a documented maximum.
    const error = await rejection(q.publish({ n: 2 }, { idempotencyKey: incompressibleKey(4000) }));
    expect(error).not.toBeNull();
    expect(error.code).toBe('54000');

    // Only the accepted publish landed.
    expect(await h!.jobCount(queueId)).toBe(1);
  });

  interface GroupConcurrencyCase {
    /** Spelled out because vitest renders the number 0 as "+0" in a test name. */
    label: string;
    concurrency: number;
    /** SQLSTATE the publish rejects with, or null when it succeeds. */
    errorCode: string | null;
    jobs: number;
    counters: Array<{ groupId: string; maxRunning: number; running: number }>;
  }

  // KNOWN DEFECTS, all four. `group.concurrency` is passed straight through to
  // an INTEGER column with no validation: two of these quietly produce a job no
  // worker can ever pull, and two blow up with a driver error. Correct behaviour
  // would be a BadParamInputError for anything but a positive safe integer.
  const groupConcurrencyCases: GroupConcurrencyCase[] = [
    {
      // Falsy, so insertJobs skips the limit insert entirely and the job lands
      // in a group nothing can admit it to (see the strand test above).
      label: '0',
      concurrency: 0,
      errorCode: null,
      jobs: 1,
      counters: [],
    },
    {
      // Truthy, and max_running has no CHECK, so a negative cap is stored.
      // `running < max_running` can never hold: published, equally unpullable.
      label: '-1',
      concurrency: -1,
      errorCode: null,
      jobs: 1,
      counters: [{ groupId: 'boundary', maxRunning: -1, running: 0 }],
    },
    {
      // Bound as a parameter to an INTEGER column, so Postgres refuses the text
      // rather than rounding it.
      label: '2.5',
      concurrency: 2.5,
      errorCode: '22P02',
      jobs: 0,
      counters: [],
    },
    {
      // One past INTEGER: 22003, value out of range.
      label: '2 ** 31',
      concurrency: 2 ** 31,
      errorCode: '22003',
      jobs: 0,
      counters: [],
    },
  ];

  it.each(groupConcurrencyCases)(
    'pins publishing with group concurrency $label',
    async ({ concurrency, errorCode, jobs, counters }) => {
      const q = h!.tq.defineQueue<{ n: number }>(h!.name('grp-boundary'), {
        requiresGroupId: true,
      });
      const queueId = await q.id();

      const error = await rejection(q.publish({ n: 1 }, { group: { id: 'boundary', concurrency } }));

      if (errorCode === null) {
        expect(error).toBeNull();
      } else {
        expect(error).not.toBeNull();
        expect(error.code).toBe(errorCode);
      }

      // The throwing cases double as the atomicity proof: publishJobs opens a
      // transaction of its own only when a group is present, so a group whose
      // limit insert fails must take the job row down with it rather than leave
      // an orphan no worker can pull.
      expect(await h!.jobCount(queueId)).toBe(jobs);
      expect(await readGroupCounters(h!.pool, queueId)).toEqual(counters);
    }
  );
});

describe('publishMany', () => {
  it('preserves input order in the returned array and in id order', async () => {
    const q = h!.tq.defineQueue<{ n: number }>(h!.name('batch'));
    const queueId = await q.id();
    const payloads = Array.from({ length: 25 }, (_, n) => ({ n }));

    const published = await q.publishMany(payloads);

    expect(published).toHaveLength(25);
    expect(published.map((job) => decode(job.payload))).toEqual(payloads);
    expect(new Set(published.map((job) => job.id)).size).toBe(25);
    expect(new Set(published.map((job) => job.idempotencyKey)).size).toBe(25);
    expect(published.map((job) => job.deduplicated)).toEqual(payloads.map(() => false));
    expect(await h!.jobCount(queueId)).toBe(25);

    // Ground truth: the rows in id order carry the payloads in input order.
    const stored = await h!.jobs(queueId);
    expect(stored.map((row) => decode(row.payload))).toEqual(payloads);
    expect(stored.map((row) => [row.status, row.attempts])).toEqual(
      payloads.map(() => [JobStatus.PENDING, 0])
    );

    // created_at defaults to now(), which is the transaction timestamp, so all
    // 25 rows share one value. Both pull paths ORDER BY created_at, so *delivery*
    // order inside a batch comes down to whatever tie-break the plan happens to
    // use — input order survives in the returned array and in id order only.
    const { rows } = await h!.pool.query(
      'SELECT count(DISTINCT created_at)::int AS n FROM jobs WHERE queue_id = $1',
      [queueId]
    );
    expect(rows[0].n).toBe(1);
  });

  it('sends the whole batch as a single INSERT', async () => {
    const q = h!.tq.defineQueue<{ n: number }>(h!.name('batch-one-call'));
    const queueId = await q.id();
    // A recording executor rather than the pool: "one round trip" is a claim
    // about the statements sent, and only the executor can see those.
    const recorder = recordingExecutor(h!.pool);

    await q.publishMany(
      Array.from({ length: 25 }, (_, n) => ({ n })),
      { tx: recorder }
    );

    expect(insertsIntoJobs(recorder.statements)).toBe(1);
    expect(await h!.jobCount(queueId)).toBe(25);
  });

  it('is a no-op for an empty batch', async () => {
    const name = h!.name('batch-empty');
    const q = h!.tq.defineQueue<{ n: number }>(name);

    expect(await q.publishMany([])).toEqual([]);

    // It returns before resolving, so it does not even create the queue.
    const { rows } = await h!.pool.query('SELECT count(*)::int AS n FROM queues WHERE name = $1', [
      name,
    ]);
    expect(rows[0].n).toBe(0);
    expect(await h!.jobCount(await q.id())).toBe(0);
  });

  it('inserts one row when a key repeats inside a single batch', async () => {
    const q = h!.tq.defineQueue<{ n: number }>(h!.name('batch-dup'));
    const queueId = await q.id();
    const key = uniqueName('batch-key');

    // publishMany applies one options object to every payload, so an explicit
    // key makes the whole batch collide with itself.
    const published = await q.publishMany([{ n: 1 }, { n: 2 }, { n: 3 }], {
      idempotencyKey: key,
    });

    expect(published).toHaveLength(3);
    expect(published.map((job) => job.deduplicated)).toEqual([false, true, true]);
    expect(new Set(published.map((job) => job.id)).size).toBe(1);
    expect(await h!.jobCount(queueId)).toBe(1);
    // The first payload is the one that survives.
    expect(decode((await h!.jobs(queueId))[0].payload)).toEqual({ n: 1 });
  });

  it('deduplicates a batch against a row an earlier publish already inserted', async () => {
    const q = h!.tq.defineQueue<{ n: number }>(h!.name('batch-existing'));
    const queueId = await q.id();
    const key = uniqueName('pre-existing');

    const first = await q.publish({ n: 1 }, { idempotencyKey: key });
    const [again] = await q.publishMany([{ n: 2 }], { idempotencyKey: key });

    expect(again.deduplicated).toBe(true);
    expect(again.id).toBe(first.id);
    expect(await h!.jobCount(queueId)).toBe(1);
  });

  it('creates the group limit row for a batch', async () => {
    const q = h!.tq.defineQueue<{ n: number }>(h!.name('batch-grp'), { requiresGroupId: true });
    const queueId = await q.id();

    const published = await q.publishMany([{ n: 1 }, { n: 2 }, { n: 3 }], {
      group: { id: 'tenant-9', concurrency: 2 },
    });

    expect(published).toHaveLength(3);
    expect(await readGroupCounters(h!.pool, queueId)).toEqual([
      { groupId: 'tenant-9', maxRunning: 2, running: 0 },
    ]);
    // All three rows, not "every row that happens to exist" — an INSERT that
    // wrote fewer of them must not pass on the group id alone.
    expect((await h!.jobs(queueId)).map((row) => row.groupId)).toEqual([
      'tenant-9',
      'tenant-9',
      'tenant-9',
    ]);
  });

  it('creates one limit row per group when a single batch spans several groups', async () => {
    // `publishMany` takes a single PublishOptions, so the facade cannot express
    // a multi-group batch at all; drive the repository to cover that path.
    const q = h!.tq.defineQueue(h!.name('batch-multi-grp'), { requiresGroupId: true });
    const queueId = await q.id();
    const repo = new JobRepository(h!.pool, silentLogger);

    await repo.publishJobs([
      {
        idempotencyKey: uniqueName('mg'),
        payload: '{"n":1}',
        queueId,
        group: { id: 'alpha', concurrency: 2 },
      },
      {
        idempotencyKey: uniqueName('mg'),
        payload: '{"n":2}',
        queueId,
        group: { id: 'beta', concurrency: 5 },
      },
      {
        idempotencyKey: uniqueName('mg'),
        payload: '{"n":3}',
        queueId,
        group: { id: 'alpha', concurrency: 2 },
      },
    ]);

    expect(await readGroupCounters(h!.pool, queueId)).toEqual([
      { groupId: 'alpha', maxRunning: 2, running: 0 },
      { groupId: 'beta', maxRunning: 5, running: 0 },
    ]);
    expect(await h!.jobCount(queueId)).toBe(3);
  });

  it('rejects the whole batch when one payload cannot be serialized', async () => {
    const q = h!.tq.defineQueue<unknown>(h!.name('batch-bad'));
    const queueId = await q.id();

    await expect(q.publishMany([{ n: 1 }, undefined, { n: 3 }])).rejects.toThrow(
      BadParamInputError
    );

    // Serialization happens before the INSERT is built, so nothing lands.
    expect(await h!.jobCount(queueId)).toBe(0);
  });
});

describe('foreign executors', () => {
  it('joins a transaction owned by a client from a separate pool', async () => {
    const q = h!.tq.defineQueue<{ n: number }>(h!.name('foreign'));
    const queueId = await q.id();

    const foreignPool = createPool(pgConfig({ max: 2 }));
    let client: PoolClient | undefined;
    try {
      client = await foreignPool.connect();
      await client.query('BEGIN');

      const published = await q.publish({ n: 7 }, { tx: client });
      expect(published.deduplicated).toBe(false);
      expect(published.queueId).toBe(queueId);

      // The publish emitted no COMMIT of its own, so the row stays invisible to
      // every other session for as long as the caller's transaction is open.
      expect(await h!.jobCount(queueId)).toBe(0);

      await client.query('COMMIT');

      expect(await h!.jobCount(queueId)).toBe(1);
      const stored = await h!.job(published.id);
      expect(decode(stored!.payload)).toEqual({ n: 7 });
      // The id addresses the committed row, so it is the real BIGSERIAL value.
      // What this test cannot show is deserializeJob's defensive coercion:
      // createPool registers an INT8 parser on this package's `pg` copy
      // process-wide, so ids arrive as numbers here whether or not the coercion
      // is there. The next test is the one that covers it.
      expect(stored!.id).toBe(published.id);
    } finally {
      client?.release();
      await foreignPool.end();
    }
  });

  it('coerces BIGINT columns a foreign client hands back as strings', async () => {
    const q = h!.tq.defineQueue<{ n: number }>(h!.name('foreign-bigint'));
    const queueId = await q.id();
    const key = uniqueName('bigint-key');

    // Simulates a caller whose own copy of `pg` has no INT8 type parser, so
    // every BIGINT arrives as a string — node-postgres' default, and the case
    // deserializeJob's Number() calls exist for.
    const stringyBigints = {
      query: async (text: string, values?: any[]) => {
        const result = await h!.pool.query(text, values);
        return {
          ...result,
          rows: result.rows.map((row: any) => ({
            ...row,
            id: row.id == null ? row.id : String(row.id),
            queue_id: row.queue_id == null ? row.queue_id : String(row.queue_id),
            // Rewritten for completeness, but a freshly published job always has
            // lease_seq NULL — lockSeq's coercion is only ever exercised through
            // the library's own pool (consumer.test.ts asserts it after a pull).
            lease_seq: row.lease_seq == null ? row.lease_seq : String(row.lease_seq),
          })),
        };
      },
    };

    const published = await q.publish({ n: 7 }, { tx: stringyBigints, idempotencyKey: key });

    expect(typeof published.id).toBe('number');
    expect(Number.isInteger(published.id)).toBe(true);
    expect(typeof published.queueId).toBe('number');
    expect(published.queueId).toBe(queueId);

    // Numerically right, not merely numeric — `Number('12')` and a mangled parse
    // are both numbers.
    const { rows } = await h!.pool.query('SELECT id FROM jobs WHERE idempotency_key = $1', [key]);
    expect(rows).toHaveLength(1);
    expect(published.id).toBe(Number(rows[0].id));
    expect((await h!.job(published.id))!.idempotencyKey).toBe(key);
  });

  it('publishes through a bare object that only exposes query()', async () => {
    const q = h!.tq.defineQueue<{ n: number }>(h!.name('adapter'));
    const queueId = await q.id();

    // The Executor contract is structural — an ORM's transaction handle can be
    // wrapped in exactly this much.
    const adapter = recordingExecutor(h!.pool);

    const published = await q.publish({ n: 5 }, { tx: adapter });

    expect(published.deduplicated).toBe(false);
    expect(await h!.jobCount(queueId)).toBe(1);
    // The adapter was actually used: exactly one INSERT went through it. Without
    // this, a publish that ignored `tx` and went to the pool directly would
    // satisfy the "no BEGIN" assertion below on an empty array.
    expect(insertsIntoJobs(adapter.statements)).toBe(1);
    // It joins the caller's transaction rather than opening one of its own.
    expect(adapter.statements.some((sql) => /^(BEGIN|COMMIT|ROLLBACK)/i.test(sql))).toBe(false);
  });
});
