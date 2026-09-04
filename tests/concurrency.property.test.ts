import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { NUMBER_OF_SHARD } from '../src/domain/queue';
import {
  Harness,
  createHarness,
  publishJobs,
  queueInput,
  resetDatabase,
} from './support/harness';
import {
  ConcurrencyTracker,
  KeyedConcurrencyTracker,
  ProcessingSampler,
  countByStatus,
  expectNoNegativeCounters,
  readGroupCounters,
  readShardCounters,
} from './support/invariants';
import { duplicates, runRace } from './support/racers';

const RACERS = 90;

/** Pool for the racing workers. */
let h: Harness;
/** Separate pool for observation, so sampling can never be starved by the race. */
let obs: Harness;

beforeEach(async () => {
  h = createHarness({ maxConnections: RACERS + 10 });
  obs = createHarness({ maxConnections: 4 });
  await resetDatabase(obs.pool);
});

afterEach(async () => {
  await Promise.all([h.close(), obs.close()]);
});

describe('mutual exclusion', () => {
  it('never hands the same job to two workers (fast path)', async () => {
    // maxAttempts 1 makes a failure terminal, so every job is pulled exactly
    // once and any repeat in the log is a genuine double-delivery rather than a
    // legitimate retry.
    const queue = await h.queueService.createQueue(
      queueInput({ concurrency: 0, leaseDuration: 120_000, maxAttempts: 1 })
    );
    const total = 300;
    await publishJobs(h, queue, total);

    const result = await runRace(h, queue, {
      workers: 60,
      holdMs: 5,
      // Leases are long enough that nothing can legitimately be re-delivered,
      // so any repeat in the pull log is a genuine double-delivery.
      outcome: (_job, i) => (i % 7 === 0 ? 'fail' : 'complete'),
    });

    expect(result.pullErrors).toEqual([]);
    expect(duplicates(result.pulledIds)).toEqual([]);
    expect(result.pulledIds).toHaveLength(total);
    expect(result.settleErrors).toEqual([]);
    expect(result.completedIds.length + result.failedIds.length).toBe(total);

    expect(await countByStatus(obs.pool, queue.id)).toMatchObject({ PENDING: 0, PROCESSING: 0 });
  });

  it('never hands the same job to two workers (coordination path)', async () => {
    const queue = await h.queueService.createQueue(
      queueInput({ concurrency: NUMBER_OF_SHARD * 4, leaseDuration: 120_000 })
    );
    const total = 200;
    await publishJobs(h, queue, total);

    const result = await runRace(h, queue, { workers: 60, holdMs: 5 });

    expect(result.pullErrors).toEqual([]);
    expect(duplicates(result.pulledIds)).toEqual([]);
    expect(result.pulledIds).toHaveLength(total);
  });
});

describe('queue concurrency cap', () => {
  it('never exceeds the configured cap with more workers than slots', async () => {
    const concurrency = NUMBER_OF_SHARD * 2; // 64 — evenly divisible, the best case
    const queue = await h.queueService.createQueue(
      queueInput({ concurrency, leaseDuration: 120_000 })
    );
    await publishJobs(h, queue, 500);

    const tracker = new ConcurrencyTracker();
    const sampler = new ProcessingSampler(obs.pool, queue.id, 5);
    sampler.start();

    const result = await runRace(h, queue, {
      workers: RACERS, // deliberately more racers than slots
      holdMs: 40,
      // Stop once we have enough evidence; draining 500 jobs is not the point.
      stopWhen: () => tracker.entered >= 300,
      tracker,
      deadlineMs: 45_000,
    });

    sampler.stop();

    expect(result.pullErrors).toEqual([]);
    expect(sampler.samples, 'sampler produced no observations').toBeGreaterThan(10);

    // Two independent witnesses of the same invariant.
    expect(
      tracker.peak,
      `client-observed peak in-flight jobs exceeded the cap of ${concurrency}`
    ).toBeLessThanOrEqual(concurrency);

    // ...and the cap has to actually bind. Without a lower bound this test would
    // still pass if a regression let the queue admit one job at a time.
    expect(
      tracker.peak,
      `the queue never came close to its cap of ${concurrency} — the cap assertion above proves nothing`
    ).toBeGreaterThan(concurrency / 2);
    expect(
      sampler.peak,
      `database-observed PROCESSING rows exceeded the cap of ${concurrency}`
    ).toBeLessThanOrEqual(concurrency);
  });

  it('returns every shard counter to zero once the queue drains', async () => {
    const queue = await h.queueService.createQueue(
      queueInput({ concurrency: NUMBER_OF_SHARD * 2, leaseDuration: 120_000 })
    );
    await publishJobs(h, queue, 150);

    await runRace(h, queue, { workers: 40, holdMs: 10 });

    expect(await countByStatus(obs.pool, queue.id)).toMatchObject({ PENDING: 0, PROCESSING: 0 });

    const shards = await readShardCounters(obs.pool, queue.id);
    const leaked = shards.filter((s) => s.running !== 0);
    expect(leaked, 'shard counters leaked after the queue drained').toEqual([]);
    await expectNoNegativeCounters(obs.pool, queue.id);
  });

  it('provisions exactly the configured concurrency across shards', async () => {
    // 100 is not a multiple of NUMBER_OF_SHARD (32); the split must not silently
    // discard the remainder.
    const concurrency = 100;
    const queue = await h.queueService.createQueue(queueInput({ concurrency }));

    const shards = await readShardCounters(obs.pool, queue.id);
    const admissible = shards.reduce((sum, s) => sum + s.maxRunning, 0);
    expect(
      admissible,
      `queue advertises concurrency ${concurrency} but shards admit ${admissible}`
    ).toBe(concurrency);
  });

  it('still admits work when concurrency is smaller than the shard count', async () => {
    const concurrency = 8; // < NUMBER_OF_SHARD
    const queue = await h.queueService.createQueue(
      queueInput({ concurrency, leaseDuration: 120_000 })
    );
    await publishJobs(h, queue, 20);

    const tracker = new ConcurrencyTracker();
    const result = await runRace(h, queue, {
      workers: 16,
      holdMs: 10,
      maxIdlePolls: 20,
      deadlineMs: 15_000,
      stopWhen: () => tracker.entered >= 20,
      tracker,
    });

    expect(
      result.pulledIds.length,
      `a queue with concurrency ${concurrency} admitted no jobs at all`
    ).toBeGreaterThan(0);
    expect(tracker.peak, 'cap exceeded').toBeLessThanOrEqual(concurrency);
  });
});

describe('group concurrency cap', () => {
  const GROUPS = ['tenant-a', 'tenant-b', 'tenant-c', 'tenant-d'];
  const GROUP_CAP = 2;

  it('never exceeds a group cap with many workers racing', async () => {
    const queue = await h.queueService.createQueue(
      queueInput({ concurrency: 0, requiresGroupId: true, leaseDuration: 120_000 })
    );
    const total = 80;
    await publishJobs(h, queue, total, { groupIds: GROUPS, groupConcurrency: GROUP_CAP });

    const groupTracker = new KeyedConcurrencyTracker();
    const result = await runRace(h, queue, {
      workers: 40,
      holdMs: 15,
      // The coordination pull only ever considers the single oldest PENDING job,
      // so a saturated head-of-line group stalls every worker. Be patient.
      maxIdlePolls: 200,
      pollIntervalMs: 5,
      deadlineMs: 60_000,
      groupTracker,
    });

    expect(result.pullErrors).toEqual([]);
    expect(duplicates(result.pulledIds)).toEqual([]);

    for (const [groupId, peak] of groupTracker.peaks) {
      expect(peak, `group ${groupId} exceeded its cap of ${GROUP_CAP}`).toBeLessThanOrEqual(
        GROUP_CAP
      );
    }

    expect(result.pulledIds).toHaveLength(total);
    expect(await countByStatus(obs.pool, queue.id)).toMatchObject({ PENDING: 0, PROCESSING: 0 });
  });

  it('returns every group counter to zero once the queue drains', async () => {
    const queue = await h.queueService.createQueue(
      queueInput({ concurrency: 0, requiresGroupId: true, leaseDuration: 120_000 })
    );
    await publishJobs(h, queue, 40, { groupIds: GROUPS, groupConcurrency: GROUP_CAP });

    await runRace(h, queue, {
      workers: 20,
      holdMs: 10,
      maxIdlePolls: 200,
      deadlineMs: 60_000,
    });

    const groups = await readGroupCounters(obs.pool, queue.id);
    expect(groups.map((g) => g.running)).toEqual(GROUPS.map(() => 0));
    await expectNoNegativeCounters(obs.pool, queue.id);
  });

  // KNOWN OPEN ITEM — deliberately left red. Group-fair scheduling is a separate
  // piece of work; this test is the marker for it, not a broken test.
  it('does not let a saturated group block unrelated groups', async () => {
    // 'busy' owns the head of the queue and can only run one job at a time;
    // 'idle' has plenty of headroom. A fair scheduler must still serve 'idle'.
    const queue = await h.queueService.createQueue(
      queueInput({ concurrency: 0, requiresGroupId: true, leaseDuration: 120_000 })
    );

    for (let i = 0; i < 5; i++) {
      await h.jobRepo.publishJob({
        idempotencyKey: `hol-busy-${process.pid}-${i}`,
        payload: '{}',
        queueId: queue.id,
        group: { id: 'busy', concurrency: 1 },
      });
    }
    await h.jobRepo.publishJob({
      idempotencyKey: `hol-idle-${process.pid}`,
      payload: '{}',
      queueId: queue.id,
      group: { id: 'idle', concurrency: 5 },
    });

    // Hold one 'busy' job so that group sits at its cap for the whole test.
    const held = await h.jobRepo.pullJob(queue);
    expect(held!.groupId).toBe('busy');

    const next = await h.jobRepo.pullJob(queue);
    expect(
      next?.groupId,
      'a saturated group at the head of the queue starves every other group'
    ).toBe('idle');
  });
});
