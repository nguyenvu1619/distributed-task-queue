import { setTimeout as delay } from 'node:timers/promises';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { JobStatus } from '../src/domain/job';
import { JobSnooze, NonRetryable } from '../src/domain/errors';
import { WorkerService } from '../src/services/worker.service';
import { Harness, createHarness, jobInput, queueInput, resetDatabase, sleep, waitFor } from './support/harness';
import { readJobRow } from './support/invariants';

let h: Harness;

beforeEach(async () => {
  h = createHarness({ maxConnections: 10 });
  await resetDatabase(h.pool);
});

afterEach(async () => {
  vi.restoreAllMocks();
  await h.close();
});

/** Resolves once `signal` is aborted. */
function aborted(signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve();
  return new Promise((resolve) => signal.addEventListener('abort', () => resolve(), { once: true }));
}

describe('stop signal', () => {
  it('aborts the signal on stop(), so a handler that watches it hands its job back early', async () => {
    const queue = await h.queueService.createQueue(queueInput({ maxAttempts: 3 }));
    const published = await h.jobRepo.publishJob(jobInput(queue.id));

    let abortedAtStart: boolean | undefined;
    let batches = 0;
    const worker = new WorkerService(h.jobService, {
      queueId: queue.id,
      pollInterval: 20,
      handler: async (_job, _payload, signal) => {
        abortedAtStart = signal.aborted;
        for (let i = 0; i < 1_000; i++) {
          if (signal.aborted) throw new JobSnooze(0);
          await sleep(10);
          batches += 1;
        }
      },
    });

    await worker.start();
    await waitFor(() => abortedAtStart !== undefined, { message: 'the handler to start' });
    await worker.stop();

    expect(abortedAtStart, 'the signal must not start out aborted').toBe(false);
    expect(batches, 'the handler must stop at its next check, not run to its end').toBeLessThan(1_000);
    const row = await readJobRow(h.pool, published.id);
    expect(row?.status).toBe(JobStatus.PENDING);
    expect(row.attempts, 'handing the job back must refund its attempt').toBe(0);
    expect(row.deferred, 'JobSnooze(0) makes it due at once').toBe(false);
  });

  it('hands the job back without spending an attempt when an aborted call escapes the handler', async () => {
    // One attempt: had the escaped error counted as a failure, the job would be deleted.
    const queue = await h.queueService.createQueue(queueInput({ maxAttempts: 1 }));
    const published = await h.jobRepo.publishJob(jobInput(queue.id));
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});

    let started = false;
    const worker = new WorkerService(h.jobService, {
      queueId: queue.id,
      pollInterval: 20,
      handler: async (_job, _payload, signal) => {
        started = true;
        await delay(30_000, undefined, { signal }); // rejects with an AbortError on stop()
      },
    });

    await worker.start();
    await waitFor(() => started, { message: 'the handler to start' });
    await worker.stop();

    const row = await readJobRow(h.pool, published.id);
    expect(row, 'a job interrupted by stop() must not be failed on its last attempt').not.toBeNull();
    expect(row.status).toBe(JobStatus.PENDING);
    expect(row.attempts).toBe(0);
    expect(row.deferred).toBe(false);
    expect(warn).toHaveBeenCalledWith(
      expect.stringContaining(`returning job ${published.id} to the queue, interrupted by stop()`)
    );

    // Its one attempt is still there to spend.
    const again = await h.jobRepo.pullJob(queue);
    expect(again?.id).toBe(published.id);
    expect(again?.attempts).toBe(1);
  });

  it('still counts an error thrown before stop() as a failure', async () => {
    const queue = await h.queueService.createQueue(queueInput({ maxAttempts: 1 }));
    const published = await h.jobRepo.publishJob(jobInput(queue.id));

    const worker = new WorkerService(h.jobService, {
      queueId: queue.id,
      pollInterval: 20,
      handler: async () => {
        throw new Error('boom');
      },
    });

    await worker.start();
    try {
      await waitFor(async () => (await readJobRow(h.pool, published.id)) === null, {
        message: 'the job to fail on its only attempt',
      });
    } finally {
      await worker.stop();
    }
  });

  it('still fails the job for good when the handler throws NonRetryable after stop()', async () => {
    const queue = await h.queueService.createQueue(queueInput({ maxAttempts: 3 }));
    const published = await h.jobRepo.publishJob(jobInput(queue.id));

    let started = false;
    const worker = new WorkerService(h.jobService, {
      queueId: queue.id,
      pollInterval: 20,
      handler: async (_job, _payload, signal) => {
        started = true;
        await aborted(signal);
        throw new NonRetryable('cannot succeed');
      },
    });

    await worker.start();
    await waitFor(() => started, { message: 'the handler to start' });
    await worker.stop();

    expect(await readJobRow(h.pool, published.id), 'NonRetryable must still discard the job').toBeNull();
  });

  it('completes the job when the handler returns after stop()', async () => {
    const queue = await h.queueService.createQueue(queueInput({ maxAttempts: 3 }));
    const published = await h.jobRepo.publishJob(jobInput(queue.id));
    const complete = vi.spyOn(h.jobService, 'completeJobDirect');

    let started = false;
    const worker = new WorkerService(h.jobService, {
      queueId: queue.id,
      pollInterval: 20,
      handler: async (_job, _payload, signal) => {
        started = true;
        await aborted(signal);
      },
    });

    await worker.start();
    await waitFor(() => started, { message: 'the handler to start' });
    await worker.stop();

    // Returning means done, even mid-work: stopping early has to throw JobSnooze.
    expect(complete).toHaveBeenCalledTimes(1);
    expect(await readJobRow(h.pool, published.id)).toBeNull();
  });

  it('gives a restarted worker a signal that is not aborted', async () => {
    const queue = await h.queueService.createQueue(queueInput());
    const seen: boolean[] = [];
    const worker = new WorkerService(h.jobService, {
      queueId: queue.id,
      pollInterval: 20,
      handler: async (_job, _payload, signal) => {
        seen.push(signal.aborted);
      },
    });

    await worker.start();
    await worker.stop();

    const published = await h.jobRepo.publishJob(jobInput(queue.id));
    await worker.start();
    try {
      await waitFor(async () => (await readJobRow(h.pool, published.id)) === null, {
        message: 'the job to complete',
      });
    } finally {
      await worker.stop();
    }

    expect(seen, 'an aborted signal cannot be reset, so start() must make a new one').toEqual([false]);
  });
});
