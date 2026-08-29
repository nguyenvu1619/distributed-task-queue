import { Job } from '../../src/domain/job';
import { Queue } from '../../src/domain/queue';
import { Harness, sleep } from './harness';
import { ConcurrencyTracker, KeyedConcurrencyTracker } from './invariants';

export interface RaceOptions {
  /** Number of independent pull loops racing each other. */
  workers: number;
  /** How long a worker pretends to work before completing/failing. */
  holdMs?: number;
  /** Backoff when a pull returns null. */
  pollIntervalMs?: number;
  /** Give up a worker after this many consecutive empty pulls. */
  maxIdlePolls?: number;
  /** Hard wall-clock stop so a broken cap can never hang the suite. */
  deadlineMs?: number;
  /** Stop every worker as soon as this returns true (checked per iteration). */
  stopWhen?: () => boolean;
  /** Decide per job whether to complete or fail it. Default: always complete. */
  outcome?: (job: Job, index: number) => 'complete' | 'fail';
  tracker?: ConcurrencyTracker;
  groupTracker?: KeyedConcurrencyTracker;
}

export interface RaceResult {
  /** Job ids in pull order, including any pulled more than once. */
  pulledIds: string[];
  completedIds: string[];
  failedIds: string[];
  /** Errors raised by complete/fail — e.g. a lease that was reaped mid-hold. */
  settleErrors: Error[];
  /** Errors raised by pull itself. */
  pullErrors: Error[];
  durationMs: number;
  hitDeadline: boolean;
}

/**
 * Drives N concurrent pull → hold → settle loops against a single queue.
 *
 * Deliberately built on JobRepository directly rather than WorkerService: the
 * property under test is the *repository's* mutual exclusion, and WorkerService
 * adds a poll-sleep that would blur the contention window.
 */
export async function runRace(
  harness: Harness,
  queue: Queue,
  options: RaceOptions
): Promise<RaceResult> {
  const {
    workers,
    holdMs = 25,
    pollIntervalMs = 5,
    maxIdlePolls = 40,
    deadlineMs = 60_000,
    stopWhen,
    outcome = () => 'complete' as const,
    tracker,
    groupTracker,
  } = options;

  const result: RaceResult = {
    pulledIds: [],
    completedIds: [],
    failedIds: [],
    settleErrors: [],
    pullErrors: [],
    durationMs: 0,
    hitDeadline: false,
  };

  const startedAt = Date.now();
  const deadline = startedAt + deadlineMs;
  let index = 0;

  const loop = async (): Promise<void> => {
    let idle = 0;

    while (Date.now() < deadline && idle < maxIdlePolls && !stopWhen?.()) {
      let job: Job | null = null;
      try {
        job = await harness.jobRepo.pullJob(queue);
      } catch (err) {
        result.pullErrors.push(err as Error);
        await sleep(pollIntervalMs);
        continue;
      }

      if (!job) {
        idle += 1;
        await sleep(pollIntervalMs);
        continue;
      }

      idle = 0;
      const id = String(job.id);
      const groupId = job.groupId;
      result.pulledIds.push(id);
      tracker?.enter();
      if (groupTracker && groupId) groupTracker.enter(groupId);

      const decision = outcome(job, index++);

      try {
        if (holdMs > 0) await sleep(holdMs);
        if (decision === 'complete') {
          await harness.jobRepo.completeJob(job.id, job.lockSeq!, queue);
          result.completedIds.push(id);
        } else {
          await harness.jobRepo.failJob(job.id, job.lockSeq!, queue);
          result.failedIds.push(id);
        }
      } catch (err) {
        result.settleErrors.push(err as Error);
      } finally {
        tracker?.exit();
        if (groupTracker && groupId) groupTracker.exit(groupId);
      }
    }
  };

  await Promise.all(Array.from({ length: workers }, () => loop()));

  result.durationMs = Date.now() - startedAt;
  result.hitDeadline = Date.now() >= deadline;
  return result;
}

/** Ids that appear more than once in a pull log. */
export function duplicates(ids: string[]): string[] {
  const seen = new Set<string>();
  const dupes = new Set<string>();
  for (const id of ids) {
    if (seen.has(id)) dupes.add(id);
    seen.add(id);
  }
  return [...dupes];
}
