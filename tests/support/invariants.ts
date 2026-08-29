import { Pool } from 'pg';
import { expect } from 'vitest';

import { JobStatus } from '../../src/domain/job';

export interface ShardCounter {
  shardNo: number;
  maxRunning: number;
  running: number;
}

export interface GroupCounter {
  groupId: string;
  maxRunning: number;
  running: number;
}

export async function readShardCounters(pool: Pool, queueId: number): Promise<ShardCounter[]> {
  const { rows } = await pool.query(
    `SELECT shard_no, max_running, running FROM queue_shards WHERE queue_id = $1 ORDER BY shard_no`,
    [queueId]
  );
  return rows.map((r) => ({
    shardNo: Number(r.shard_no),
    maxRunning: Number(r.max_running),
    running: Number(r.running),
  }));
}

export async function readGroupCounters(pool: Pool, queueId: number): Promise<GroupCounter[]> {
  const { rows } = await pool.query(
    `SELECT group_id, max_running, running FROM group_queue_limits WHERE queue_id = $1 ORDER BY group_id`,
    [queueId]
  );
  return rows.map((r) => ({
    groupId: r.group_id,
    maxRunning: Number(r.max_running),
    running: Number(r.running),
  }));
}

export async function countByStatus(pool: Pool, queueId: number): Promise<Record<string, number>> {
  const { rows } = await pool.query(
    `SELECT status, count(*)::int AS n FROM jobs WHERE queue_id = $1 GROUP BY status`,
    [queueId]
  );
  const out: Record<string, number> = { PENDING: 0, PROCESSING: 0 };
  for (const r of rows) out[r.status] = r.n;
  return out;
}

export async function countProcessing(pool: Pool, queueId: number): Promise<number> {
  const { rows } = await pool.query(
    `SELECT count(*)::int AS n FROM jobs WHERE queue_id = $1 AND status = $2`,
    [queueId, JobStatus.PROCESSING]
  );
  return rows[0].n;
}

export async function countProcessingInGroup(
  pool: Pool,
  queueId: number,
  groupId: string
): Promise<number> {
  const { rows } = await pool.query(
    `SELECT count(*)::int AS n FROM jobs
     WHERE queue_id = $1 AND group_id = $2 AND status = $3`,
    [queueId, groupId, JobStatus.PROCESSING]
  );
  return rows[0].n;
}

export async function readJobRow(pool: Pool, id: number | string) {
  const { rows } = await pool.query(
    `SELECT id, status, group_id, queue_shard_no, attempts, lease_seq, lease_expires_at
     FROM jobs WHERE id = $1`,
    [id]
  );
  return rows[0] ?? null;
}

/** Counters must never dip below zero — a negative value means a double decrement. */
export async function expectNoNegativeCounters(pool: Pool, queueId: number): Promise<void> {
  for (const shard of await readShardCounters(pool, queueId)) {
    expect(
      shard.running,
      `queue_shards.running went negative on shard ${shard.shardNo}`
    ).toBeGreaterThanOrEqual(0);
  }
  for (const group of await readGroupCounters(pool, queueId)) {
    expect(
      group.running,
      `group_queue_limits.running went negative for group ${group.groupId}`
    ).toBeGreaterThanOrEqual(0);
  }
}

/**
 * Samples `SELECT count(*) ... WHERE status = 'PROCESSING'` on a fixed cadence
 * for the duration of a race. This is the ground-truth cap observation: it is
 * independent of any bookkeeping the client-side workers do for themselves.
 */
export class ProcessingSampler {
  peak = 0;
  samples = 0;
  private timer: NodeJS.Timeout | null = null;
  private inFlight = false;

  constructor(
    private pool: Pool,
    private queueId: number,
    private intervalMs = 10
  ) {}

  start(): void {
    if (this.timer) return;
    this.timer = setInterval(() => {
      if (this.inFlight) return; // never queue up behind a slow sample
      this.inFlight = true;
      countProcessing(this.pool, this.queueId)
        .then((n) => {
          this.samples += 1;
          if (n > this.peak) this.peak = n;
        })
        .catch(() => {
          /* pool saturated mid-race — drop the sample rather than fail the run */
        })
        .finally(() => {
          this.inFlight = false;
        });
    }, this.intervalMs);
    this.timer.unref?.();
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }
}

/**
 * Client-side high-water mark of concurrently-held jobs. Complements
 * ProcessingSampler: this one cannot miss a window, but it only sees the
 * jobs held by workers in *this* process.
 */
export class ConcurrencyTracker {
  current = 0;
  peak = 0;
  entered = 0;

  enter(): void {
    this.current += 1;
    this.entered += 1;
    if (this.current > this.peak) this.peak = this.current;
  }

  exit(): void {
    this.current -= 1;
  }
}

/** Per-key high-water marks, for group concurrency. */
export class KeyedConcurrencyTracker {
  private current = new Map<string, number>();
  readonly peaks = new Map<string, number>();

  enter(key: string): void {
    const next = (this.current.get(key) ?? 0) + 1;
    this.current.set(key, next);
    if (next > (this.peaks.get(key) ?? 0)) this.peaks.set(key, next);
  }

  exit(key: string): void {
    this.current.set(key, (this.current.get(key) ?? 0) - 1);
  }

  maxPeak(): number {
    return Math.max(0, ...this.peaks.values());
  }
}
