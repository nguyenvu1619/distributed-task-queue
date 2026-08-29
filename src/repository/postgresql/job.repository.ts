import { Pool, PoolClient } from 'pg';
import { Job, JobStatus, CreateJobInput, Metadata, PublishedJob } from '../../domain/job';
import { Queue } from '../../domain/queue';
import { ConflictError, NotFoundError } from '../../domain/errors';
import { Executor } from '../../domain/executor';
import { Logger, consoleLogger } from '../../domain/logger';

// Every read of an active job returns the same projection.
const JOB_COLUMNS = `id, idempotency_key, payload, status, group_id, queue_id, queue_shard_no,
         attempts, metadata, created_at, updated_at, lease_seq, lease_expires_at`;

// Database row interface for the active jobs table (snake_case)
// Note: completed_at is not stored — completed/failed jobs are deleted
interface JobRow {
  id: number;
  idempotency_key: string;
  payload: string;
  status: string;
  group_id: string | null;
  queue_id: number;
  attempts: number;
  metadata: any;
  queue_shard_no: number | null;
  created_at: Date;
  updated_at: Date;
  lease_seq: number | null;
  lease_expires_at: Date | null;
}


export class JobRepository {
  constructor(
    private pool: Pool,
    private logger: Logger = consoleLogger
  ) {}

  /**
   * The fast path is a single statement with no coordination counters to keep.
   * Same predicate governs pull, complete, fail and discard, so it lives here
   * rather than being restated at each call site.
   */
  private isFastPath(queue: Queue): boolean {
    return (queue.concurrency === 0 || queue.concurrency === null) && !queue.requiresGroupId;
  }

  /**
   * Runs `fn` inside a transaction — unless the caller supplied one, in which
   * case it joins theirs and emits no BEGIN/COMMIT/ROLLBACK of its own. This is
   * what lets a publish ride along in the user's business transaction: the job
   * row and their own writes commit together, or neither does, with no outbox
   * table in between.
   */
  private async withExecutor<T>(
    executor: Executor | undefined,
    fn: (db: Executor) => Promise<T>
  ): Promise<T> {
    if (executor) {
      return fn(executor);
    }

    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const result = await fn(client);
      await client.query('COMMIT');
      return result;
    } catch (error) {
      // A failing ROLLBACK (dead connection) must not mask why we got here.
      await client.query('ROLLBACK').catch((rollbackError) => {
        this.logger.error('Rollback failed after a publish error', rollbackError);
      });
      throw error;
    } finally {
      client.release();
    }
  }

  async getById(id: number): Promise<Job> {
    const result = await this.pool.query(
      `SELECT id, idempotency_key, payload, status, group_id, queue_id, queue_shard_no, attempts,
       metadata, created_at, updated_at, lease_seq, lease_expires_at
       FROM jobs WHERE id = $1`,
      [id]
    );

    if (result.rows.length === 0) {
      throw new NotFoundError(`Job with id ${id} not found`);
    }

    return this.deserializeJob(result.rows[0] as JobRow);
  }

  /**
   * Publishes one job. Pass `executor` — a pg PoolClient, or anything exposing
   * `query(text, values)` — to enqueue inside a transaction the caller owns.
   */
  async publishJob(input: CreateJobInput, executor?: Executor): Promise<PublishedJob> {
    const [job] = await this.publishJobs([input], executor);
    return job;
  }

  /**
   * Publishes a batch in a single round trip. One INSERT with N rows, rather
   * than N connections each doing BEGIN/INSERT/COMMIT.
   */
  async publishJobs(inputs: CreateJobInput[], executor?: Executor): Promise<PublishedJob[]> {
    if (inputs.length === 0) {
      return [];
    }
    // Without a group there is only one statement, so a transaction buys
    // nothing but two extra round trips.
    const needsTransaction = inputs.some((input) => input.group?.id && input.group?.concurrency);
    if (!executor && !needsTransaction) {
      return this.insertJobs(this.pool, inputs);
    }
    return this.withExecutor(executor, (db) => this.insertJobs(db, inputs));
  }

  private async insertJobs(db: Executor, inputs: CreateJobInput[]): Promise<PublishedJob[]> {
    const placeholders: string[] = [];
    const values: any[] = [];

    inputs.forEach((input, index) => {
      const offset = index * 7;
      placeholders.push(
        `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, ` +
          `$${offset + 5}, $${offset + 6}, $${offset + 7}::jsonb)`
      );
      values.push(
        input.idempotencyKey,
        input.payload,
        JobStatus.PENDING,
        input.group?.id || null,
        input.queueId,
        input.attempts || 0,
        JSON.stringify(input.metadata || {})
      );
    });

    // ON CONFLICT DO NOTHING rather than letting the UNIQUE violation fly: a
    // raw 23505 aborts the *whole* enclosing transaction, so a duplicate
    // publish would take the caller's business writes down with it.
    const inserted = await db.query(
      `INSERT INTO jobs (idempotency_key, payload, status, group_id, queue_id, attempts, metadata)
       VALUES ${placeholders.join(', ')}
       ON CONFLICT (idempotency_key) DO NOTHING
       RETURNING ${JOB_COLUMNS}`,
      values
    );

    const groups = new Map<string, { groupId: string; queueId: number; concurrency: number }>();
    for (const input of inputs) {
      if (input.group?.id && input.group?.concurrency) {
        groups.set(`${input.queueId}:${input.group.id}`, {
          groupId: input.group.id,
          queueId: input.queueId,
          concurrency: input.group.concurrency,
        });
      }
    }

    for (const group of groups.values()) {
      await db.query(
        `INSERT INTO group_queue_limits (group_id, queue_id, max_running, running, updated_at, created_at)
         VALUES ($1, $2, $3, $4, now(), now()) ON CONFLICT DO NOTHING`,
        [group.groupId, group.queueId, group.concurrency, 0]
      );
    }

    const rowsByKey = new Map<string, JobRow>();
    for (const row of inserted.rows as JobRow[]) {
      rowsByKey.set(row.idempotency_key, row);
    }

    // Anything the insert skipped already exists — read it back so the caller
    // gets the live job rather than an error.
    const conflicted = inputs
      .map((input) => input.idempotencyKey)
      .filter((key) => !rowsByKey.has(key));

    if (conflicted.length > 0) {
      const existing = await db.query(
        `SELECT ${JOB_COLUMNS} FROM jobs WHERE idempotency_key = ANY($1::text[])`,
        [conflicted]
      );
      for (const row of existing.rows as JobRow[]) {
        rowsByKey.set(row.idempotency_key, row);
      }
    }

    const freshKeys = new Set((inserted.rows as JobRow[]).map((row) => row.idempotency_key));
    const seen = new Set<string>();

    return inputs.map((input) => {
      const row = rowsByKey.get(input.idempotencyKey);
      if (!row) {
        // The insert skipped it and the follow-up read could not see it either:
        // a concurrent publisher under REPEATABLE READ, or the conflicting job
        // reached a terminal state and was deleted in between. Typed error, and
        // the caller's transaction is still usable.
        throw new ConflictError(
          `Job with idempotency key ${input.idempotencyKey} conflicts with a concurrent publish`
        );
      }
      // Second and later occurrences of a key within one batch are duplicates
      // of the row this same call just inserted.
      const isFresh = freshKeys.has(input.idempotencyKey) && !seen.has(input.idempotencyKey);
      seen.add(input.idempotencyKey);
      return { ...this.deserializeJob(row), deduplicated: !isFresh };
    });
  }

  /** Live counts for a queue. Terminal jobs are deleted, so this is the backlog. */
  async countByStatus(queueId: number): Promise<{ pending: number; processing: number }> {
    const result = await this.pool.query(
      `SELECT status, count(*)::int AS count FROM jobs WHERE queue_id = $1 GROUP BY status`,
      [queueId]
    );

    const counts = { pending: 0, processing: 0 };
    for (const row of result.rows as Array<{ status: string; count: number }>) {
      if (row.status === JobStatus.PENDING) {
        counts.pending = Number(row.count);
      } else if (row.status === JobStatus.PROCESSING) {
        counts.processing = Number(row.count);
      }
    }
    return counts;
  }

  async pullJobs(status: JobStatus, limit: number): Promise<Job[]> {
    const result = await this.pool.query(
      `SELECT id, idempotency_key, payload, status, group_id, queue_id, queue_shard_no, attempts,
       metadata, created_at, updated_at, lease_seq, lease_expires_at
       FROM jobs WHERE status = $1 ORDER BY created_at LIMIT $2`,
      [status, limit]
    );

    return result.rows.map((row) => this.deserializeJob(row as JobRow));
  }

  /**
   * Fast path for pulling jobs - single query, no transactions, no coordination
   * Use when queue.concurrency === 0 AND queue.requiresGroupId === false
   */
  private async pullJobFast(queue: Queue): Promise<Job | null> {
    // Single UPDATE with RETURNING - combines SELECT + UPDATE atomically
    const result = await this.pool.query(
      `UPDATE jobs
       SET status = 'PROCESSING',
           lease_expires_at = now() + ($1 || ' milliseconds')::interval,
           lease_seq = COALESCE(lease_seq, 0) + 1,
           attempts = attempts + 1,
           updated_at = now()
       WHERE id = (
         SELECT id FROM jobs
         WHERE status = 'PENDING' AND queue_id = $2
         ORDER BY created_at
         FOR UPDATE SKIP LOCKED
         LIMIT 1
       )
       RETURNING id, idempotency_key, payload, status, group_id, queue_id,
                 queue_shard_no, attempts, metadata, created_at, updated_at,
                 lease_seq, lease_expires_at`,
      [queue.leaseDuration, queue.id]
    );

    return result.rows.length > 0
      ? this.deserializeJob(result.rows[0] as JobRow)
      : null;
  }

  /**
   * Full path for pulling jobs - multiple queries with transactions and coordination
   * Use when queue.concurrency > 0 OR queue.requiresGroupId === true
   */
  private async pullJobWithCoordination(queue: Queue): Promise<Job | null> {
    const client = await this.pool.connect();

    try {
      await client.query('BEGIN');
      // update queue shard
      let shareNo = null;
      if (queue.concurrency) {
        const queueShardResult = await client.query(
          `SELECT shard_no, max_running, running FROM queue_shards WHERE queue_id = $1 AND running < max_running FOR UPDATE SKIP LOCKED LIMIT 1`,
          [queue.id]
        );
        if (queueShardResult.rows.length === 0) {
          await client.query('ROLLBACK');
          this.logger.warn(`No available queue shard for queue ${queue.id}`);
          return null;
        }
        shareNo = queueShardResult.rows[0].shard_no;
      }

      // Select and lock a pending job
      const selectResult = await client.query(
        `SELECT id, idempotency_key, payload, status, group_id, queue_id, queue_shard_no, attempts,
         metadata, created_at, updated_at, lease_seq, lease_expires_at
         FROM jobs WHERE status = 'PENDING' AND queue_id = $1
         ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED`,
        [queue.id]
      );

      if (selectResult.rows.length === 0) {
        await client.query('ROLLBACK');
        return null;
      }

      const job = this.deserializeJob(selectResult.rows[0] as JobRow);
      const isExpired = job.status === JobStatus.PROCESSING; // if processing, it means it's expired
      const newLockSeq = (job.lockSeq || 0) + 1;

      // Update group queue limits
      if (job.groupId && !isExpired) {
        const groupQueueLimitResult = await client.query(
          `UPDATE group_queue_limits SET running = running + 1 WHERE group_id = $1 AND queue_id = $2 AND running < max_running RETURNING group_id`,
          [job.groupId, queue.id]
        );
        if (groupQueueLimitResult.rows.length === 0) {
          await client.query('ROLLBACK');
          this.logger.warn(
            `Group queue limit reached for group ${job.groupId} and queue ${queue.id}`
          );
          return null;
        }
      }
      // `shareNo !== null` rather than a truthiness test: shard 0 is a real
      // shard, and treating it as falsy left it permanently uncounted — an
      // unbounded hole in the concurrency cap.
      if (!isExpired && shareNo !== null) {
        // increase running count
        await client.query(
          `UPDATE queue_shards SET running = running + 1 WHERE queue_id = $1 AND shard_no = $2`,
          [queue.id, shareNo]
        );
      }

      // Lease in milliseconds, matching the fast path. Truncating to whole
      // seconds made any sub-second lease expire at the instant it was issued.
      // RETURNING the updated row so the caller gets the lease that was actually
      // written, not the pre-UPDATE snapshot.
      const leased = await client.query(
        `UPDATE jobs
         SET lease_expires_at = now() + ($1 || ' milliseconds')::interval,
             queue_shard_no = $2,
             lease_seq = $3,
             status = 'PROCESSING',
             attempts = attempts + 1,
             updated_at = now()
         WHERE id = $4
         RETURNING id, idempotency_key, payload, status, group_id, queue_id, queue_shard_no,
                   attempts, metadata, created_at, updated_at, lease_seq, lease_expires_at`,
        [queue.leaseDuration, shareNo, newLockSeq, job.id]
      );

      await client.query('COMMIT');

      return this.deserializeJob(leased.rows[0] as JobRow);
    } catch (error) {
      await client.query('ROLLBACK');
      this.logger.error('Failed to pull a job with coordination', error);
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * Public API: pulls a job from the queue
   * Automatically selects fast or full path based on queue configuration
   * Fast path requires BOTH: concurrency === 0 AND requiresGroupId === false
   */
  async pullJob(queue: Queue): Promise<Job | null> {
    if (this.isFastPath(queue)) {
      return this.pullJobFast(queue);
    }
    return this.pullJobWithCoordination(queue);
  }

  // ---------------------------------------------------------------------------
  // Complete job
  // ---------------------------------------------------------------------------

  /**
   * Fast path for completing jobs.
   * Single DELETE — no transaction needed.
   * Use when queue.concurrency === 0 AND queue.requiresGroupId === false
   */
  private async completeJobFast(id: number, lockSeq: number): Promise<Job> {
    const result = await this.pool.query(
      `DELETE FROM jobs
       WHERE id = $1 AND lease_seq = $2 AND status = 'PROCESSING'
       RETURNING id, idempotency_key, payload, status, group_id, queue_id, queue_shard_no,
                 attempts, metadata, created_at, updated_at, lease_seq, lease_expires_at`,
      [id, lockSeq]
    );

    if (result.rows.length === 0) {
      throw new NotFoundError(`Job with id ${id} and lock_seq ${lockSeq} not found or not in PROCESSING status`);
    }

    const completedAt = new Date();
    return {
      ...this.deserializeJob(result.rows[0] as JobRow),
      status: JobStatus.COMPLETED,
      completedAt,
      lockSeq: null,
      leaseExpiresAt: null,
    };
  }

  /**
   * Full path for completing jobs — includes shard / group-limit coordination.
   * Use when queue.concurrency > 0 OR queue.requiresGroupId === true
   */
  private async completeJobWithCoordination(id: number, lockSeq: number, queue: Queue): Promise<Job> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');

      // Lock and fetch the job row before deleting
      const jobResult = await client.query(
        `SELECT id, idempotency_key, payload, status, group_id, queue_id, queue_shard_no, attempts,
         metadata, created_at, updated_at, lease_seq, lease_expires_at
         FROM jobs WHERE id = $1 AND lease_seq = $2 AND status = 'PROCESSING' FOR UPDATE`,
        [id, lockSeq]
      );

      if (jobResult.rows.length === 0) {
        await client.query('ROLLBACK');
        throw new NotFoundError(`Job with id ${id} and lock_seq ${lockSeq} not found or not in PROCESSING status`);
      }

      const jobRow = jobResult.rows[0] as JobRow;
      const job = this.deserializeJob(jobRow);

      // Delete from active jobs table
      await client.query(`DELETE FROM jobs WHERE id = $1`, [id]);

      const completedAt = new Date();

      await this.releaseCoordinationSlots(client, queue.id, job.queueShardNo, job.groupId);

      await client.query('COMMIT');

      return {
        ...job,
        status: JobStatus.COMPLETED,
        completedAt,
        lockSeq: null,
        leaseExpiresAt: null,
      };
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * Public API: marks a job as completed
   * Automatically selects fast or full path based on queue configuration
   */
  async completeJob(id: number, lockSeq: number, queue: Queue): Promise<Job> {
    if (this.isFastPath(queue)) {
      return this.completeJobFast(id, lockSeq);
    }
    return this.completeJobWithCoordination(id, lockSeq, queue);
  }

  // ---------------------------------------------------------------------------
  // Fail job
  // ---------------------------------------------------------------------------

  /**
   * Fast path for failing jobs.
   * Single DELETE — mirrors completeJobFast.
   * Use when queue.concurrency === 0 AND queue.requiresGroupId === false
   */
  private async failJobFast(id: number, lockSeq: number, queue: Queue): Promise<Job> {
    // `attempts` was incremented when the job was leased, so a job still under
    // its budget goes straight back to PENDING. lease_seq is deliberately kept:
    // it is the fence token, and the next lease must out-rank the one that just
    // failed so a late settle from this worker is refused.
    const retried = await this.pool.query(
      `UPDATE jobs
       SET status = 'PENDING', lease_expires_at = NULL, updated_at = now()
       WHERE id = $1 AND lease_seq = $2 AND status = 'PROCESSING' AND attempts < $3
       RETURNING id, idempotency_key, payload, status, group_id, queue_id, queue_shard_no,
                 attempts, metadata, created_at, updated_at, lease_seq, lease_expires_at`,
      [id, lockSeq, queue.maxAttempts]
    );

    if (retried.rows.length > 0) {
      return this.deserializeJob(retried.rows[0] as JobRow);
    }

    // Budget spent (or the job is not ours) — discard it.
    const discarded = await this.pool.query(
      `DELETE FROM jobs
       WHERE id = $1 AND lease_seq = $2 AND status = 'PROCESSING'
       RETURNING id, idempotency_key, payload, status, group_id, queue_id, queue_shard_no,
                 attempts, metadata, created_at, updated_at, lease_seq, lease_expires_at`,
      [id, lockSeq]
    );

    if (discarded.rows.length === 0) {
      throw new NotFoundError(`Job with id ${id} and lock_seq ${lockSeq} not found or not in PROCESSING status`);
    }

    return {
      ...this.deserializeJob(discarded.rows[0] as JobRow),
      status: JobStatus.FAILED,
      completedAt: new Date(),
      lockSeq: null,
      leaseExpiresAt: null,
    };
  }

  /**
   * Full path for failing jobs — includes shard / group-limit coordination.
   * Use when queue.concurrency > 0 OR queue.requiresGroupId === true
   */
  private async failJobWithCoordination(id: number, lockSeq: number, queue: Queue): Promise<Job> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');

      // Lock and fetch the job row before deleting
      const jobResult = await client.query(
        `SELECT id, idempotency_key, payload, status, group_id, queue_id, queue_shard_no, attempts,
         metadata, created_at, updated_at, lease_seq, lease_expires_at
         FROM jobs WHERE id = $1 AND lease_seq = $2 AND status = 'PROCESSING' FOR UPDATE`,
        [id, lockSeq]
      );

      if (jobResult.rows.length === 0) {
        await client.query('ROLLBACK');
        throw new NotFoundError(`Job with id ${id} and lock_seq ${lockSeq} not found or not in PROCESSING status`);
      }

      const jobRow = jobResult.rows[0] as JobRow;
      const job = this.deserializeJob(jobRow);

      // The slot is released either way — the worker is done with this job.
      await this.releaseCoordinationSlots(client, queue.id, job.queueShardNo, job.groupId);

      if (job.attempts < queue.maxAttempts) {
        // Back to PENDING for another attempt. lease_seq is kept as the fence
        // token; queue_shard_no is cleared because the slot has been given back.
        const retried = await client.query(
          `UPDATE jobs
           SET status = 'PENDING', lease_expires_at = NULL, queue_shard_no = NULL, updated_at = now()
           WHERE id = $1
           RETURNING id, idempotency_key, payload, status, group_id, queue_id, queue_shard_no,
                     attempts, metadata, created_at, updated_at, lease_seq, lease_expires_at`,
          [id]
        );

        await client.query('COMMIT');
        return this.deserializeJob(retried.rows[0] as JobRow);
      }

      await client.query(`DELETE FROM jobs WHERE id = $1`, [id]);
      await client.query('COMMIT');

      return {
        ...job,
        status: JobStatus.FAILED,
        completedAt: new Date(),
        lockSeq: null,
        leaseExpiresAt: null,
      };
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * Public API: marks a job as failed
   * Automatically selects fast or full path based on queue configuration
   */
  async failJob(id: number, lockSeq: number, queue: Queue): Promise<Job> {
    if (this.isFastPath(queue)) {
      return this.failJobFast(id, lockSeq, queue);
    }
    return this.failJobWithCoordination(id, lockSeq, queue);
  }

  // ---------------------------------------------------------------------------
  // Discard job
  // ---------------------------------------------------------------------------

  /**
   * Removes a job without spending an attempt or scheduling a retry.
   *
   * This is the poison-message path: a payload that cannot be deserialized will
   * fail identically on every redelivery, so routing it through failJob would
   * only burn the attempt budget one useless run at a time. The job is deleted,
   * exactly as the attempt-exhausted paths already do, and any coordination
   * slots it held are released.
   */
  async discardJob(id: number, lockSeq: number, queue: Queue): Promise<Job> {
    if (this.isFastPath(queue)) {
      const result = await this.pool.query(
        `DELETE FROM jobs
         WHERE id = $1 AND lease_seq = $2 AND status = 'PROCESSING'
         RETURNING ${JOB_COLUMNS}`,
        [id, lockSeq]
      );

      if (result.rows.length === 0) {
        throw new NotFoundError(
          `Job with id ${id} and lock_seq ${lockSeq} not found or not in PROCESSING status`
        );
      }

      return this.asDiscarded(this.deserializeJob(result.rows[0] as JobRow));
    }

    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');

      const jobResult = await client.query(
        `SELECT ${JOB_COLUMNS}
         FROM jobs WHERE id = $1 AND lease_seq = $2 AND status = 'PROCESSING' FOR UPDATE`,
        [id, lockSeq]
      );

      if (jobResult.rows.length === 0) {
        await client.query('ROLLBACK');
        throw new NotFoundError(
          `Job with id ${id} and lock_seq ${lockSeq} not found or not in PROCESSING status`
        );
      }

      const job = this.deserializeJob(jobResult.rows[0] as JobRow);

      await this.releaseCoordinationSlots(client, queue.id, job.queueShardNo, job.groupId);
      await client.query(`DELETE FROM jobs WHERE id = $1`, [id]);
      await client.query('COMMIT');

      return this.asDiscarded(job);
    } catch (error) {
      await client.query('ROLLBACK').catch(() => {
        // Already rolled back above on the not-found path; the original error wins.
      });
      throw error;
    } finally {
      client.release();
    }
  }

  private asDiscarded(job: Job): Job {
    return {
      ...job,
      status: JobStatus.FAILED,
      completedAt: new Date(),
      lockSeq: null,
      leaseExpiresAt: null,
    };
  }

  // ---------------------------------------------------------------------------
  // Reaper
  // ---------------------------------------------------------------------------

  /**
   * Reclaims jobs whose lease has expired — the crash-recovery path.
   *
   * A job that has burned through max_attempts is discarded here rather than
   * reset, so a job that reliably kills its worker cannot loop for ever.
   */
  async recoverJobs(limit: number = 100): Promise<number[]> {
    const client = await this.pool.connect();

    try {
      await client.query('BEGIN');

      // max_attempts lives on the queue, and the reaper spans every queue, so
      // the budget has to be joined in. Only `jobs` is locked — `queues` is a
      // read-only lookup here.
      const listJobs = await client.query(
        `SELECT j.id, j.group_id, j.queue_id, j.queue_shard_no, j.attempts, q.max_attempts
         FROM jobs j
         JOIN queues q ON q.id = j.queue_id
         WHERE j.status = 'PROCESSING' AND j.lease_expires_at <= now()
         ORDER BY j.created_at DESC
         LIMIT $1
         FOR UPDATE OF j SKIP LOCKED`,
        [limit]
      );

      if (listJobs.rows.length === 0) {
        await client.query('ROLLBACK');
        return [];
      }

      const rows = listJobs.rows as Array<{
        id: number;
        group_id: string | null;
        queue_id: number;
        queue_shard_no: number | null;
        attempts: number;
        max_attempts: number;
      }>;

      // Give back every coordination slot the dead workers were holding. One
      // statement per counter type, aggregated by key: previously only the first
      // row of the batch was released, and it was released on every pass.
      const shardHolders = rows.filter((row) => row.queue_shard_no !== null);
      if (shardHolders.length > 0) {
        await client.query(
          `UPDATE queue_shards s
           SET running = GREATEST(s.running - d.released, 0), updated_at = now()
           FROM (
             SELECT queue_id, shard_no, count(*)::int AS released
             FROM unnest($1::bigint[], $2::int[]) AS t(queue_id, shard_no)
             GROUP BY queue_id, shard_no
           ) d
           WHERE s.queue_id = d.queue_id AND s.shard_no = d.shard_no`,
          [shardHolders.map((row) => row.queue_id), shardHolders.map((row) => row.queue_shard_no)]
        );
      }

      const groupHolders = rows.filter((row) => row.group_id !== null);
      if (groupHolders.length > 0) {
        await client.query(
          `UPDATE group_queue_limits g
           SET running = GREATEST(g.running - d.released, 0), updated_at = now()
           FROM (
             SELECT queue_id, group_id, count(*)::int AS released
             FROM unnest($1::bigint[], $2::text[]) AS t(queue_id, group_id)
             GROUP BY queue_id, group_id
           ) d
           WHERE g.queue_id = d.queue_id AND g.group_id = d.group_id`,
          [groupHolders.map((row) => row.queue_id), groupHolders.map((row) => row.group_id)]
        );
      }

      // A lease expiry counts as a spent attempt — `attempts` was already
      // incremented when the job was leased.
      const exhausted = rows.filter((row) => row.attempts >= row.max_attempts).map((row) => row.id);
      if (exhausted.length > 0) {
        await client.query(`DELETE FROM jobs WHERE id = ANY($1::bigint[])`, [exhausted]);
      }

      const retryable = rows.filter((row) => row.attempts < row.max_attempts).map((row) => row.id);
      let recovered: number[] = [];
      if (retryable.length > 0) {
        // lease_seq is NOT cleared. It is the fence token: keeping it means the
        // next lease is strictly higher, so a worker returning from the dead is
        // rejected when it tries to settle. Nulling it here handed the next
        // owner the very same token the zombie still held.
        const result = await client.query(
          `UPDATE jobs
           SET status = 'PENDING',
               lease_expires_at = NULL,
               queue_shard_no = NULL,
               updated_at = now()
           WHERE id = ANY($1::bigint[])
           RETURNING id`,
          [retryable]
        );
        recovered = result.rows.map((row) => Number(row.id));
      }

      await client.query('COMMIT');

      return recovered;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * Gives back the shard and group slots a job was occupying. Clamped at zero so
   * a counter that has already drifted cannot go negative — a negative `running`
   * satisfies `running < max_running` for ever, which would disable the cap.
   */
  private async releaseCoordinationSlots(
    client: PoolClient,
    queueId: number,
    queueShardNo: number | null,
    groupId: string | null
  ): Promise<void> {
    if (queueShardNo !== null) {
      await client.query(
        `UPDATE queue_shards SET running = GREATEST(running - 1, 0), updated_at = now()
         WHERE queue_id = $1 AND shard_no = $2`,
        [queueId, queueShardNo]
      );
    }

    if (groupId !== null) {
      await client.query(
        `UPDATE group_queue_limits SET running = GREATEST(running - 1, 0), updated_at = now()
         WHERE queue_id = $1 AND group_id = $2`,
        [queueId, groupId]
      );
    }
  }

  // ---------------------------------------------------------------------------
  // Deserialization helpers
  // ---------------------------------------------------------------------------

  /**
   * Deserialize an active jobs row (snake_case) → Job domain model (camelCase)
   */
  private deserializeJob(row: JobRow): Job {
    const metadata = this.deserializeMetadata(row.metadata);

    // id / queue_id / lease_seq are BIGINT. connection.ts registers an INT8
    // parser on this package's `pg`, but a caller-supplied client may come from
    // a different copy of pg and hand back strings — and `lease_seq + 1` on a
    // string is concatenation, which silently breaks lease fencing.
    return {
      id: Number(row.id),
      idempotencyKey: row.idempotency_key,
      payload: row.payload,
      queueShardNo: row.queue_shard_no === null ? null : Number(row.queue_shard_no),
      status: row.status as JobStatus,
      groupId: row.group_id,
      queueId: Number(row.queue_id),
      attempts: row.attempts,
      metadata,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
      completedAt: null,              // active jobs are never completed
      leaseExpiresAt: row.lease_expires_at,
      lockSeq: row.lease_seq === null ? null : Number(row.lease_seq),
    };
  }

  private deserializeMetadata(raw: any): Metadata {
    let metadata: Metadata = {};
    if (raw) {
      if (typeof raw === 'string') {
        metadata = JSON.parse(raw);
      } else {
        metadata = raw;
      }
    }

    // Normalise snake_case metadata keys to camelCase
    if (metadata.consumer_id !== undefined) {
      metadata.consumerId = metadata.consumer_id;
      delete metadata.consumer_id;
    }
    if (metadata.last_pull_at !== undefined) {
      metadata.lastPullAt = metadata.last_pull_at;
      delete metadata.last_pull_at;
    }

    return metadata;
  }
}
