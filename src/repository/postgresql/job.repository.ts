import { Pool } from 'pg';
import { Job, JobStatus, CreateJobInput, Metadata } from '../../domain/job';
import { Queue } from '../../domain/queue';
import { NotFoundError } from '../../domain/errors';

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
  constructor(private pool: Pool) {}

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

  async publishJob(input: CreateJobInput): Promise<Job> {
    const metadata = input.metadata || {};
    const attempts = input.attempts || 0;
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const publishResult = await client.query(
        `INSERT INTO jobs (idempotency_key, payload, status, group_id, queue_id, attempts, metadata)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         RETURNING id, idempotency_key, payload, status, group_id, queue_id, queue_shard_no, attempts,
         metadata, created_at, updated_at, lease_seq, lease_expires_at`,
        [
          input.idempotencyKey,
          input.payload,
          JobStatus.PENDING,
          input.group?.id || null,
          input.queueId,
          attempts,
          JSON.stringify(metadata),
        ]
      );
      if (input.group?.id && input.group?.concurrency) {
        await client.query(
          `INSERT INTO group_queue_limits (group_id, queue_id, max_running, running, updated_at, created_at)
           VALUES ($1, $2, $3, $4, now(), now()) ON CONFLICT DO NOTHING`,
          [input.group.id, input.queueId, input.group.concurrency, 0]
        );
      }
      await client.query('COMMIT');
      return this.deserializeJob(publishResult.rows[0] as JobRow);
    } catch (error) {
      console.error(error);
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
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
           lease_seq = COALESCE(lease_seq, 0) + 1
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
          console.warn(`No available queue shard for queue ${queue.id}`);
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
          console.warn(`Group queue limit reached for group ${job.groupId} and queue ${queue.id}`);
          return null;
        }
      }
      if (!isExpired && shareNo) {
        // increase running count
        await client.query(
          `UPDATE queue_shards SET running = running + 1 WHERE queue_id = $1 AND shard_no = $2`,
          [queue.id, shareNo]
        );
      }

      // Convert leaseDuration from milliseconds to PostgreSQL interval
      const leaseDurationMs = queue.leaseDuration;
      const leaseDurationSeconds = Math.floor(leaseDurationMs / 1000);
      const intervalStr = leaseDurationSeconds >= 60
        ? `${Math.floor(leaseDurationSeconds / 60)} minutes ${leaseDurationSeconds % 60} seconds`
        : `${leaseDurationSeconds} seconds`;

      // Update job with lease
      await client.query(
        `UPDATE jobs SET lease_expires_at = now() + $1::interval, queue_shard_no = $2,
         lease_seq = $3, status = 'PROCESSING' WHERE id = $4`,
        [intervalStr, shareNo, newLockSeq, job.id]
      );

      await client.query('COMMIT');

      return {
        ...job,
        lockSeq: newLockSeq,
        status: JobStatus.PROCESSING,
      };
    } catch (error) {
      await client.query('ROLLBACK');
      console.error('error', error);
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
    if ((queue.concurrency === 0 || queue.concurrency === null) && !queue.requiresGroupId) {
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

      // Decrease shard running count
      if (job.queueShardNo) {
        await client.query(
          `UPDATE queue_shards SET running = running - 1 WHERE queue_id = $1 AND shard_no = $2`,
          [queue.id, job.queueShardNo]
        );
      }

      // Decrease group queue limits
      if (job.groupId) {
        await client.query(
          `UPDATE group_queue_limits SET running = running - 1 WHERE group_id = $1 AND queue_id = $2`,
          [job.groupId, queue.id]
        );
      }

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
    if ((queue.concurrency === 0 || queue.concurrency === null) && !queue.requiresGroupId) {
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
  private async failJobFast(id: number, lockSeq: number): Promise<Job> {
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

    const failedAt = new Date();
    return {
      ...this.deserializeJob(result.rows[0] as JobRow),
      status: JobStatus.FAILED,
      completedAt: failedAt,
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

      // Delete from active jobs table
      await client.query(`DELETE FROM jobs WHERE id = $1`, [id]);

      const failedAt = new Date();

      // Decrease shard running count
      if (job.queueShardNo) {
        await client.query(
          `UPDATE queue_shards SET running = running - 1 WHERE queue_id = $1 AND shard_no = $2`,
          [queue.id, job.queueShardNo]
        );
      }

      // Decrease group queue limits
      if (job.groupId) {
        await client.query(
          `UPDATE group_queue_limits SET running = running - 1 WHERE group_id = $1 AND queue_id = $2`,
          [job.groupId, queue.id]
        );
      }

      await client.query('COMMIT');

      return {
        ...job,
        status: JobStatus.FAILED,
        completedAt: failedAt,
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
    if ((queue.concurrency === 0 || queue.concurrency === null) && !queue.requiresGroupId) {
      return this.failJobFast(id, lockSeq);
    }
    return this.failJobWithCoordination(id, lockSeq, queue);
  }

  // ---------------------------------------------------------------------------
  // Reaper
  // ---------------------------------------------------------------------------

  // Recover jobs that are expired (reaper process)
  async recoverJobs(): Promise<number[]> {
    const client = await this.pool.connect();

    try {
      await client.query('BEGIN');
      const listJobs = await client.query(
        `SELECT id, group_id, queue_id, queue_shard_no FROM jobs WHERE status = 'PROCESSING' AND lease_expires_at <= now() ORDER BY created_at DESC LIMIT 100 FOR UPDATE SKIP LOCKED`,
      );
      if (listJobs.rows.length === 0) {
        await client.query('ROLLBACK');
        return [];
      }
      const requireCoordinationJobs = listJobs.rows.some((row) => row.group_id !== null || row.queue_shard_no !== null);
      if (requireCoordinationJobs) {
        // Decrease the running count of the queue shard
        await client.query(
          `UPDATE queue_shards SET running = running - 1 WHERE queue_id = $1 AND shard_no = $2`,
          [listJobs.rows[0].queue_id, listJobs.rows[0].queue_shard_no]
        );
        // Decrease the running count of the group queue limits
        await client.query(
          `UPDATE group_queue_limits SET running = running - 1 WHERE group_id = $1 AND queue_id = $2`,
          [listJobs.rows[0].group_id, listJobs.rows[0].queue_id]
        );
        await client.query('COMMIT');
        return [];
      }

      const jobIds = listJobs.rows.map((row) => row.id);
      const result = await client.query(
        `UPDATE jobs
         SET status = 'PENDING',
             lease_expires_at = NULL,
             lease_seq = NULL
         WHERE id = ANY($1::bigint[])
         RETURNING id`,
        [jobIds]
      );

      await client.query('COMMIT');

      return result.rows.map((row) => parseInt(row.id, 10));
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
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

    return {
      id: row.id,
      idempotencyKey: row.idempotency_key,
      payload: row.payload,
      queueShardNo: row.queue_shard_no,
      status: row.status as JobStatus,
      groupId: row.group_id,
      queueId: row.queue_id,
      attempts: row.attempts,
      metadata,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
      completedAt: null,              // active jobs are never completed
      leaseExpiresAt: row.lease_expires_at,
      lockSeq: row.lease_seq,
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
