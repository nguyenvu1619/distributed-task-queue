import { Pool, PoolClient } from 'pg';
import { Job, JobStatus, CreateJobInput, Metadata } from '../../domain/job';
import { NUMBER_OF_SHARD, Queue } from '../../domain/queue';
import { NotFoundError } from '../../domain/errors';

// Database row interface (snake_case)
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
  completed_at: Date | null;
  lease_seq: number | null;
  lease_expires_at: Date | null;
}

export class JobRepository {
  constructor(private pool: Pool) {}

  async getById(id: number): Promise<Job> {
    const result = await this.pool.query(
      `SELECT id, idempotency_key, payload, status, group_id, queue_id, queue_shard_no, attempts, 
       metadata, created_at, updated_at, completed_at, lease_seq, lease_expires_at 
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
         metadata, created_at, updated_at, completed_at, lease_seq, lease_expires_at`,
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
      if(input.group?.id && input.group?.concurrency) {
        // if(!input.group.concurrency) {
        //     throw new Error('Group concurrency is required');
        // }
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
       metadata, created_at, updated_at, completed_at, lease_seq, lease_expires_at 
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
                 completed_at, lease_seq, lease_expires_at`,
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
      if(queue.concurrency) {
      const queueShardResult = await client.query(
        `SELECT max_running, running FROM queue_shards WHERE queue_id = $1 AND running < max_running FOR UPDATE SKIP LOCKED LIMIT 1`,
        [queue.id]
      );
      if(queueShardResult.rows.length === 0) {
        await client.query('ROLLBACK');
        console.warn(`No available queue shard for queue ${queue.id}`);
        return null;
      }
      shareNo = queueShardResult.rows[0].shard_no;
    }
      // Select and lock a pending job
      const selectResult = await client.query(
        `SELECT id, idempotency_key, payload, status, group_id, queue_id, queue_shard_no, attempts, 
         metadata, created_at, updated_at, completed_at, lease_seq 
         FROM jobs WHERE status = 'PENDING' AND queue_id = $1
         ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED`,
        [queue.id]
      );

      if (selectResult.rows.length === 0) {
        await client.query('ROLLBACK');
        return null;
      }

      const job = this.deserializeJob(selectResult.rows[0] as JobRow);
      const isExpired = job.status === JobStatus.PROCESSING // if the job is processing, it means it's expired
      const newLockSeq = (job.lockSeq || 0) + 1;

      // Update group queue limits
      if(job.groupId && !isExpired) {
        const groupQueueLimitResult = await client.query(
          `UPDATE group_queue_limits SET running = running + 1 WHERE group_id = $1 AND queue_id = $2 AND running < max_running RETURNING group_id`,
          [job.groupId, queue.id]
        );
        if(groupQueueLimitResult.rows.length === 0) {
          await client.query('ROLLBACK');
          console.warn(`Group queue limit reached for group ${job.groupId} and queue ${queue.id}`);
          return null;
        }
      }
      if(!isExpired && shareNo) {
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
    // Fast path only if BOTH conditions are met:
    // 1. No concurrency limit (concurrency === 0 or null)
    // 2. No group coordination required (requiresGroupId === false)
    if ((queue.concurrency === 0 || queue.concurrency === null) && !queue.requiresGroupId) {
      return this.pullJobFast(queue);
    }
    return this.pullJobWithCoordination(queue);
  }

  /**
   * Fast path for completing jobs - single query, no transactions
   * Use when queue.concurrency === 0 AND queue.requiresGroupId === false
   */
  private async completeJobFast(id: number, lockSeq: number): Promise<Job> {
    const result = await this.pool.query(
      `UPDATE jobs 
       SET status = 'COMPLETED', 
           completed_at = now(), 
           lease_seq = NULL, 
           lease_expires_at = NULL
       WHERE id = $1 AND lease_seq = $2 AND status = 'PROCESSING'
       RETURNING id, idempotency_key, payload, status, group_id, queue_id, queue_shard_no, attempts, 
                 metadata, created_at, updated_at, completed_at, lease_seq, lease_expires_at`,
      [id, lockSeq]
    );
    
    if (result.rows.length === 0) {
      throw new NotFoundError(`Job with id ${id} and lock_seq ${lockSeq} not found or not in PROCESSING status`);
    }
    
    return this.deserializeJob(result.rows[0] as JobRow);
  }

  /**
   * Full path for completing jobs - multiple queries with transactions
   * Use when queue.concurrency > 0 OR queue.requiresGroupId === true
   */
  private async completeJobWithCoordination(id: number, lockSeq: number, queue: Queue): Promise<Job> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const result = await client.query(
        `UPDATE jobs SET status = 'COMPLETED', completed_at = now(), lease_seq = NULL, lease_expires_at = NULL
       WHERE lease_seq = $1 AND id = $2 AND status = 'PROCESSING' 
       RETURNING id, idempotency_key, payload, status, group_id, queue_id, queue_shard_no, attempts, 
       metadata, created_at, updated_at, completed_at, lease_seq, lease_expires_at`,
      [lockSeq, id]
      );
      if(result.rows.length === 0) {
        await client.query('ROLLBACK');
        throw new NotFoundError(`Job with id ${id} and lock_token ${lockSeq} not found or not in PROCESSING status`);
      }
      const job = this.deserializeJob(result.rows[0] as JobRow);
      // decrease running count
      if(job.queueShardNo) {
        await client.query(
          `UPDATE queue_shards SET running = running - 1 WHERE queue_id = $1 AND shard_no = $2`,
          [queue.id, job.queueShardNo]
        );
      }
      // decrease group queue limits
      if(job.groupId) {
      await client.query(
          `UPDATE group_queue_limits SET running = running - 1 WHERE group_id = $1 AND queue_id = $2`,
          [job.groupId, queue.id]
        );
      }
      await client.query('COMMIT');
    return job;
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
   * Fast path requires BOTH: concurrency === 0 AND requiresGroupId === false
   */
  async completeJob(id: number, lockSeq: number, queue: Queue): Promise<Job> {
    if ((queue.concurrency === 0 || queue.concurrency === null) && !queue.requiresGroupId) {
      return this.completeJobFast(id, lockSeq);
    }
    return this.completeJobWithCoordination(id, lockSeq, queue);
  }

  /**
   * Fast path for failing jobs - single query, no transactions
   * Use when queue.concurrency === 0 AND queue.requiresGroupId === false
   */
  private async failJobFast(id: number, lockSeq: number): Promise<Job> {
    const result = await this.pool.query(
      `UPDATE jobs 
       SET status = 'FAILED',
           lease_seq = NULL,
           lease_expires_at = NULL
       WHERE id = $1 AND lease_seq = $2 AND status = 'PROCESSING'
       RETURNING id, idempotency_key, payload, status, group_id, queue_id, queue_shard_no, attempts, 
                 metadata, created_at, updated_at, completed_at, lease_seq, lease_expires_at`,
      [id, lockSeq]
    );
    
    if (result.rows.length === 0) {
      throw new NotFoundError(`Job with id ${id} and lock_seq ${lockSeq} not found or not in PROCESSING status`);
    }
    
    return this.deserializeJob(result.rows[0] as JobRow);
  }

  /**
   * Full path for failing jobs - multiple queries with transactions
   * Use when queue.concurrency > 0 OR queue.requiresGroupId === true
   */
  private async failJobWithCoordination(id: number, lockSeq: number, queue: Queue): Promise<Job> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const result = await client.query(
      `UPDATE jobs SET status = 'FAILED' 
       WHERE lease_seq = $1 AND id = $2 AND status = 'PROCESSING' 
       RETURNING id, idempotency_key, payload, status, group_id, queue_id, queue_shard_no, attempts, 
       metadata, created_at, updated_at, completed_at, lease_seq, lease_expires_at`,
      [lockSeq, id]
    );
    if(result.rows.length === 0) {
      await client.query('ROLLBACK');
      throw new NotFoundError(`Job with id ${id} and lock_seq ${lockSeq} not found or not in PROCESSING status`);
    }
    const job = this.deserializeJob(result.rows[0] as JobRow);
    // decrease running count
    if(job.queueShardNo) {
      await client.query(
        `UPDATE queue_shards SET running = running - 1 WHERE queue_id = $1 AND shard_no = $2`,
        [queue.id, job.queueShardNo]
      );
    }
    // decrease group queue limits
    if(job.groupId) {
      await client.query(
        `UPDATE group_queue_limits SET running = running - 1 WHERE group_id = $1 AND queue_id = $2`,
        [job.groupId, queue.id]
      );
    }
    await client.query('COMMIT');
    return job;
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
   * Fast path requires BOTH: concurrency === 0 AND requiresGroupId === false
   */
  async failJob(id: number, lockSeq: number, queue: Queue): Promise<Job> {
    if ((queue.concurrency === 0 || queue.concurrency === null) && !queue.requiresGroupId) {
      return this.failJobFast(id, lockSeq);
    }
    return this.failJobWithCoordination(id, lockSeq, queue);
  }

  // Recover jobs that are expired (reaper process, consider if we need it or we can recover job in the pullJob method)
  async recoverJobs(): Promise<number[]> {
    const client = await this.pool.connect();

    try {
      await client.query('BEGIN');
      const result = await client.query(
        `WITH cte AS (
          SELECT id
          FROM job
          WHERE status = 'PROCESSING'
          AND lease_expires_at <= now()
          FOR UPDATE SKIP LOCKED
          ORDER BY created_at DESC
          LIMIT 100
        )
        UPDATE job
        SET status = 'PENDING',
            lease_expires_at = NULL,
            lease_seq = NULL
        WHERE id IN (SELECT id FROM cte)
        RETURNING id`
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

  /**
   * Deserialize database row (snake_case) to domain model (camelCase)
   */
  private deserializeJob(row: JobRow): Job {
    let metadata: Metadata = {};
    if (row.metadata) {
      if (typeof row.metadata === 'string') {
        metadata = JSON.parse(row.metadata);
      } else {
        metadata = row.metadata;
      }
    }

    // Convert snake_case metadata keys to camelCase if needed
    if (metadata.consumer_id !== undefined) {
      metadata.consumerId = metadata.consumer_id;
      delete metadata.consumer_id;
    }
    if (metadata.last_pull_at !== undefined) {
      metadata.lastPullAt = metadata.last_pull_at;
      delete metadata.last_pull_at;
    }

    return {
      id: row.id,
      idempotencyKey: row.idempotency_key,
      payload: row.payload,
      queueShardNo: row.queue_shard_no,
      status: row.status as JobStatus,
      groupId: row.group_id,
      queueId: row.queue_id,
      attempts: row.attempts,
      metadata: metadata,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
      completedAt: row.completed_at,
      leaseExpiresAt: row.lease_expires_at,
      lockSeq: row.lease_seq,
    };
  }

  /**
   * Serialize domain model (camelCase) to database format (snake_case)
   * Used when inserting/updating jobs
   */
  private serializeJob(job: Partial<Job>): Partial<JobRow> {
    const serialized: any = {};
    
    if (job.id !== undefined) serialized.id = job.id.toString();
    if (job.idempotencyKey !== undefined) serialized.idempotency_key = job.idempotencyKey;
    if (job.payload !== undefined) serialized.payload = job.payload;
    if (job.status !== undefined) serialized.status = job.status;
    if (job.groupId !== undefined) serialized.group_id = job.groupId;
    if (job.queueId !== undefined) serialized.queue_id = job.queueId.toString();
    if (job.attempts !== undefined) serialized.attempts = job.attempts.toString();
    if (job.metadata !== undefined) {
      // Convert camelCase metadata keys to snake_case if needed
      const metadataCopy = { ...job.metadata };
      if (metadataCopy.consumerId !== undefined) {
        metadataCopy.consumer_id = metadataCopy.consumerId;
        delete metadataCopy.consumerId;
      }
      if (metadataCopy.lastPullAt !== undefined) {
        metadataCopy.last_pull_at = metadataCopy.lastPullAt;
        delete metadataCopy.lastPullAt;
      }
      serialized.metadata = JSON.stringify(metadataCopy);
    }
    if (job.createdAt !== undefined) serialized.created_at = job.createdAt;
    if (job.updatedAt !== undefined) serialized.updated_at = job.updatedAt;
    if (job.completedAt !== undefined) serialized.completed_at = job.completedAt;
    if (job.leaseExpiresAt !== undefined) serialized.lease_expires_at = job.leaseExpiresAt;
    if (job.lockSeq !== undefined) serialized.lease_seq = job.lockSeq;

    return serialized;
  }
}
