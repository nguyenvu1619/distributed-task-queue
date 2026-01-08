import { Pool, PoolClient } from 'pg';
import { Job, JobStatus, CreateJobInput, Metadata } from '../../domain/job';
import { Queue } from '../../domain/queue';
import { NotFoundError } from '../../domain/errors';

// Database row interface (snake_case)
interface JobRow {
  id: string;
  idempotency_key: string;
  payload: string;
  status: string;
  group_id: string;
  queue_id: string;
  attempts: string;
  metadata: any;
  created_at: Date;
  updated_at: Date;
  completed_at: Date | null;
  lease_token: string | null;
  lease_expires_at: Date | null;
}

export class JobRepository {
  constructor(private pool: Pool) {}

  async getById(id: number): Promise<Job> {
    const result = await this.pool.query(
      `SELECT id, idempotency_key, payload, status, group_id, queue_id, attempts, 
       metadata, created_at, updated_at, completed_at, lease_token, lease_expires_at 
       FROM job WHERE id = $1`,
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

    const result = await this.pool.query(
      `INSERT INTO job (idempotency_key, payload, status, group_id, queue_id, attempts, metadata) 
       VALUES ($1, $2, $3, $4, $5, $6, $7) 
       RETURNING id, idempotency_key, payload, status, group_id, queue_id, attempts, 
       metadata, created_at, updated_at, completed_at, lease_token, lease_expires_at`,
      [
        input.idempotencyKey,
        input.payload,
        JobStatus.PENDING,
        input.groupId,
        input.queueId,
        attempts,
        JSON.stringify(metadata),
      ]
    );

    return this.deserializeJob(result.rows[0] as JobRow);
  }

  async pullJobs(status: JobStatus, limit: number): Promise<Job[]> {
    const result = await this.pool.query(
      `SELECT id, idempotency_key, payload, status, group_id, queue_id, attempts, 
       metadata, created_at, updated_at, completed_at, lease_token, lease_expires_at 
       FROM job WHERE status = $1 ORDER BY created_at LIMIT $2`,
      [status, limit]
    );

    return result.rows.map((row) => this.deserializeJob(row as JobRow));
  }

  async pullJob(queue: Queue): Promise<Job | null> {
    const client = await this.pool.connect();

    try {
      await client.query('BEGIN');
      await client.query('SET TRANSACTION ISOLATION LEVEL READ COMMITTED');

      // Select and lock a pending job
      const selectResult = await client.query(
        `SELECT id, idempotency_key, payload, status, group_id, queue_id, attempts, 
         metadata, created_at, updated_at, completed_at, lease_token 
         FROM job WHERE status = 'PENDING' AND queue_id = $1 
         ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED`,
        [queue.id]
      );

      if (selectResult.rows.length === 0) {
        await client.query('ROLLBACK');
        return null;
      }

      const job = this.deserializeJob(selectResult.rows[0] as JobRow);
      const newLockToken = (job.lockToken || 0) + 1;

      // Convert leaseDuration from milliseconds to PostgreSQL interval
      const leaseDurationMs = queue.leaseDuration;
      const leaseDurationSeconds = Math.floor(leaseDurationMs / 1000);
      const intervalStr = leaseDurationSeconds >= 60 
        ? `${Math.floor(leaseDurationSeconds / 60)} minutes ${leaseDurationSeconds % 60} seconds`
        : `${leaseDurationSeconds} seconds`;

      // Update job with lease
      await client.query(
        `UPDATE job SET lease_expires_at = now() + $1::interval, 
         lease_token = $2, status = 'PROCESSING' WHERE id = $3`,
        [intervalStr, newLockToken, job.id]
      );

      await client.query('COMMIT');

      return {
        ...job,
        lockToken: newLockToken,
        status: JobStatus.PROCESSING,
      };
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async completeJob(id: number, lockToken: number, queue: Queue): Promise<Job> {
    const result = await this.pool.query(
      `UPDATE job SET status = 'COMPLETED', completed_at = now() 
       WHERE lease_token = $1 AND id = $2 AND status = 'PROCESSING' 
       RETURNING id, idempotency_key, payload, status, group_id, queue_id, attempts, 
       metadata, created_at, updated_at, completed_at, lease_token, lease_expires_at`,
      [lockToken, id]
    );

    if (result.rows.length === 0) {
      throw new NotFoundError(`Job with id ${id} and lock_token ${lockToken} not found or not in PROCESSING status`);
    }

    return this.deserializeJob(result.rows[0] as JobRow);
  }

  async failJob(id: number, lockToken: number, queue: Queue): Promise<Job> {
    const result = await this.pool.query(
      `UPDATE job SET status = 'FAILED' 
       WHERE lease_token = $1 AND id = $2 AND status = 'PROCESSING' 
       RETURNING id, idempotency_key, payload, status, group_id, queue_id, attempts, 
       metadata, created_at, updated_at, completed_at, lease_token, lease_expires_at`,
      [lockToken, id]
    );

    if (result.rows.length === 0) {
      throw new NotFoundError(`Job with id ${id} and lock_token ${lockToken} not found or not in PROCESSING status`);
    }

    return this.deserializeJob(result.rows[0] as JobRow);
  }

  async recoverJobs(): Promise<number[]> {
    const client = await this.pool.connect();

    try {
      await client.query('BEGIN');
      await client.query('SET TRANSACTION ISOLATION LEVEL READ COMMITTED');

      const result = await client.query(
        `WITH cte AS (
          SELECT id
          FROM job
          WHERE status = 'PROCESSING'
          AND lease_expires_at <= now()
          FOR UPDATE SKIP LOCKED
          LIMIT 100
        )
        UPDATE job
        SET status = 'PENDING',
            lease_expires_at = NULL,
            lease_token = NULL
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
      id: parseInt(row.id, 10),
      idempotencyKey: row.idempotency_key,
      payload: row.payload,
      status: row.status as JobStatus,
      groupId: row.group_id,
      queueId: parseInt(row.queue_id, 10),
      attempts: parseInt(row.attempts, 10),
      metadata: metadata,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
      completedAt: row.completed_at,
      leaseExpiresAt: row.lease_expires_at,
      lockToken: row.lease_token ? parseInt(row.lease_token, 10) : 0,
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
    if (job.lockToken !== undefined) serialized.lease_token = job.lockToken.toString();

    return serialized;
  }
}
