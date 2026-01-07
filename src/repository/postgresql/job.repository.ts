import { Pool, PoolClient } from 'pg';
import { Job, JobStatus, CreateJobInput, Metadata } from '../../domain/job';
import { Queue } from '../../domain/queue';
import { NotFoundError } from '../../domain/errors';

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

    return this.mapRowToJob(result.rows[0]);
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
        input.idempotency_key,
        input.payload,
        JobStatus.PENDING,
        input.group_id,
        input.queue_id,
        attempts,
        JSON.stringify(metadata),
      ]
    );

    return this.mapRowToJob(result.rows[0]);
  }

  async pullJobs(status: JobStatus, limit: number): Promise<Job[]> {
    const result = await this.pool.query(
      `SELECT id, idempotency_key, payload, status, group_id, queue_id, attempts, 
       metadata, created_at, updated_at, completed_at, lease_token, lease_expires_at 
       FROM job WHERE status = $1 ORDER BY created_at LIMIT $2`,
      [status, limit]
    );

    return result.rows.map((row) => this.mapRowToJob(row));
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

      const job = this.mapRowToJob(selectResult.rows[0]);
      const newLockToken = (job.lockToken || 0) + 1;

      // Convert lease_duration from milliseconds to PostgreSQL interval
      // queue.lease_duration is in milliseconds, convert to interval string
      const leaseDurationMs = queue.lease_duration;
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
        lock_token: newLockToken,
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

    return this.mapRowToJob(result.rows[0]);
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

    return this.mapRowToJob(result.rows[0]);
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

  private mapRowToJob(row: any): Job {
    let metadata: Metadata = {};
    if (row.metadata) {
      if (typeof row.metadata === 'string') {
        metadata = JSON.parse(row.metadata);
      } else {
        metadata = row.metadata;
      }
    }

    return {
      id: parseInt(row.id, 10),
      idempotency_key: row.idempotency_key,
      payload: row.payload,
      status: row.status as JobStatus,
      group_id: row.group_id,
      queue_id: parseInt(row.queue_id, 10),
      attempts: parseInt(row.attempts, 10),
      metadata: metadata,
      created_at: row.created_at,
      updated_at: row.updated_at,
      completed_at: row.completed_at,
      lease_expires_at: row.lease_expires_at,
      lock_token: row.lease_token ? parseInt(row.lease_token, 10) : 0,
    };
  }
}

