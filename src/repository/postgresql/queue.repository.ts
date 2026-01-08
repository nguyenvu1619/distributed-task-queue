import { Pool } from 'pg';
import { Queue, CreateQueueInput } from '../../domain/queue';
import { NotFoundError } from '../../domain/errors';

// Database row interface (snake_case)
interface QueueRow {
  id: string;
  name: string;
  max_attempts: string;
  lease_duration: string | number;
  created_at: Date;
  updated_at: Date;
}

export class QueueRepository {
  private cache: Map<number, Queue> = new Map();

  constructor(private pool: Pool) {}

  async getById(id: number): Promise<Queue> {
    // Check cache first
    const cached = this.cache.get(id);
    if (cached) {
      return cached;
    }

    const result = await this.pool.query(
      `SELECT id, name, max_attempts, lease_duration, created_at, updated_at 
       FROM queue WHERE id = $1`,
      [id]
    );

    if (result.rows.length === 0) {
      throw new NotFoundError(`Queue with id ${id} not found`);
    }

    const queue = this.deserializeQueue(result.rows[0] as QueueRow);
    this.cache.set(id, queue);
    return queue;
  }

  async getAll(): Promise<Queue[]> {
    const result = await this.pool.query(
      `SELECT id, name, max_attempts, lease_duration, created_at, updated_at FROM queue`
    );

    const queues = result.rows.map((row) => this.deserializeQueue(row as QueueRow));
    
    // Update cache
    queues.forEach((queue) => {
      this.cache.set(queue.id, queue);
    });

    return queues;
  }

  async createQueue(input: CreateQueueInput): Promise<Queue> {
    const client = await this.pool.connect();

    try {
      await client.query('BEGIN');
      await client.query('SET TRANSACTION ISOLATION LEVEL READ COMMITTED');

      // Serialize input to database format
      const serialized = this.serializeQueueInput(input);
      
      // Convert milliseconds to nanoseconds for storage (to match Go's time.Duration)
      const leaseDurationNs = input.leaseDuration * 1000000;
      
      const queueResult = await client.query(
        `INSERT INTO queue (name, max_attempts, lease_duration, created_at, updated_at) 
         VALUES ($1, $2, $3, now(), now()) 
         RETURNING id, name, max_attempts, lease_duration, created_at, updated_at`,
        [serialized.name, serialized.max_attempts, leaseDurationNs]
      );

      const queue = this.deserializeQueue(queueResult.rows[0] as QueueRow);

      // Insert queue permits
      const permitValues: any[] = [];
      const placeholders: string[] = [];
      
      for (let i = 0; i < input.concurrency; i++) {
        const offset = i * 3;
        placeholders.push(`($${offset + 1}, $${offset + 2}, now())`);
        permitValues.push(queue.id, i);
      }

      await client.query(
        `INSERT INTO queue_permits (queue_id, slot, updated_at) 
         VALUES ${placeholders.join(', ')}`,
        permitValues
      );

      await client.query('COMMIT');

      // Update cache
      this.cache.set(queue.id, queue);

      return queue;
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
  private deserializeQueue(row: QueueRow): Queue {
    // lease_duration is stored as BIGINT (nanoseconds in Go)
    // Convert nanoseconds to milliseconds for TypeScript
    const leaseDurationNs = typeof row.lease_duration === 'string' 
      ? parseInt(row.lease_duration, 10) 
      : row.lease_duration;
    const leaseDurationMs = leaseDurationNs / 1000000; // Convert nanoseconds to milliseconds

    return {
      id: parseInt(row.id, 10),
      name: row.name,
      maxAttempts: parseInt(row.max_attempts, 10),
      leaseDuration: leaseDurationMs,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
    };
  }

  /**
   * Serialize domain model (camelCase) to database format (snake_case)
   */
  private serializeQueueInput(input: CreateQueueInput): { name: string; max_attempts: number } {
    return {
      name: input.name,
      max_attempts: input.maxAttempts,
    };
  }
}
