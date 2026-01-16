import { Pool } from 'pg';
import { Queue, CreateQueueInput, NUMBER_OF_SHARD, QueueShards } from '../../domain/queue';
import { NotFoundError } from '../../domain/errors';

// Database row interface (snake_case)
interface QueueRow {
  id: number;
  name: string;
  max_attempts: number;
  lease_duration: number;
  created_at: Date;
  updated_at: Date;
  concurrency: number;
  requires_group_id: boolean;
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
      `SELECT id, name, max_attempts, lease_duration, concurrency, requires_group_id, created_at, updated_at 
       FROM queues WHERE id = $1`,
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
      `SELECT id, name, max_attempts, lease_duration, concurrency, requires_group_id, created_at, updated_at FROM queues`
    );

    const queues = result.rows.map((row) => this.deserializeQueue(row as QueueRow));
    
    // Update cache
    queues.forEach((queue) => {
      this.cache.set(queue.id, queue);
    });

    return queues;
  }

  async createQueue(input: CreateQueueInput): Promise<Queue> {
    const { name, maxAttempts, leaseDuration, concurrency, requiresGroupId = false } = input;
    const client = await this.pool.connect();

    try {
      await client.query('BEGIN');
      await client.query('SET TRANSACTION ISOLATION LEVEL READ COMMITTED');
      const leaseDurationNs = leaseDuration * 1000000;
      
      const queueResult = await client.query(
        `INSERT INTO queues (name, max_attempts, lease_duration, concurrency, requires_group_id, created_at, updated_at) 
         VALUES ($1, $2, $3, $4, $5, now(), now()) 
         RETURNING id, name, max_attempts, lease_duration, concurrency, requires_group_id, created_at, updated_at`,
        [name, maxAttempts, leaseDurationNs, concurrency, requiresGroupId]
      );

      const queue = this.deserializeQueue(queueResult.rows[0] as QueueRow);
if(concurrency > 0) {
      const queueShardValues: any[] = [];
      const placeholders: string[] = [];
      let tempMaxRunning = Math.floor(concurrency / NUMBER_OF_SHARD);
      for (let i = 0; i < NUMBER_OF_SHARD; i++) {   
        const offset = i * 4;
        placeholders.push(`($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, now(), now())`);
        queueShardValues.push(queue.id, i, tempMaxRunning, 0);
      }
      console.log('queueShardValues', queueShardValues);
      console.log('placeholders', placeholders);
      await client.query(
        `INSERT INTO queue_shards (queue_id, shard_no, max_running, running, created_at, updated_at) 
         VALUES ${placeholders.join(', ')}`,
         queueShardValues
      );
}
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
    const leaseDurationMs = row.lease_duration / 1000000; // Convert nanoseconds to milliseconds
    return {
      id: row.id,
      name: row.name,
      maxAttempts: row.max_attempts,
      leaseDuration: leaseDurationMs,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
      concurrency: row.concurrency,
      requiresGroupId: row.requires_group_id,
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
