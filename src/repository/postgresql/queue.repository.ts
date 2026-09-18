import { Pool } from 'pg';
import { Queue, CreateQueueInput, NUMBER_OF_SHARD, QueueShards } from '../../domain/queue';
import { ConflictError, NotFoundError } from '../../domain/errors';
import { Logger, consoleLogger } from '../../domain/logger';

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

  constructor(
    private pool: Pool,
    private logger: Logger = consoleLogger
  ) {}

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
      // Pinned rather than left to `default_transaction_isolation`, which any
      // deployment can move at the server, database or role level. The conflict
      // path below depends on it: measured on PG16, the same ON CONFLICT insert
      // under REPEATABLE READ fails with 40001 instead of quietly writing
      // nothing, leaving the transaction aborted so the read-back never runs.
      // Folded into BEGIN — a separate SET TRANSACTION costs a second round trip.
      await client.query('BEGIN ISOLATION LEVEL READ COMMITTED');
      const leaseDurationNs = leaseDuration * 1000000;
      const queueResult = await client.query(
        `INSERT INTO queues (name, max_attempts, lease_duration, concurrency, requires_group_id, created_at, updated_at) 
         VALUES ($1, $2, $3, $4, $5, now(), now()) 
         ON CONFLICT (name) DO NOTHING
         RETURNING id, name, max_attempts, lease_duration, concurrency, requires_group_id, created_at, updated_at`,
        [name, maxAttempts, leaseDurationNs, concurrency, requiresGroupId]
      );

      // RETURNING yields nothing when DO NOTHING fires, and that empty result is
      // the whole conflict signal: no error is raised, so the transaction stays
      // healthy and the winner is read back on the spot — where a bare insert
      // would have aborted it and answered 25P02 to every statement until it was
      // unwound. The insert blocked until the winning transaction committed, so
      // the row is on disk by now and this is one read, not a retry loop. Its
      // shard rows are left alone: a second set would multiply the queue's cap.
      if (queueResult.rows.length === 0) {
        const existingResult = await client.query(
          `SELECT id, name, max_attempts, lease_duration, concurrency, requires_group_id, created_at, updated_at
           FROM queues WHERE name = $1`,
          [name]
        );

        if (existingResult.rows.length === 0) {
          // Taken, then dropped again before this read. Nothing to hand back.
          throw new ConflictError(`Queue "${name}" was created and removed mid-create`);
        }

        const existing = this.deserializeQueue(existingResult.rows[0] as QueueRow);
        await client.query('COMMIT');

        this.logger.warn(
          `Queue "${name}" already exists (id ${existing.id}); returning the existing queue — ` +
            'the configuration passed to createQueue was not applied'
        );
        this.cache.set(existing.id, existing);
        return existing;
      }

      const queue = this.deserializeQueue(queueResult.rows[0] as QueueRow);

      if (concurrency > 0) {
        // Spread the configured concurrency across the shards without losing the
        // remainder: the first `concurrency % NUMBER_OF_SHARD` shards each take
        // one extra slot. Plain floor division would silently under-provision
        // (100 -> 96) and, for any concurrency below NUMBER_OF_SHARD, would give
        // every shard zero slots — leaving the queue unable to admit anything.
        const baseMaxRunning = Math.floor(concurrency / NUMBER_OF_SHARD);
        const remainder = concurrency % NUMBER_OF_SHARD;

        // A shard with max_running = 0 can never satisfy the pull's
        // `running < max_running` gate, so its row is dead weight — it only adds
        // rows for every shard pick to scan past. Those are exactly the shards
        // beyond the remainder when concurrency < NUMBER_OF_SHARD, so cap the
        // insert at the number of shards that actually carry a slot.
        const shardCount = Math.min(concurrency, NUMBER_OF_SHARD);

        const queueShardValues: any[] = [];
        const placeholders: string[] = [];
        for (let i = 0; i < shardCount; i++) {
          const offset = i * 4;
          placeholders.push(
            `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, now(), now())`
          );
          queueShardValues.push(queue.id, i, baseMaxRunning + (i < remainder ? 1 : 0), 0);
        }

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
}
