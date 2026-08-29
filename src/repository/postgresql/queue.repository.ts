import { Pool } from 'pg';
import { Queue, CreateQueueInput, NUMBER_OF_SHARD } from '../../domain/queue';
import { NotFoundError } from '../../domain/errors';
import { Logger, consoleLogger } from '../../domain/logger';

const QUEUE_COLUMNS = `id, name, max_attempts, lease_duration, concurrency, requires_group_id,
         created_at, updated_at`;

const UNIQUE_VIOLATION = '23505';

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
  private cacheByName: Map<string, Queue> = new Map();

  constructor(
    private pool: Pool,
    private logger: Logger = consoleLogger
  ) {}

  private remember(queue: Queue): Queue {
    this.cache.set(queue.id, queue);
    this.cacheByName.set(queue.name, queue);
    return queue;
  }

  async getByName(name: string): Promise<Queue | null> {
    const cached = this.cacheByName.get(name);
    if (cached) {
      return cached;
    }

    const result = await this.pool.query(
      `SELECT ${QUEUE_COLUMNS} FROM queues WHERE name = $1`,
      [name]
    );

    if (result.rows.length === 0) {
      return null;
    }

    return this.remember(this.deserializeQueue(result.rows[0] as QueueRow));
  }

  /**
   * Get-or-create by name. Two processes booting at once both call this, so the
   * loser of the INSERT race re-reads the winner's row instead of failing.
   *
   * Deliberately does not accept a caller transaction: createQueue issues
   * `SET TRANSACTION ISOLATION LEVEL`, which Postgres rejects (25001) once any
   * statement has run in the enclosing transaction. Only publishing is
   * transaction-joinable.
   */
  async ensureQueue(input: CreateQueueInput): Promise<Queue> {
    const existing = await this.getByName(input.name);
    if (existing) {
      this.warnOnConfigDrift(existing, input);
      return existing;
    }

    try {
      return await this.createQueue(input);
    } catch (error) {
      if ((error as { code?: string }).code !== UNIQUE_VIOLATION) {
        throw error;
      }
      // A concurrent creator won. Their configuration is now the queue's.
      const winner = await this.getByName(input.name);
      if (!winner) {
        throw error;
      }
      this.warnOnConfigDrift(winner, input);
      return winner;
    }
  }

  /**
   * Queue configuration is immutable in v1 — there is no update path — so a
   * mismatch between the stored queue and what the caller asked for is silently
   * ignored unless we say something.
   */
  private warnOnConfigDrift(existing: Queue, requested: CreateQueueInput): void {
    const drift: string[] = [];
    if (existing.maxAttempts !== requested.maxAttempts) {
      drift.push(`maxAttempts ${existing.maxAttempts} != ${requested.maxAttempts}`);
    }
    if (existing.leaseDuration !== requested.leaseDuration) {
      drift.push(`leaseDuration ${existing.leaseDuration} != ${requested.leaseDuration}`);
    }
    if ((existing.concurrency ?? 0) !== requested.concurrency) {
      drift.push(`concurrency ${existing.concurrency} != ${requested.concurrency}`);
    }
    if (existing.requiresGroupId !== (requested.requiresGroupId ?? false)) {
      drift.push(
        `requiresGroupId ${existing.requiresGroupId} != ${requested.requiresGroupId ?? false}`
      );
    }

    if (drift.length > 0) {
      this.logger.warn(
        `Queue "${existing.name}" already exists with a different configuration ` +
          `(${drift.join(', ')}). Queue configuration is immutable; the stored values are in effect.`
      );
    }
  }

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

    return this.remember(this.deserializeQueue(result.rows[0] as QueueRow));
  }

  async getAll(): Promise<Queue[]> {
    const result = await this.pool.query(
      `SELECT id, name, max_attempts, lease_duration, concurrency, requires_group_id, created_at, updated_at FROM queues`
    );

    const queues = result.rows.map((row) => this.deserializeQueue(row as QueueRow));
    
    // Update cache
    queues.forEach((queue) => this.remember(queue));

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

      if (concurrency > 0) {
        // Spread the configured concurrency across the shards without losing the
        // remainder: the first `concurrency % NUMBER_OF_SHARD` shards each take
        // one extra slot. Plain floor division would silently under-provision
        // (100 -> 96) and, for any concurrency below NUMBER_OF_SHARD, would give
        // every shard zero slots — leaving the queue unable to admit anything.
        const baseMaxRunning = Math.floor(concurrency / NUMBER_OF_SHARD);
        const remainder = concurrency % NUMBER_OF_SHARD;

        const queueShardValues: any[] = [];
        const placeholders: string[] = [];
        for (let i = 0; i < NUMBER_OF_SHARD; i++) {
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

      // Cached only after COMMIT: an aborted create must not leave a phantom
      // queue memoised in-process.
      return this.remember(queue);
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
}
