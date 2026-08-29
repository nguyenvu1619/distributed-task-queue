import { Pool } from 'pg';
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
    // A single statement is already atomic, so a standalone publish needs no
    // BEGIN/COMMIT of its own. With an executor it joins the caller's
    // transaction.
    return this.insertJobs(executor ?? this.pool, inputs);
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

    // The group-limit rows ride in the same statement as the insert. That keeps
    // the pair atomic without a transaction: a job whose limit row never landed
    // would fail the group gate on every pull and jam the queue head for ever.
    const limitPlaceholders: string[] = [];
    for (const group of groups.values()) {
      const offset = values.length;
      limitPlaceholders.push(`($${offset + 1}, $${offset + 2}, $${offset + 3}, 0, now(), now())`);
      values.push(group.groupId, group.queueId, group.concurrency);
    }
    const limitsCte =
      limitPlaceholders.length > 0
        ? `,
       limits AS (
         INSERT INTO group_queue_limits (group_id, queue_id, max_running, running, updated_at, created_at)
         VALUES ${limitPlaceholders.join(', ')}
         ON CONFLICT DO NOTHING
       )`
        : '';

    // ON CONFLICT DO NOTHING rather than letting the UNIQUE violation fly: a
    // raw 23505 aborts the *whole* enclosing transaction, so a duplicate
    // publish would take the caller's business writes down with it.
    const inserted = await db.query(
      `WITH ins AS (
         INSERT INTO jobs (idempotency_key, payload, status, group_id, queue_id, attempts, metadata)
         VALUES ${placeholders.join(', ')}
         ON CONFLICT (idempotency_key) DO NOTHING
         RETURNING ${JOB_COLUMNS}
       )${limitsCte}
       SELECT * FROM ins`,
      values
    );

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
   * Full path for pulling jobs — shard and group coordination folded into ONE
   * statement, so it costs a single round trip exactly like the fast path.
   *
   * Why the chain needs no BEGIN/COMMIT:
   * - A single statement runs in its own implicit transaction, so either every
   *   CTE's write applies or none does — a refused gate is a CTE that matches
   *   zero rows, and nothing downstream of it writes.
   * - Writes only ever flow forward through RETURNING references (CTEs share
   *   one snapshot and cannot see each other's writes any other way).
   * - Each gate is an atomic conditional UPDATE: the group cap re-checks
   *   `running < max_running` under the row lock at write time.
   * - Lock order is shard → job → group, the same order settle and the reaper
   *   release in, so the graph stays acyclic; the shard pick is SKIP LOCKED,
   *   so pullers never queue on it.
   *
   * Use when queue.concurrency > 0 OR queue.requiresGroupId === true
   */
  private async pullJobWithCoordination(queue: Queue): Promise<Job | null> {
    const sharded = Boolean(queue.concurrency);

    // The shard CTEs only exist for sharded queues; a group-only queue skips
    // straight to the candidate. Assembled here because queue config is static
    // per call — the group gate stays dynamic since it depends on the job row.
    const shardCte = sharded
      ? `shard AS (
           SELECT queue_id AS qid, shard_no
           FROM queue_shards
           WHERE queue_id = $2 AND running < max_running
           FOR UPDATE SKIP LOCKED
           LIMIT 1
         ),`
      : '';
    const candidateGate = sharded ? 'AND EXISTS (SELECT 1 FROM shard)' : '';
    const admittedShard = sharded
      ? 'SELECT c.job_id, c.grp, s.shard_no FROM candidate c, shard s'
      : 'SELECT c.job_id, c.grp, NULL::int AS shard_no FROM candidate c';
    const shardBumpCte = sharded
      ? `shard_bump AS (
           UPDATE queue_shards qs
           SET running = qs.running + 1
           FROM admitted a
           WHERE qs.queue_id = $2 AND qs.shard_no = a.shard_no
           RETURNING qs.shard_no
         ),`
      : '';
    const leaseGate = sharded ? 'AND EXISTS (SELECT 1 FROM shard_bump)' : '';
    const shardDiag = sharded ? 'EXISTS (SELECT 1 FROM shard)' : 'TRUE';

    const result = await this.pool.query(
      `WITH ${shardCte}
       candidate AS (
         SELECT id AS job_id, group_id AS grp
         FROM jobs
         WHERE status = 'PENDING' AND queue_id = $2
           ${candidateGate}
         ORDER BY created_at
         FOR UPDATE SKIP LOCKED
         LIMIT 1
       ),
       admitted_group AS (
         UPDATE group_queue_limits g
         SET running = g.running + 1, updated_at = now()
         FROM candidate c
         WHERE c.grp IS NOT NULL
           AND g.queue_id = $2 AND g.group_id = c.grp
           AND g.running < g.max_running
         RETURNING g.group_id
       ),
       admitted AS (
         ${admittedShard}
         WHERE c.grp IS NULL OR EXISTS (SELECT 1 FROM admitted_group)
       ),
       ${shardBumpCte}
       leased AS (
         UPDATE jobs j
         SET status = 'PROCESSING',
             lease_expires_at = now() + ($1 || ' milliseconds')::interval,
             queue_shard_no = a.shard_no,
             lease_seq = COALESCE(j.lease_seq, 0) + 1,
             attempts = j.attempts + 1,
             updated_at = now()
         FROM admitted a
         WHERE j.id = a.job_id
           ${leaseGate}
         RETURNING ${JOB_COLUMNS}
       )
       SELECT d.has_shard, d.candidate_group, d.group_admitted, l.*
       FROM (SELECT ${shardDiag} AS has_shard,
                    (SELECT c.grp FROM candidate c) AS candidate_group,
                    EXISTS (SELECT 1 FROM admitted_group) AS group_admitted) d
       LEFT JOIN leased l ON TRUE`,
      [queue.leaseDuration, queue.id]
    );

    // Exactly one row always comes back: diagnostics, plus the job when one
    // was leased. The warns cover the two gates that can refuse a pull.
    const row = result.rows[0];
    if (row.id !== null && row.id !== undefined) {
      return this.deserializeJob(row as JobRow);
    }
    if (!row.has_shard) {
      this.logger.warn(`No available queue shard for queue ${queue.id}`);
    } else if (row.candidate_group !== null && !row.group_admitted) {
      this.logger.warn(
        `Group queue limit reached for group ${row.candidate_group} and queue ${queue.id}`
      );
    }
    return null;
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
   * Delete and slot release travel as one statement (one round trip), atomic by
   * virtue of being a single statement rather than a transaction.
   * Use when queue.concurrency > 0 OR queue.requiresGroupId === true
   */
  private async completeJobWithCoordination(id: number, lockSeq: number, queue: Queue): Promise<Job> {
    const row = await this.deleteWithCoordination(id, lockSeq, queue);
    if (!row) {
      throw new NotFoundError(`Job with id ${id} and lock_seq ${lockSeq} not found or not in PROCESSING status`);
    }

    return {
      ...this.deserializeJob(row),
      status: JobStatus.COMPLETED,
      completedAt: new Date(),
      lockSeq: null,
      leaseExpiresAt: null,
    };
  }

  /**
   * Deletes a settled job and gives back its shard / group slots in one
   * statement. Shared by complete (fence-checked success) and discard (poison
   * removal) — the SQL is identical, only the returned Job shape differs.
   *
   * Slot release is chained shard-then-group via the `shard_release` reference:
   * independent data-modifying CTEs run in unspecified order, and releasing in
   * the opposite order of pull's shard → job → group locking could deadlock.
   * The clamp at zero keeps a drifted counter from going negative — a negative
   * `running` satisfies `running < max_running` for ever, disabling the cap.
   */
  private async deleteWithCoordination(
    id: number,
    lockSeq: number,
    queue: Queue
  ): Promise<JobRow | null> {
    const result = await this.pool.query(
      `WITH victim AS (
         SELECT id AS job_id, queue_shard_no AS held_shard, group_id AS grp
         FROM jobs
         WHERE id = $1 AND lease_seq = $2 AND status = 'PROCESSING'
         FOR UPDATE
       ),
       shard_release AS (
         UPDATE queue_shards qs
         SET running = GREATEST(qs.running - 1, 0), updated_at = now()
         FROM victim v
         WHERE v.held_shard IS NOT NULL
           AND qs.queue_id = $3 AND qs.shard_no = v.held_shard
         RETURNING qs.shard_no
       ),
       group_release AS (
         UPDATE group_queue_limits g
         SET running = GREATEST(g.running - 1, 0), updated_at = now()
         FROM victim v
         WHERE v.grp IS NOT NULL
           AND g.queue_id = $3 AND g.group_id = v.grp
           AND (SELECT count(*) FROM shard_release) >= 0
         RETURNING g.group_id
       ),
       removed AS (
         DELETE FROM jobs j
         USING victim v
         WHERE j.id = v.job_id
         RETURNING ${JOB_COLUMNS}
       )
       SELECT * FROM removed`,
      [id, lockSeq, queue.id]
    );

    return result.rows.length > 0 ? (result.rows[0] as JobRow) : null;
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
    // Retry and discard are disjoint on the attempts budget, so both branches
    // ride in one statement and at most one touches the row.
    const result = await this.pool.query(
      `WITH retried AS (
         UPDATE jobs
         SET status = 'PENDING', lease_expires_at = NULL, updated_at = now()
         WHERE id = $1 AND lease_seq = $2 AND status = 'PROCESSING' AND attempts < $3
         RETURNING ${JOB_COLUMNS}
       ),
       removed AS (
         DELETE FROM jobs
         WHERE id = $1 AND lease_seq = $2 AND status = 'PROCESSING' AND attempts >= $3
         RETURNING ${JOB_COLUMNS}
       )
       SELECT ${JOB_COLUMNS}, TRUE AS retried FROM retried
       UNION ALL
       SELECT ${JOB_COLUMNS}, FALSE AS retried FROM removed`,
      [id, lockSeq, queue.maxAttempts]
    );

    if (result.rows.length === 0) {
      throw new NotFoundError(`Job with id ${id} and lock_seq ${lockSeq} not found or not in PROCESSING status`);
    }

    const row = result.rows[0];
    if (row.retried) {
      return this.deserializeJob(row as JobRow);
    }

    return {
      ...this.deserializeJob(row as JobRow),
      status: JobStatus.FAILED,
      completedAt: new Date(),
      lockSeq: null,
      leaseExpiresAt: null,
    };
  }

  /**
   * Full path for failing jobs — slot release and the retry-or-discard branch
   * folded into one statement, one round trip. The slot is released either way:
   * the worker is done with this job.
   *
   * The retry keeps lease_seq (the fence token — the next lease must out-rank
   * a late settle from this worker) and clears queue_shard_no because the slot
   * has been given back. Retry and discard are disjoint on the attempts budget,
   * so at most one branch touches the row. group_release references
   * shard_release to pin shard-then-group order — see deleteWithCoordination.
   * Use when queue.concurrency > 0 OR queue.requiresGroupId === true
   */
  private async failJobWithCoordination(id: number, lockSeq: number, queue: Queue): Promise<Job> {
    const result = await this.pool.query(
      `WITH victim AS (
         SELECT id AS job_id, queue_shard_no AS held_shard, group_id AS grp, attempts AS spent
         FROM jobs
         WHERE id = $1 AND lease_seq = $2 AND status = 'PROCESSING'
         FOR UPDATE
       ),
       shard_release AS (
         UPDATE queue_shards qs
         SET running = GREATEST(qs.running - 1, 0), updated_at = now()
         FROM victim v
         WHERE v.held_shard IS NOT NULL
           AND qs.queue_id = $3 AND qs.shard_no = v.held_shard
         RETURNING qs.shard_no
       ),
       group_release AS (
         UPDATE group_queue_limits g
         SET running = GREATEST(g.running - 1, 0), updated_at = now()
         FROM victim v
         WHERE v.grp IS NOT NULL
           AND g.queue_id = $3 AND g.group_id = v.grp
           AND (SELECT count(*) FROM shard_release) >= 0
         RETURNING g.group_id
       ),
       retried AS (
         UPDATE jobs j
         SET status = 'PENDING', lease_expires_at = NULL, queue_shard_no = NULL, updated_at = now()
         FROM victim v
         WHERE j.id = v.job_id AND v.spent < $4
         RETURNING ${JOB_COLUMNS}
       ),
       removed AS (
         DELETE FROM jobs j
         USING victim v
         WHERE j.id = v.job_id AND v.spent >= $4
         RETURNING ${JOB_COLUMNS}
       )
       SELECT ${JOB_COLUMNS}, TRUE AS retried FROM retried
       UNION ALL
       SELECT ${JOB_COLUMNS}, FALSE AS retried FROM removed`,
      [id, lockSeq, queue.id, queue.maxAttempts]
    );

    if (result.rows.length === 0) {
      throw new NotFoundError(`Job with id ${id} and lock_seq ${lockSeq} not found or not in PROCESSING status`);
    }

    const row = result.rows[0];
    if (row.retried) {
      return this.deserializeJob(row as JobRow);
    }

    return {
      ...this.deserializeJob(row as JobRow),
      status: JobStatus.FAILED,
      completedAt: new Date(),
      lockSeq: null,
      leaseExpiresAt: null,
    };
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

    const row = await this.deleteWithCoordination(id, lockSeq, queue);
    if (!row) {
      throw new NotFoundError(
        `Job with id ${id} and lock_seq ${lockSeq} not found or not in PROCESSING status`
      );
    }

    return this.asDiscarded(this.deserializeJob(row));
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
    // The whole sweep — pick, release slots, discard exhausted, reset the rest —
    // is one statement, so the reaper neither pins a pooled connection nor
    // opens a transaction.
    //
    // - max_attempts lives on the queue, and the reaper spans every queue, so
    //   the budget has to be joined in. Only `jobs` is locked — `queues` is a
    //   read-only lookup, and FOR UPDATE OF v applies to the victims CTE.
    // - Slot release is aggregated per counter key: previously only the first
    //   row of the batch was released, and it was released on every pass.
    // - group_release references shard_release only to pin shard-then-group
    //   order, matching pull's shard → job → group locking (independent
    //   data-modifying CTEs otherwise run in unspecified order). The clamp at
    //   zero keeps a drifted counter from going negative — a negative `running`
    //   satisfies `running < max_running` for ever, which would disable the cap.
    // - A lease expiry counts as a spent attempt — `attempts` was already
    //   incremented when the job was leased — so exhausted jobs are discarded
    //   here rather than reset.
    // - lease_seq is NOT cleared on the retried path. It is the fence token:
    //   keeping it means the next lease is strictly higher, so a worker
    //   returning from the dead is rejected when it tries to settle. Nulling it
    //   here handed the next owner the very same token the zombie still held.
    const result = await this.pool.query(
      `WITH victims AS (
         SELECT j.id AS job_id, j.group_id AS grp, j.queue_id AS qid,
                j.queue_shard_no AS shard_no, j.attempts AS spent, q.max_attempts AS budget
         FROM jobs j
         JOIN queues q ON q.id = j.queue_id
         WHERE j.status = 'PROCESSING' AND j.lease_expires_at <= now()
         ORDER BY j.created_at DESC
         LIMIT $1
         FOR UPDATE OF j SKIP LOCKED
       ),
       shard_release AS (
         UPDATE queue_shards s
         SET running = GREATEST(s.running - d.released, 0), updated_at = now()
         FROM (
           SELECT qid, shard_no, count(*)::int AS released
           FROM victims
           WHERE shard_no IS NOT NULL
           GROUP BY qid, shard_no
         ) d
         WHERE s.queue_id = d.qid AND s.shard_no = d.shard_no
         RETURNING s.shard_no
       ),
       group_release AS (
         UPDATE group_queue_limits g
         SET running = GREATEST(g.running - d.released, 0), updated_at = now()
         FROM (
           SELECT qid, grp, count(*)::int AS released
           FROM victims
           WHERE grp IS NOT NULL
           GROUP BY qid, grp
         ) d
         WHERE g.queue_id = d.qid AND g.group_id = d.grp
           AND (SELECT count(*) FROM shard_release) >= 0
         RETURNING g.group_id
       ),
       removed AS (
         DELETE FROM jobs j
         USING victims v
         WHERE j.id = v.job_id AND v.spent >= v.budget
         RETURNING j.id
       ),
       retried AS (
         UPDATE jobs j
         SET status = 'PENDING',
             lease_expires_at = NULL,
             queue_shard_no = NULL,
             updated_at = now()
         FROM victims v
         WHERE j.id = v.job_id AND v.spent < v.budget
         RETURNING j.id
       )
       SELECT r.id FROM retried r`,
      [limit]
    );

    return result.rows.map((row) => Number(row.id));
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
