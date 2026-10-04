import { Pool } from 'pg';
import { Job, JobStatus, CreateJobInput, Metadata, PublishedJob } from '../../domain/job';
import { Queue } from '../../domain/queue';
import { ConflictError, NotFoundError } from '../../domain/errors';
import { Logger, consoleLogger } from '../../domain/logger';
import { Executor } from '../../domain/executor';

// Every read of an active job returns the same projection.
const JOB_COLUMNS = `id, idempotency_key, payload, status, group_id, queue_id, queue_shard_no,
         attempts, metadata, created_at, updated_at, lease_seq, lease_expires_at`;


// Database row interface for the active jobs table (snake_case)
// Note: completed_at is not stored — completed/failed jobs are deleted
interface JobRow {
  // The BIGINT columns are typed as `number | string`: they arrive as strings
  // from any `pg` copy without this package's INT8 parser. See `toNumber`.
  id: number | string;
  idempotency_key: string;
  payload: string;
  status: string;
  group_id: string | null;
  queue_id: number | string;
  attempts: number;
  metadata: any;
  queue_shard_no: number | string | null;
  created_at: Date;
  updated_at: Date;
  lease_seq: number | string;
  lease_expires_at: Date | null;
}


/**
 * Coerces a BIGINT column to a JavaScript number.
 *
 * `connection.ts` registers an INT8 parser, but only on *this* package's copy
 * of `pg`. A publish handed an `Executor` runs on the caller's client, which
 * may come from a different copy with no parser registered — there those
 * columns arrive as strings, and `lease_seq + 1` in the fencing path becomes
 * string concatenation rather than arithmetic. Every id, queue id, shard number
 * and lease sequence therefore passes through here regardless of who ran the
 * statement.
 *
 * Every 64-bit column in this schema stays far below 2^53, so the conversion
 * is lossless.
 */
/**
 * Identity of a job for dedup purposes, matching the unique index from
 * migration 000004. NUL separates the parts so no queue id / key pair can
 * collide with another by concatenation.
 */
function jobKey(queueId: number, idempotencyKey: string): string {
  return `${queueId}\u0000${idempotencyKey}`;
}

/** Same construction for the `(queue_id, group_id)` primary key. */
function groupKey(queueId: number, groupId: string): string {
  return `${queueId}\u0000${groupId}`;
}

function toNumber(value: number | string | null): number | null {
  if (value === null || value === undefined) {
    return null;
  }
  return typeof value === 'number' ? value : Number(value);
}

export class JobRepository {
  constructor(
    private pool: Pool,
    private logger: Logger = consoleLogger
  ) {}

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
   * Publishes one job. See {@link publishJobs} — this is that path with a batch
   * of one, so the transactional and deduplication semantics are identical by
   * construction rather than by two implementations agreeing.
   */
  async publishJob(input: CreateJobInput, executor: Executor = this.pool): Promise<PublishedJob> {
    const [job] = await this.publishJobs([input], executor);
    return job;
  }

  /**
   * Publishes jobs, deduplicating on `(queueId, idempotencyKey)`.
   *
   * ## Transactions
   *
   * `executor` — a `PoolClient` mid-transaction, or any handle that can run
   * `query(text, values)` — enrols the publish in the caller's transaction, so
   * the jobs commit with the business writes that justify them or not at all.
   * That is what lets a caller skip an outbox table: these rows *are* the outbox
   * rows, and the worker is what polls them. It only holds while the queue lives
   * in the same database as those writes; across databases an outbox (or 2PC) is
   * still the only atomic story. Omitted, the publish runs on the pool and
   * commits on its own. Either way this method never opens, commits or rolls
   * back a transaction of its own.
   *
   * ## Deduplication
   *
   * A key already held by a live job is not an error: that job comes back with
   * `deduplicated: true`, so exactly one row — and therefore exactly one
   * delivery — exists for it. Raising instead would be unusable inside a
   * caller's transaction, where an unhandled constraint violation aborts the
   * whole transaction and takes their business writes down with the publish.
   *
   * Insert and conflict are resolved by one statement, so there is no window in
   * which the job holding a key can settle and disappear before the publish
   * learns its identity. A duplicate does write the row it resolves to — that
   * is what makes the row come back — but it writes only `revision`, leaving
   * `status`, `attempts` and the lease columns alone, so a job a worker is
   * holding is unaffected by it.
   *
   * The cost is a row lock on that job, held until the caller commits:
   * `pullJob` and the reaper both use SKIP LOCKED and step around it, but that
   * one job's `completeJob`/`failJob` waits. One more reason to keep the
   * transaction short.
   *
   * Uniqueness is per queue, so the same business id can back a job on the email
   * queue and another on the invoicing queue. It covers only *live* jobs:
   * settled jobs are deleted, which frees the key.
   *
   * Within a single batch, the **first occurrence of a key wins** — it reports
   * `deduplicated: false` and every later occurrence reports `true` against the
   * same row. Repeats are collapsed before the statement runs, since ON CONFLICT
   * DO UPDATE refuses to touch one row twice in a single command.
   *
   * ## Shape
   *
   * The insert travels as one statement, so the whole batch lands or none of it
   * does, in a single round trip. The rows go as parallel arrays through
   * `unnest` rather than a generated `VALUES` list, which keeps the parameter
   * count fixed no matter how many jobs are in the batch — nothing has to be
   * chunked to stay under PostgreSQL's 65535-parameter ceiling, and chunking is
   * what would quietly cost the all-or-nothing guarantee.
   *
   * No cap is enforced on batch size. Keep batches in the low thousands anyway —
   * what grows is the transaction (lock duration, WAL, how long one connection
   * is held), not the round-trip count.
   *
   * Every job in a batch is stamped with the same `created_at`: `now()` is the
   * transaction's start time, not the row's. Pulls order by `created_at`, so
   * jobs within one batch come out in no particular order relative to each
   * other. Order across batches is unaffected.
   *
   * @throws ConflictError if the statement returns no row for an input. There is
   * no known way to reach this — DO UPDATE returns on both the insert and the
   * conflict path — so it guards an invariant rather than a race.
   */
  async publishJobs(
    inputs: CreateJobInput[],
    executor: Executor = this.pool
  ): Promise<PublishedJob[]> {
    if (inputs.length === 0) {
      return [];
    }

    // Collapse repeats before the statement, for two reasons. ON CONFLICT DO
    // UPDATE refuses to touch the same row twice in one command (SQLSTATE
    // 21000), so a repeated key would fail the whole publish. And the choice of
    // which occurrence is the real one belongs in code that can state it —
    // first in, first served — rather than in whatever the database happens to
    // do with the rest.
    const firstByKey = new Map<string, CreateJobInput>();
    for (const input of inputs) {
      const key = jobKey(input.queueId, input.idempotencyKey);
      if (!firstByKey.has(key)) {
        firstByKey.set(key, input);
      }
    }

    // Sorted so every publisher takes overlapping job row locks in the same
    // order. Two batches sharing a pair of keys in opposite orders would
    // otherwise be able to deadlock against each other.
    const unique = [...firstByKey.values()].sort(
      (a, b) =>
        a.queueId - b.queueId ||
        (a.idempotencyKey < b.idempotencyKey ? -1 : a.idempotencyKey > b.idempotencyKey ? 1 : 0)
    );

    const idempotencyKeys: string[] = [];
    const payloads: string[] = [];
    const statuses: string[] = [];
    const groupIds: (string | null)[] = [];
    const queueIds: number[] = [];
    const attempts: number[] = [];
    const metadatas: string[] = [];

    // Keyed on queue + group: the same group name under two queues is two
    // independent caps, and `group_queue_limits` is keyed that way too.
    const groupLimits = new Map<string, { groupId: string; queueId: number; concurrency: number }>();

    // Every column is pushed once per iteration, in one loop body with no
    // branch around any push. `unnest` pads a short array with NULLs rather
    // than raising, and `group_id` is the one nullable column here — a
    // `groupIds` array that fell behind would silently produce ungrouped jobs
    // that escape their concurrency cap. The loop shape is what rules that out.
    for (const input of unique) {
      idempotencyKeys.push(input.idempotencyKey);
      payloads.push(input.payload);
      statuses.push(JobStatus.PENDING);
      groupIds.push(input.group?.id || null);
      queueIds.push(input.queueId);
      attempts.push(input.attempts || 0);
      metadatas.push(JSON.stringify(input.metadata || {}));

      if (input.group?.id && input.group?.concurrency) {
        const key = groupKey(input.queueId, input.group.id);
        // First declaration wins. A later, differing concurrency for the same
        // group would be dropped by ON CONFLICT DO NOTHING regardless — the cap
        // belongs to whoever seeded the row first, batch or not.
        if (!groupLimits.has(key)) {
          groupLimits.set(key, {
            groupId: input.group.id,
            queueId: input.queueId,
            concurrency: input.group.concurrency,
          });
        }
      }
    }

    const values: any[] = [
      idempotencyKeys,
      payloads,
      statuses,
      groupIds,
      queueIds,
      attempts,
      metadatas,
    ];

    let limitsCte = '';
    if (groupLimits.size > 0) {
      // Sorted for the same reason the job rows are.
      const limits = [...groupLimits.values()].sort(
        (a, b) =>
          a.queueId - b.queueId || (a.groupId < b.groupId ? -1 : a.groupId > b.groupId ? 1 : 0)
      );

      // The `ins` reference does nothing to the rows selected; it exists to pin
      // this CTE after the job insert. Independent data-modifying CTEs run in
      // unspecified order, and `ins` now takes row locks — seeding limits first
      // would give publish a group → job lock order against settle's and pull's
      // job → group, which is a cycle. Same device as `recoverJobs` uses to
      // pin shard-then-group.
      limitsCte = `,
       limits AS (
         INSERT INTO group_queue_limits (group_id, queue_id, max_running, running, updated_at, created_at)
         SELECT g, q, m, 0, now(), now()
         FROM unnest($8::text[], $9::bigint[], $10::int[]) AS t(g, q, m)
         WHERE (SELECT count(*) FROM ins) >= 0
         ON CONFLICT DO NOTHING
       )`;
      values.push(
        limits.map((l) => l.groupId),
        limits.map((l) => l.queueId),
        limits.map((l) => l.concurrency)
      );
    }

    // `DO UPDATE` rather than `DO NOTHING`: RETURNING only reports rows the
    // statement wrote, so a conflict that writes nothing comes back as no row at
    // all and the publish would have to ask a second time who holds the key.
    // That second statement is a race — the job can settle and be deleted in the
    // gap — and the fix is not to retry it but to remove it. A conflict that
    // writes gives its row back in the same statement, and PostgreSQL re-drives
    // the insert internally if the conflicting row disappears mid-flight.
    //
    // `revision` is what separates the two outcomes: the column defaults to 0,
    // so a row that comes back above 0 arrived through the conflict path. The
    // system column `xmax` would report the same thing without a schema change,
    // but only inside the RETURNING of the statement that wrote the row — read
    // anywhere else it silently means something different. `revision` is real
    // data and reads correctly everywhere.
    //
    // Nothing else is assigned. The existing job wins a duplicate publish with
    // its `status`, `attempts`, `lease_seq` and `lease_expires_at` intact —
    // clobbering any of those would break a job a worker is holding right now.
    const result = await executor.query(
      `WITH ins AS (
         INSERT INTO jobs (idempotency_key, payload, status, group_id, queue_id, attempts, metadata)
         SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::bigint[],
                              $6::int[], $7::jsonb[])
         ON CONFLICT (queue_id, idempotency_key)
           DO UPDATE SET revision = jobs.revision + 1
         RETURNING ${JOB_COLUMNS}, revision
       )${limitsCte}
       SELECT * FROM ins`,
      values
    );

    // RETURNING makes no promise about row order, so the batch is realigned on
    // the key that identifies a job rather than on position. Every input has a
    // row here — DO UPDATE returns on both paths — so a miss below is a broken
    // invariant, not a race.
    const resolved = new Map<string, PublishedJob>();
    for (const row of result.rows) {
      const job = this.deserializeJob(row as JobRow);
      resolved.set(jobKey(job.queueId, job.idempotencyKey), {
        ...job,
        deduplicated: Number(row.revision) > 0,
      });
    }

    const emitted = new Set<string>();
    return inputs.map((input) => {
      const key = jobKey(input.queueId, input.idempotencyKey);
      const job = resolved.get(key);

      if (!job) {
        throw new ConflictError(
          `Could not publish "${input.idempotencyKey}" on queue ${input.queueId}: ` +
            'the statement returned no row for it'
        );
      }

      // The first occurrence of a key in the batch owns the insert; later ones
      // describe the same row and are duplicates of it.
      if (emitted.has(key)) {
        return { ...job, deduplicated: true };
      }
      emitted.add(key);
      return { ...job };
    });
  }



  /** Live backlog for a queue. Terminal jobs are deleted, so this is all of it. */
  async countByStatus(queueId: number): Promise<{ pending: number; processing: number }> {
    const result = await this.pool.query(
      `SELECT status, count(*)::int AS n FROM jobs WHERE queue_id = $1 GROUP BY status`,
      [queueId]
    );

    const counts = { pending: 0, processing: 0 };
    for (const row of result.rows) {
      if (row.status === JobStatus.PENDING) {
        counts.pending = row.n;
      } else if (row.status === JobStatus.PROCESSING) {
        counts.processing = row.n;
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
           lease_seq = lease_seq + 1,
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
             lease_seq = j.lease_seq + 1,
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
    if ((queue.concurrency === 0 || queue.concurrency === null) && !queue.requiresGroupId) {
      return this.pullJobFast(queue);
    }
    return this.pullJobWithCoordination(queue);
  }

  // ---------------------------------------------------------------------------
  // Complete job
  // ---------------------------------------------- -----------------------------

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

    // The row is gone, so what comes back is a record of the job as it settled,
    // not a live handle. It keeps the token it was settled with rather than
    // blanking it — there is no lease left to fence, and the caller asked to
    // settle exactly this one.
    const completedAt = new Date();
    return {
      ...this.deserializeJob(result.rows[0] as JobRow),
      status: JobStatus.COMPLETED,
      completedAt,
      leaseExpiresAt: null,
    };
  }

  /**
   * Full path for completing jobs — includes shard / group-limit coordination.
   * Delete and slot release travel as one statement (one round trip), atomic by
   * virtue of being a single statement rather than a transaction.
   * Use when queue.concurrency > 0 OR queue.requiresGroupId === true
   */
  private async completeJobWithCoordination(id: number, lockSeq: number, queue: Queue): Promise<void> {
    const row = await this.deleteWithCoordination(id, lockSeq, queue);
    if (!row) {
      throw new NotFoundError(`Job with id ${id} and lock_seq ${lockSeq} not found or not in PROCESSING status`);
    }
  }

  /**
   * Deletes a settled job and gives back its shard / group slots in one
   * statement.
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
  async completeJob(id: number, lockSeq: number, queue: Queue): Promise<void> {
    if ((queue.concurrency === 0 || queue.concurrency === null) && !queue.requiresGroupId) {
      await this.completeJobFast(id, lockSeq);
      return
    }
    await this.completeJobWithCoordination(id, lockSeq, queue);
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
      leaseExpiresAt: null,
    };
  }

  /**
   * Public API: marks a job as failed
   * Automatically selects fast or full path based on queue configuration
   */
  async failJob(id: number, lockSeq: number, queue: Queue): Promise<Job> {
    if ((queue.concurrency === 0 || queue.concurrency === null) && !queue.requiresGroupId) {
      return this.failJobFast(id, lockSeq, queue);
    }
    return this.failJobWithCoordination(id, lockSeq, queue);
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

    return {
      id: toNumber(row.id)!,
      idempotencyKey: row.idempotency_key,
      payload: row.payload,
      queueShardNo: toNumber(row.queue_shard_no),
      status: row.status as JobStatus,
      groupId: row.group_id,
      queueId: toNumber(row.queue_id)!,
      attempts: row.attempts,
      metadata,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
      completedAt: null,              // active jobs are never completed
      leaseExpiresAt: row.lease_expires_at,
      lockSeq: toNumber(row.lease_seq)!,
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
