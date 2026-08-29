# v1 checklist

First audited 2026-08-21. **Re-audited 2026-08-28** against the working tree — every
line reference below was re-resolved, two new blockers were found and confirmed
empirically against a real Postgres, and several items had drifted out of date.
Changes from the previous revision are marked **[new]** / **[updated]**.

**Done:** reaper works · `lease_seq` fencing correct through recovery · retries wired to
`attempts`/`max_attempts` · shard-0 and remainder-distribution fixed · INT8 parsed as number ·
**§3 transactional publish** · **public `TaskQueue` facade + narrowed exports** · 4 of the §1
worker-loop bugs · README with quickstart and guarantees · **195 integration tests on real
Postgres**, of which 135 drive the public API: `client` 22, `producer` 35, `consumer` 41,
`transaction` 37 · nine mutation checks confirm the headline guarantees fail when broken.

**Suite state (verified 2026-08-28, full run 37s):** `Tests 1 failed | 194 passed (195)`.
The single failure is the deliberate group-fairness marker at
`concurrency.property.test.ts:237`, deferred to v2 (§6). Nothing else is red.

Estimate to v1: **8–11 focused days** remaining. The two new blockers add roughly half a
day between them — the idempotency one folds into the §2 migration that was already planned.

---

## 1. Correctness — blockers (4–5 d)

- [ ] **[new] A publish can silently vanish: idempotency keys are global, but the
      read-back is not.** `idempotency_key` is `UNIQUE` across the whole table
      (`000001:23`). `insertJobs` uses `ON CONFLICT (idempotency_key) DO NOTHING`
      (`job.repository.ts:146`) and then reads the conflicting row back with
      `WHERE idempotency_key = ANY($1::text[])` (`:183`) — **no `queue_id` filter**. So
      publishing key `k` to queue B while `k` is live on queue A inserts nothing, and hands
      the caller queue A's job with `deduplicated: true`. Verified against Postgres: two
      queues, one shared key, `second.queueId === a.id`, `second.payload === '{"q":"a"}'`,
      and exactly one row in `jobs`. The caller sees a successful publish and a plausible
      dedup flag; the job was never enqueued. Bites the natural pattern of reusing a
      business id (`order-123`) as the key on more than one queue. Fix is the per-queue
      uniqueness in §2 plus a `queue_id` predicate on the read-back.
- [ ] **[new] The reaper reclaims newest-first, so the oldest stranded jobs starve.**
      `recoverJobs` orders `ORDER BY j.created_at DESC` (`job.repository.ts:695`) under
      `LIMIT $1` (default 100). Verified: five expired jobs, `recoverJobs(2)` returned
      `[4, 5]` — the two newest — leaving the three oldest in PROCESSING. Whenever the
      expiry rate exceeds `batchSize` per pass, the jobs that have been stuck longest are
      the ones never recovered, which is the exact inverse of what crash recovery is for.
      One-word fix (`ASC`), but it needs a test.
- [ ] **Group published without `concurrency` is permanently unpullable.** The repository
      still skips the `group_queue_limits` insert when `concurrency` is falsy
      (`job.repository.ts:153`); the pull's `UPDATE … WHERE running < max_running RETURNING`
      (`:318`) then matches 0 rows and rolls back for ever. *Now unreachable through the
      public API* — `PublishOptions.group.concurrency` is a required field — but the
      repository path is still open and untested.
- [x] **Success recorded as failure.** ~~`worker.service.ts:61-64`~~ — completion now settles
      outside the handler's `try` (`worker.service.ts:186-195`); a DB error there leaves the
      lease to expire instead of re-running a job that already succeeded. Covered by
      `client.test.ts`.
- [ ] **No lease renewal.** A handler outliving `leaseDuration` is reclaimed and re-delivered
      while still running: job executes twice *and* the cap is exceeded. Add heartbeat/extend,
      or document the contract and enforce it. Still nothing in `WorkerService` touches
      `lease_expires_at` after the pull.
- [x] **`JSON.parse(payload)` inside handler try.** ~~`worker.service.ts:62`~~ — deserialization
      happens before the handler `try` (`worker.service.ts:161-168`); an undecodable payload is
      discarded as poison via `JobRepository.discardJob` (`:609`) and reported as
      `onError({ phase: 'deserialize' })`.
      **Consequence to revisit:** discard deletes the job with no trace, since the `job_status`
      archive in §2 does not exist yet.
- [ ] **`requiresGroupId` never enforced in the repository.** A job with no `group_id` still
      publishes fine to such a queue and then takes the coordination path with no coordination.
      `QueueHandle.publish` rejects it with `BadParamInputError` (`queue-handle.ts:114`), so the
      public API is covered, but `insertJobs` has no check.
- [x] **`stop()` can hang for ever.** ~~`worker.service.ts:41-42`~~ — `stop({ timeout })` bounds
      the drain and reports `{ drained }` (`worker.service.ts:92-134`); the poll sleep is
      interruptible via an `AbortController`, also surfaced to handlers as `ctx.signal`.
- [x] **`start()` race.** ~~`worker.service.ts:26-37`~~ — the in-flight start promise is stored
      before the first `await` and returned on re-entry (`worker.service.ts:55-67`).
- [ ] **Counter reconciler.** Nothing recomputes `queue_shards.running` /
      `group_queue_limits.running` from `jobs`, so any leak is permanent. ~10 lines in the
      reaper, next to the release statements at `job.repository.ts:718-746`.
- [ ] **[updated] Double `ROLLBACK`** on settle-after-lease-loss — line references moved:
      `job.repository.ts:432→456` (complete) and `:541→578` (fail). The not-found branch rolls
      back and throws; the catch-all rolls back again, so the common path logs
      `no transaction in progress` every time. `discardJob` already models the fix — its
      catch-all guards with `.catch()` (`:652`).
- [ ] `ON CONFLICT DO NOTHING` on the `group_queue_limits` insert (`job.repository.ts:165`)
      silently ignores a later, different group concurrency — first publisher's cap wins for ever.

## 2. Schema — one migration, decide together (2–3 d)

Hardest thing to change after release. Add the columns even if nothing reads them yet.

**[updated] Numbering:** `000002_split_job_tables` now exists but is *only* an index
change (broad indexes → partial ones scoped by status). Despite the filename it does not
split anything. The schema migration below is `000003`.

- [ ] `run_at` / `scheduled_at` — retry backoff (today a poison job hot-loops at full speed)
      **and** delayed jobs, for free.
- [ ] `priority` — change pull ordering to `(queue_id, priority, id)`; `created_at` ties are
      non-deterministic for a batch published in one transaction. Both pull paths currently
      order on bare `created_at` (`job.repository.ts:258, :302`).
- [ ] `job_status` archive + `failed_reason` — terminal jobs are `DELETE`d
      (`:393, :440, :503, :567, :612, :647, :752`), so nothing survives. `failJob` has no
      reason parameter anywhere in the chain. `CLAUDE.md:71,75,113` still documents this table;
      no migration creates it.
- [ ] **[updated] Per-queue idempotency** — `idempotency_key` is globally `UNIQUE`
      (`000001:23`) and silently freed when a terminal job is deleted. The old note here
      ("raises a raw pg `23505`") is **stale**: `ON CONFLICT DO NOTHING` swallows it. The live
      consequence is worse and is now tracked as the first §1 blocker — a cross-queue
      collision drops the publish and returns another queue's job. Make the key unique per
      `(queue_id, idempotency_key)`.
- [ ] Reserve the fairness columns now (see §5) even if unimplemented.
- [ ] `completed_at` (`000001:33`) is never written or read — `JOB_COLUMNS` omits it and
      `deserializeJob` hardcodes `completedAt: null` (`job.repository.ts:840`). Write it or
      drop it.

## 3. Differentiator — DONE

- [x] **Transactional publish.** `publishJob(input, executor?)` and `publishJobs(inputs, executor?)`
      take an optional structural `Executor`; with one supplied they emit no BEGIN/COMMIT and join
      the caller's transaction. `TaskQueue.transaction(fn)` is the convenience wrapper.
      Duplicate keys go through `ON CONFLICT DO NOTHING` + read-back so a dedup cannot abort the
      caller's transaction. Covered by commit/rollback/isolation/dedup tests in `client.test.ts`
      and the 37 cases in `transaction.test.ts`.

## 4. Packaging — cannot ship without (2 d)

- [ ] **[updated]** `npm pack` now ships **169 files / 245.1 kB** (was 121 / 152 kB — it has
      grown, not shrunk): `src/`, `tests/`, `postman_collection.json`, `clean-arch.png`, Go
      files, `queue.sql`, the stray `*_SUMMARY.md` docs. Add a `files` field.
- [ ] No `prepublishOnly`/`prepare`, and `dist/` is gitignored → publishing yields a package
      whose `main` (`dist/index.js`) does not exist.
- [ ] `version: 1.0.0` → drop to `0.1.0`; the schema is not settled.
- [ ] `LICENSE:3` — copyright is `Iman Tumorang`. `FUNDING.yml` → `bxcodec`.
      `dependabot.yml` → `gomod`, so no dependency updates ever run.
- [ ] Only CI workflow is `gotest.yml` (Go 1.19, `make lint`, no root Makefile) — always red.
      Replace with one that runs vitest.
- [x] ~~`index.ts:7-8` exports `JobRepository`/`QueueRepository`~~ — `src/index.ts` now exports
      only the `TaskQueue` facade and domain types. Repositories/services are deep-import
      internals; `NUMBER_OF_SHARD`, `QueueShards`, `GroupQueueLimits` and `pullJobs` no longer
      leak. **Note: `migrate()` resolves `__dirname/../../migrations` (`task-queue.ts:77`), so
      `migrations/` must be in the `files` field before publishing.**
- [x] Injectable logger — `Logger` interface in `src/domain/logger.ts`, threaded through both
      repositories, `WorkerService`, `ReaperService` and `TaskQueue`. The saturated-queue warnings
      go through it, and the reaper no longer logs a heartbeat when it recovers nothing.
      Remaining `console.*` calls live in `src/migration/runner.ts` only.
- [x] ~~`publishJob` takes `queueId: number`~~ — `defineQueue(name)` / `tq.publish(name, …)`
      address queues by name and resolve the id once, via `QueueRepository.ensureQueue`.
- [x] ~~Delete dead `src/domain/group.ts`~~ — removed. ~~`example:producer` missing~~ —
      `examples/producer.ts` written, plus `examples/transactional-publish.ts`;
      `examples/worker.ts` rewritten on the facade. `examples/` is now in the typecheck project,
      so a missing or broken example fails `npm run typecheck`.
- [x] README: install, quickstart, transactional publish, a written guarantees/semantics
      section, a config reference, and an explicit "not yet implemented" list.

## 1b. Correctness — found by the facade test suites

Surfaced while writing `producer`/`consumer`/`transaction`.test.ts. Each is pinned by a passing
test that documents current behaviour, so fixing one will flag the test that describes it.
**Re-verified 2026-08-28: all still open, none regressed.**

- [ ] **A `group` on a fast-path queue is silently unenforced.** `insertJobs` writes `group_id`
      and the `group_queue_limits` row whatever the queue config, so the cap looks live in the
      database — but `isFastPath` is `concurrency === 0 && !requiresGroupId`
      (`job.repository.ts:42-44`), and `pullJobFast` never reads `group_queue_limits`.
      `running` stays 0 for ever and the tenant gets unlimited concurrency. README:575 shows
      exactly this call with no mention that the queue must be group-aware. Reject the option,
      or make the pull path depend on the job's `group_id`.
- [ ] **An empty-string `group.id` fails open.** `toInput` guards with `!options.group` — an object
      check (`queue-handle.ts:114`) — so `{ id: '', concurrency: 5 }` passes; `input.group?.id || null`
      (`job.repository.ts:133`) then stores NULL and the limit-row loop is skipped. At pull time
      `if (job.groupId && ...)` is false, so the job is leased with no cap at all. An unset env var
      produces exactly this. Note this is the *inverse* of the tracked `concurrency: 0` case, which
      fails closed — only the safe half was tested.
- [ ] **A NUL in `metadata` aborts the caller's transaction.** `jobs.metadata` is JSONB bound as
      `$N::jsonb` (`job.repository.ts:127`); `JSON.stringify` escapes NUL as the six-character
      sequence `\u0000`, which jsonb rejects (22P05). Under `{ tx }` that is a real error on the caller's
      connection — the exact failure the ON CONFLICT design exists to avoid, and which
      README:340-341 sells as a guarantee. `payload` is TEXT and accepts the same string happily.
      Validate in `toInput` and raise `BadParamInputError`.
- [ ] **First-use publish inside a transaction needs a second connection.** `QueueHandle.publish`
      awaits `resolve()` before looking at `options.tx` (`queue-handle.ts:99-100`), and
      `ensureQueue` runs on the repository's own pool — so the first publish onto a queue, made
      inside `tq.transaction()`, checks out a second connection while the caller's transaction
      holds the first. On a saturated pool nothing can release it. Bites every request on a
      `max: 1` serverless/pgbouncer deployment. Constraint: `createQueue` issues
      `SET TRANSACTION ISOLATION LEVEL` (`queue.repository.ts:157`), which Postgres rejects (25001)
      inside a caller's transaction — so look the queue up over `tx` and fall back to the pool only
      to create, pre-resolve inside `TaskQueue.transaction()`, or fail fast with a typed error.
- [ ] **A grouped job with no `group_queue_limits` row stalls the entire queue.** The coordination
      pull selects the single oldest PENDING row and `ROLLBACK`s the whole transaction if that
      row's group cannot take a slot (`job.repository.ts:316-328`) — so an undeliverable grouped
      job at the head makes every later pull return null, for ever, with only a `logger.warn` per
      poll. Same mechanism as the deferred group-fairness issue, but permanent rather than
      temporary. Scan to the next candidate instead of aborting the pull.
- [ ] **A negative `group.concurrency` is stored and strands the job.** `-1` is truthy, and
      `group_queue_limits.max_running` has no CHECK constraint (`000001:44`), so
      `running < max_running` can never hold. Same failure as `concurrency: 0` but with a limit row
      present, which hides it. Values of `2.5` and `2**31` escape as raw driver errors
      (22P02 / 22003) rather than `BadParamInputError`.
- [ ] **`stop()` reports `drained: true` after an earlier timed-out stop.** A stop that hits its
      deadline still clears `slots` and `startPromise` (`worker.service.ts:125-126`) before
      returning `drained: false`, so the next `stop()` takes the early return at `:94` and reports
      a clean drain for a worker that is demonstrably not drained. `TaskQueue.close()` then closes
      the pool under a live handler with only a warning (`task-queue.ts:223-235`). Keep the slot
      promises when the drain times out.
- [ ] **`publishMany` has an undocumented ceiling near 9362 payloads.** 7 bind parameters per row
      (`job.repository.ts:124`) against the wire protocol's 65535 limit, with no chunking; past it
      the driver reports something like "bind message has 5 parameter formats but 0 parameters",
      naming neither the batch size nor the limit. Chunk internally on the same executor, or reject
      with a typed error.
- [ ] Smaller: a circular or BigInt payload escapes as a raw `TypeError` rather than
      `BadParamInputError` (`jsonSerializer` only translates the *returns*-undefined case,
      `serializer.ts:14-20`);
      `work({ concurrency: 0 })` builds zero slots while `isRunning()` reports true
      (`worker.service.ts:38, :76`);
      a nested `tq.transaction()` silently opens an independent transaction on a second connection
      (`task-queue.ts:166`);
      `publishMany` cannot express per-payload groups though `JobRepository.publishJobs` supports it
      (`queue-handle.ts:104-111`);
      `recoverJobs` never reports the jobs it discards (`job.repository.ts:750-777`), so `[]` cannot
      be told from "a job was destroyed";
      **[new]** a second `defineQueue(name, { serializer })` silently keeps the first handle's
      serializer — `assertSameConfig` compares only `ResolvedQueueConfig`, which has no
      `serializer` field (`task-queue.ts:112-142`), so the mismatch is neither applied nor reported;
      **[new]** the `isExpired` branch in `pullJobWithCoordination` is dead — the SELECT filters
      `status = 'PENDING'` (`job.repository.ts:301`), so `job.status === PROCESSING` (`:312`) is
      never true and both `!isExpired` guards are constants.

## 5. Test gaps (1–2 d)

- [ ] **[updated]** `concurrency.property.test.ts:156-178` — a lower bound *was* added, but it is
      on the wrong quantity: `pulledIds.length > 0` (`:173-176`) only catches "admitted nothing at
      all". `tracker.peak` still has no lower bound (`:177` asserts `<= 8` only), so a regression
      serialising the queue to one job at a time still passes.
- [ ] No coverage for: retry backoff.
      ~~transactional publish~~ — `tests/transaction.test.ts`, 37 tests.
      ~~group-without-concurrency~~ — pinned in `tests/producer.test.ts`.
      **[new]** no coverage for the two blockers found on 2026-08-28: cross-queue idempotency
      collision, and reaper batch ordering under `limit < expired`.
- [ ] **[updated]** `crash-process.ts:61-64` — `JSON.parse` on a possibly-partial stdout line
      (the accumulated buffer is re-split every chunk with no newline buffering) → flaky under
      chunked pipe delivery.
- [ ] `invariants.ts:125-127` — `ProcessingSampler` swallows every query error; under pool
      saturation it can drop exactly the samples covering the peak.
- [ ] **[new]** `TEST_REPORT.md` is stale: it still reports "56 integration tests … 1 failed |
      55 passed (56)" and lists only three suites. The real numbers are 195 / 194 passed across
      seven suites. Either regenerate it or drop it — it is currently the most wrong document in
      the repo.

## 6. Deferred to v2 — explicitly not v1

Group fairness (the deliberate red test at `concurrency.property.test.ts:237`) ·
rate limiting · `LISTEN/NOTIFY` · flows/dependencies · cron schedulers ·
queue mutation API (`concurrency`/`lease_duration` are immutable after creation).

---

### Suggested order

1. ~~§3 transactional publish~~ — done
2. §2 schema migration, now `000003` (2–3 d — everything else builds on it; per-queue
   idempotency closes the first §1 blocker, and `job_status` gives the poison-discard path
   somewhere to record what it threw away)
3. §1 remaining correctness (3–4 d — reaper ordering first, it is one word; then lease renewal,
   counter reconciler, repository-level group/`requiresGroupId` validation, double `ROLLBACK`;
   retry backoff lands here once `run_at` exists)
4. §5 test gaps alongside §1
5. §4 remaining packaging (1–2 d — `files`, `prepublishOnly`, version, LICENSE, CI)

> Not part of the shipped package — add to `files`/`.npmignore` or delete before publishing.
