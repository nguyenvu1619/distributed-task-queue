# Test suite — status

56 integration tests against a real PostgreSQL 16 (testcontainers). Full run ≈ 16s.

```
Tests  1 failed | 55 passed (56)
```

The one failure is **deliberate** — see [Known open](#known-open) below. Setup and
design notes: [`tests/README.md`](tests/README.md).

| Suite | Passed | Failed |
| --- | --: | --: |
| `tests/lifecycle.test.ts` | 31 | 0 |
| `tests/concurrency.property.test.ts` | 8 | 1 |
| `tests/crash-fencing.test.ts` | 16 | 0 |

The first run of this suite found **8 defects**. All are fixed; the sections
below record what was wrong and what changed, so the tests read as a ledger
rather than a wall of green.

---

## Fixed

### F0 — the reaper never ran at all

`recoverJobs()` put the locking clause before `ORDER BY`, which is not valid
PostgreSQL: every call threw `42601 syntax error at or near "ORDER"`. Lease
recovery had never worked in this codebase.

```diff
- ... AND lease_expires_at <= now() FOR UPDATE SKIP LOCKED ORDER BY created_at DESC LIMIT 100
+ ... AND lease_expires_at <= now() ORDER BY created_at DESC LIMIT 100 FOR UPDATE SKIP LOCKED
```

### F1 — shard 0 was never counted, so the queue cap was unenforced

`if (!isExpired && shareNo)` treats shard number `0` as falsy, so jobs landing on
shard 0 never incremented `queue_shards.running` — and the matching
`if (job.queueShardNo)` skipped the decrement. Shard 0 admitted an unbounded
number of jobs while reporting `running = 0`.

**A queue configured for 64 concurrent jobs was observed running 85.** Now
`!= null` in all three places. The property test asserts the cap from both
sides — never exceeded, and actually reached, so it cannot pass vacuously.

### F2 — `Math.floor(concurrency / 32)` discarded capacity

`concurrency: 100` provisioned 96 slots; **any `concurrency < 32` gave every
shard zero slots**, so `pullJob` returned `null` for ever and the queue was
completely dead behind a `console.warn`. The remainder is now spread over the
first `concurrency % NUMBER_OF_SHARD` shards, so the shards admit exactly the
configured number.

### F3 — the reaper destroyed the fence token

Recovery reset `lease_seq` to `NULL`, so the next pull recomputed
`COALESCE(lease_seq, 0) + 1` and re-issued the **same** token the dead worker
still held (`[1,1,1,1]` instead of `[1,2,3,4]`).

This was the highest-severity finding: a worker that was SIGKILLed, paused, or
GC-stalled past its lease could come back and settle a job someone else now
owned — silently discarding the new owner's work. `lease_seq` is now left
untouched during recovery, so every re-lease strictly out-ranks the one it
replaced. Verified end to end with a real process kill.

### F4 — coordinated jobs were never recovered

If any row in the batch had a `group_id` or `queue_shard_no`, the reaper
decremented counters for `rows[0]` only, committed, and returned `[]` — **no job
was ever set back to `PENDING`**, including fast-path rows that shared the batch.
Each subsequent tick decremented the same counters again, driving `running`
negative; once negative, `running < max_running` holds for ever and admission
control is gone entirely.

Now: slots are released per row (aggregated into one statement per counter type),
counters are clamped with `GREATEST(..., 0)`, and every reclaimed job is reset to
`PENDING`.

### F5 — 64-bit columns arrived as strings

No `setTypeParser` was registered, so `Queue.id`, `Job.id` and `Job.lockSeq` were
strings despite being typed `number`. `(job.lockSeq || 0) + 1` was therefore
string concatenation — a re-leased job would have gone `"1"` → `"11"` → `"111"`,
overflowing `bigint` after ~19 re-leases. It was only invisible because F4 stopped
coordinated jobs from ever being re-leased. Fixed in `connection.ts`:

```ts
types.setTypeParser(types.builtins.INT8, (value) => parseInt(value, 10));
```

### F6 — the coordination path returned a stale `Job`

The returned object spread the row read *before* the `UPDATE`, so it reported
`leaseExpiresAt: null` even though a lease had just been written — the caller
could not tell how long it held the job. The `UPDATE` now uses `RETURNING`.

### F7 — sub-second leases expired at the instant they were issued

`Math.floor(leaseDurationMs / 1000)` turned a 500 ms lease into the interval
`'0 seconds'`, making every job on such a queue immediately reapable. The
coordination path now stamps milliseconds, matching the fast path.

---

## New behaviour: retry policy

`attempts` and `max_attempts` were dead columns — nothing read or wrote them, and
`failJob` deleted the row on the first failure. Per your call, failures *and*
crashes now retry:

- **`attempts` is incremented when a job is leased**, on both paths. One lease =
  one attempt, so a crash costs an attempt exactly like an explicit failure.
- **`failJob`** returns the job to `PENDING` while `attempts < max_attempts`
  (releasing its coordination slot and keeping `lease_seq` as the fence), and
  discards it on the failure that spends the last attempt. It returns the job
  with `status: PENDING` when retried, `FAILED` when discarded.
- **The reaper** joins `queues` for the budget and discards an expired job that
  has no attempts left, so a job that reliably kills its worker cannot loop for
  ever.

Also wired up `ReaperServiceOptions.batchSize`, which was accepted and then
ignored — `recoverJobs()` had a hardcoded `LIMIT 100`.

Removed three leftover debug `console.log`s (`createQueue` printed its full
128-element parameter array on every call; `runOnce` logged on every tick).

---

## Known open

```
does not let a saturated group block unrelated groups
  → a saturated group at the head of the queue starves every other group
```

**Left red on purpose** — group-fair scheduling is yours to do separately. The
coordination pull considers only the single oldest `PENDING` job in the queue, so
if that job's group is at its cap the pull is refused even when other groups are
idle. The test is marked as the marker for that work.

The fix, when you get to it, is to filter saturated groups out of the candidate
scan. The authoritative check stays the conditional `UPDATE`, so this only
affects fairness, never correctness:

```sql
FROM jobs j
LEFT JOIN group_queue_limits g
  ON g.queue_id = j.queue_id AND g.group_id = j.group_id
WHERE j.status = 'PENDING' AND j.queue_id = $1
  AND (j.group_id IS NULL OR g.group_id IS NULL OR g.running < g.max_running)
ORDER BY j.created_at LIMIT 1
FOR UPDATE OF j SKIP LOCKED
```

## Still worth a decision

1. **No backoff between retries.** `WorkerService` fails a job and can
   immediately re-pull it, hot-looping a poison job until its attempts are spent.
   Bounded, but it burns a worker slot at full speed. A `retry_after` column
   would fix it.
2. **Terminal jobs leave no trace.** Completed, failed and attempt-exhausted jobs
   are all `DELETE`d. `CLAUDE.md` describes an append-only `job_status` archive
   that they are moved into — no such table exists in `migrations/`. Either the
   doc or the schema is stale.
3. **`isExpired` in `pullJobWithCoordination` is dead code** — the surrounding
   `SELECT` filters `status = 'PENDING'`, so it is always `false`. Left in place
   to keep this diff focused on the defects.
