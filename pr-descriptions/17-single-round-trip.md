# PR17 — perf: every job path in a single round trip

| | |
|---|---|
| **base** | `split/16-docs` |
| **head** | `split/17-single-round-trip` |
| **open** | https://github.com/nguyenvu1619/distributed-task-queue/compare/split/16-docs...split/17-single-round-trip?expand=1 |

## Title

```
perf: every job path in a single round trip
```

## Description

---

Every remaining BEGIN/COMMIT in `job.repository.ts` becomes one CTE statement. Pull, complete, fail, discard, publish and the reaper sweep are each a single round trip now, and none of them pins a pooled connection while it works.

| path | round trips before | after |
|---|---|---|
| pull (shard + group coordination) | up to 7 | 1 |
| complete / discard (coordination) | 6 | 1 |
| fail (coordination) | 6–7 | 1 |
| publish with groups, standalone | 3 + one per group | 1 |
| fail (fast path, discard branch) | 2 | 1 |
| reaper sweep | up to 7 | 1 |

Why it stays correct without a transaction:
- A single statement is atomic on its own. A refused gate (no shard free, group at cap) is a CTE matching zero rows, so nothing downstream of it writes — what ROLLBACK used to undo now never happens.
- The group cap is still an atomic conditional `UPDATE … WHERE running < max_running`, re-checked under the row lock at write time.
- Lock order stays shard → job → group everywhere. `group_release` references `shard_release` deliberately: independent data-modifying CTEs run in unspecified order, and releasing in the opposite order of pull's locking could deadlock.
- Publish folds the `group_queue_limits` seeding into the INSERT statement, so job + limit row stay atomic with no `withExecutor` — that helper and `releaseCoordinationSlots` are deleted.

**Check:**
- The dedup read-back in `insertJobs` stays a second (conflict-only) round trip on purpose: folded into the INSERT it would read the statement snapshot, which cannot see a row a concurrent publisher committed mid-flight — the separate READ COMMITTED statement can. The happy path is one trip.
- The pull statement always returns one diagnostics row (`has_shard`, `candidate_group`, `group_admitted`) LEFT JOINed with the leased job, so the two warn logs survive the rewrite.
- PR03's dead `isExpired` guards are gone as a side effect (the candidate CTE only selects PENDING).
- PR03's reaper blocker (`ORDER BY created_at DESC` under LIMIT) and PR09's cross-queue idempotency blocker are deliberately **not** fixed here — semantics preserved, round trips removed. They still need their own PRs.
- `pg_notify` still rides in the RETURNING of the statement that makes a row pullable; only retry and publish wake workers, and a discard stays silent.
