# PR03 — storage: sharded pull, lease fencing and dual-path settlement

| | |
|---|---|
| **base** | `split/02-schema` |
| **head** | `split/03-storage` |
| **open** | https://github.com/nguyenvu1619/distributed-task-queue/compare/split/02-schema...split/03-storage?expand=1 |

## Title

```
storage: sharded pull, lease fencing and dual-path settlement
```

## Description

---

Rewrites the repositories against the new schema. The core of the queue — worth the most review time.

Every job path is a single statement and a single round trip. The fast paths were always one statement; the coordination paths — shard pick, group cap and lease on pull; settle plus slot release; the reaper sweep — are data-modifying CTE chains rather than BEGIN/COMMIT transactions. A refused gate (no shard free, group at cap) is a CTE matching zero rows, so nothing downstream of it writes; no pooled connection is pinned, and a worker crash cannot leave a transaction half-open holding locks.

**Check:**
- **Blocker** — `recoverJobs` uses `ORDER BY created_at DESC` under `LIMIT`. The reaper reclaims the *newest* expired jobs, so once more leases expire than `batchSize`, the longest-stranded jobs are never recovered. One word (`ASC`), needs a test.
- `group_release` references `shard_release` (`(SELECT count(*) FROM shard_release) >= 0`) purely to pin shard-then-group lock order — independent data-modifying CTEs execute in unspecified order. Deleting the "useless" predicate reintroduces a deadlock.
- The group cap is enforced by the atomic conditional `UPDATE … WHERE running < max_running`, re-checked under the row lock at write time — not by any prior SELECT.
- Two execution paths (fast vs coordination) selected by the same predicate in pull, complete and fail — spelled out three times. If they ever disagree, a counter leaks and the queue wedges at its cap.
- Terminal jobs are `DELETE`d, not archived. Nothing records that a job ran.
