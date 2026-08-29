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

**Check:**
- **Blocker** — `recoverJobs` uses `ORDER BY created_at DESC` under `LIMIT`. The reaper reclaims the *newest* expired jobs, so once more leases expire than `batchSize`, the longest-stranded jobs are never recovered. One word (`ASC`), needs a test.
- `isExpired` is `status === PROCESSING`, but the SELECT above filters `status = 'PENDING'` — always false, so both `!isExpired` guards are dead code.
- Two execution paths (fast vs coordination) selected by the same predicate in pull, complete and fail — spelled out three times. If they ever disagree, a counter leaks and the queue wedges at its cap.
- Terminal jobs are `DELETE`d, not archived. Nothing records that a job ran.
