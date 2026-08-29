# PR10 — queue: resolve by name, get-or-create, warn on config drift

| | |
|---|---|
| **base** | `split/09-publish` |
| **head** | `split/10-queue-resolution` |
| **open** | https://github.com/nguyenvu1619/distributed-task-queue/compare/split/09-publish...split/10-queue-resolution?expand=1 |

## Title

```
queue: resolve by name, get-or-create, warn on config drift
```

## Description

---

Queues are addressed by name. `ensureQueue()` is the get-or-create the client facade builds on.

**Check:**
- Queue config is **immutable** — no update path. A queue that exists with different settings keeps the stored ones and only logs a warning. Is a warning the right severity, or should it throw?
- `ensureQueue` deliberately refuses a caller transaction: `createQueue` issues `SET TRANSACTION ISOLATION LEVEL`, which Postgres rejects (25001) mid-transaction. This is what causes the connection issue noted in PR13.
- Name cache is written only after COMMIT, so an aborted create can't leave a phantom queue. No invalidation — a queue deleted out-of-band stays cached for the process lifetime.
