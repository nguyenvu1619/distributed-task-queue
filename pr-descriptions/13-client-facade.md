# PR13 — client: the public TaskQueue facade

| | |
|---|---|
| **base** | `split/12-listen-notify` |
| **head** | `split/13-client-facade` |
| **open** | https://github.com/nguyenvu1619/distributed-task-queue/compare/split/12-listen-notify...split/13-client-facade?expand=1 |

## Title

```
client: the public TaskQueue facade
```

## Description

---

Adds `src/client/` and narrows `src/index.ts` to the facade plus domain types. Repositories/services stay reachable by deep import but leave the versioned surface.

**Check:**
- **First-use publish inside a transaction takes a second connection.** `QueueHandle.publish` awaits `resolve()` before looking at `options.tx`, and `ensureQueue` runs on the repo's own pool — on a `max: 1` pool (serverless/pgbouncer) that deadlocks. Pre-resolving avoids it; a typed error would beat a hang.
- `close()` order matters: workers → reapers → notifier → pool. The notifier holds a checked-out client and `pool.end()` waits forever on those.
- A `group` on a fast-path queue is accepted and written to `group_queue_limits`, but `pullJobFast` never reads that table — the cap is silently unenforced.
- `group.id` isn't checked non-empty: `{ id: '', concurrency: 5 }` stores NULL and the job is leased uncapped. An unset env var does exactly this.
- `migrate()` resolves `__dirname/../../migrations`, so `migrations/` must be in `files` before publishing.
