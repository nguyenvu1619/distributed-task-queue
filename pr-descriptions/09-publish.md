# PR09 — publish: join the caller's transaction, batch, and deduplicate

| | |
|---|---|
| **base** | `split/08-observability` |
| **head** | `split/09-publish` |
| **open** | https://github.com/nguyenvu1619/distributed-task-queue/compare/split/08-observability...split/09-publish?expand=1 |

## Title

```
publish: join the caller's transaction, batch, and deduplicate
```

## Description

---

`publishJob(input, executor?)` joins a caller-owned transaction — job and business writes commit together, no outbox table. Adds batch publish and turns a duplicate key into a dedup rather than an error. A standalone publish stays one statement — the group-limit seeding rides in a CTE with the INSERT — so it opens no transaction of its own.

**Check:**
- **Blocker** — `idempotency_key` is UNIQUE across the *whole* table, but the read-back after `ON CONFLICT DO NOTHING` filters on the key alone with **no `queue_id` predicate**. Publishing `order-123` to queue B while it's live on queue A inserts nothing and returns queue A's job with `deduplicated: true`. A publish that silently vanishes. Needs per-`(queue_id, idempotency_key)` uniqueness plus a `queue_id` filter.
- Why `ON CONFLICT DO NOTHING` and not a caught 23505: a raw unique violation aborts the caller's **entire** transaction. That is the whole point of the design — check the read-back preserves it.
- A NUL byte in `metadata` still breaks that guarantee: it's bound `::jsonb` and jsonb rejects the escaped NUL (22P05), erroring on the caller's connection. `payload` is TEXT and accepts it.
- `lifecycle.test.ts` flips here: duplicate key no longer rejects, now asserts one row + `deduplicated: true`.
