# PR08 — observability: injectable logger, Executor type, flexible pool config

| | |
|---|---|
| **base** | `split/07-storage-tests` |
| **head** | `split/08-observability` |
| **open** | https://github.com/nguyenvu1619/distributed-task-queue/compare/split/07-storage-tests...split/08-observability?expand=1 |

## Title

```
observability: injectable logger, Executor type, flexible pool config
```

## Description

---

The library logs on ordinary outcomes ("queue saturated"), which floods stdout on a busy queue — so the sink has to be the caller's. Adds the structural `Executor` the transactional publish will need.

**Check:**
- `DatabaseConfig.connectionString` **takes precedence** over the discrete host/port/user fields rather than merging. Is that the precedence you want?
- `ReaperService.stop()` is now async and awaits the in-flight pass, so closing the pool can't cancel a live transaction. It also drops a `QueueRepository` it never used (the `harness.ts` change).
- `Executor` is structural (`{ query(text, values) }`), not `Pool | PoolClient` — a pg `Pool` doesn't extend `ClientBase`, and this also admits a Knex/Kysely/Drizzle handle.
- Deletes `src/domain/group.ts`, a duplicate of the `Group` already in `domain/job.ts`. Nothing imported it.
