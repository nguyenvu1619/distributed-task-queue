# PR06 — test: integration harness against a real Postgres container

| | |
|---|---|
| **base** | `split/05-benchmarks` |
| **head** | `split/06-test-harness` |
| **open** | https://github.com/nguyenvu1619/distributed-task-queue/compare/split/05-benchmarks...split/06-test-harness?expand=1 |

## Title

```
test: integration harness against a real Postgres container
```

## Description

---

One throwaway PostgreSQL 16 container per `vitest run`, migrations applied once, plus wiring/reset/fixture helpers. No test cases yet.

**Check:**
- `resetDatabase()` must truncate every table the suites touch, including `queue_shards` and `group_queue_limits` — a missed table leaks state and shows up as an unreproducible failure.
- `QueueRepository` caches in-process, so the harness rebuilds repos after a reset or a cached queue outlives its row.
- `npm test` finds no test files at this commit and exits non-zero. PR07 makes it green.
