# PR07 — test: lifecycle, concurrency and crash-fencing suites

| | |
|---|---|
| **base** | `split/06-test-harness` |
| **head** | `split/07-storage-tests` |
| **open** | https://github.com/nguyenvu1619/distributed-task-queue/compare/split/06-test-harness...split/07-storage-tests?expand=1 |

## Title

```
test: lifecycle, concurrency and crash-fencing suites
```

## Description

---

Three suites over the storage layer, all against real SQL.

**Check:**
- One test fails here and stays red through the stack: the group-fairness marker in `concurrency.property.test.ts` (saturated group blocks unrelated groups), deferred to v2 in `V1_CHECKLIST.md`. If you want CI green it needs an explicit skip-with-reason — your call.
- `lifecycle` currently asserts a duplicate idempotency key **rejects** with 23505. That behaviour changes in PR09.
- `crash-fencing` SIGKILLs a real process; a parallel describe re-checks the same rules deterministically so a regression doesn't hinge on spawn timing.
