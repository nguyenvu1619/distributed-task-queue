# PR15 — test: suites for the public API

| | |
|---|---|
| **base** | `split/14-examples` |
| **head** | `split/15-facade-tests` |
| **open** | https://github.com/nguyenvu1619/distributed-task-queue/compare/split/14-examples...split/15-facade-tests?expand=1 |

## Title

```
test: suites for the public API
```

## Description

---

Facade harness plus five suites — client, producer, consumer, transaction, notify.

**Check:**
- Several tests deliberately pin behaviour that is **wrong** (group on a fast-path queue, NUL in metadata, first-use publish starving the pool). Each carries a comment. Fixing one is *expected* to turn its test red.
- The transaction suite commits/rolls back a real business write alongside the job — a test touching only `jobs` wouldn't prove the claim.
- Verified by mutation: nine one-line breaks to `src/`, each turning the relevant suite red. Table in `tests/README.md`. Re-run after changing the worker loop or publish path.
