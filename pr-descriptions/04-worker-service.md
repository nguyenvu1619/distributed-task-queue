# PR04 — worker: concurrent slot loop over a queue

| | |
|---|---|
| **base** | `split/03-storage` |
| **head** | `split/04-worker-service` |
| **open** | https://github.com/nguyenvu1619/distributed-task-queue/compare/split/03-storage...split/04-worker-service?expand=1 |

## Title

```
worker: concurrent slot loop over a queue
```

## Description

---

N slots polling one queue, sharing a single resolved `Queue` so pull/complete/fail skip the per-call lookup.

**Check** (all four are fixed later in the stack — flagging so the progression reads):
- Handler errors and `completeJob` errors share one `try`, so a transient DB error while recording success re-runs a job that already succeeded.
- `JSON.parse` is inside that `try`, so an undecodable payload burns the whole attempt budget one redelivery at a time.
- `stop()` cannot be bounded — a slot in `sleep(pollInterval)` burns the full interval.
- `start()` has no in-flight guard; two concurrent calls orphan a set of slots.
