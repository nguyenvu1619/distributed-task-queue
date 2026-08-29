# PR11 — worker: handler context, poison discard, bounded shutdown

| | |
|---|---|
| **base** | `split/10-queue-resolution` |
| **head** | `split/11-worker-hardening` |
| **open** | https://github.com/nguyenvu1619/distributed-task-queue/compare/split/10-queue-resolution...split/11-worker-hardening?expand=1 |

## Title

```
worker: handler context, poison discard, bounded shutdown
```

## Description

---

Reworks the slot loop, fixing the four defects flagged in PR04.

**Check:**
- Recording success moved **outside** the handler's `try`. A DB error while completing now leaves the lease to expire and lets the reaper decide, instead of re-running a job that succeeded.
- Deserialization moved outside too: an undecodable payload is poison, discarded without spending an attempt, reported as `onError({ phase: 'deserialize' })`. Note discard leaves **no trace** — the `job_status` archive doesn't exist.
- `stop({ timeout })` bounds the drain and returns `{ drained }`; the poll sleep is interruptible via `AbortController`, also surfaced as `ctx.signal`.
- **Still open:** no lease renewal. A handler outliving `leaseDuration` is reclaimed and re-delivered while still running — job runs twice and the cap is exceeded.
