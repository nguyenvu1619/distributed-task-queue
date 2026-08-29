# PR12 — notify: wake workers on publish instead of waiting out the poll interval

| | |
|---|---|
| **base** | `split/11-worker-hardening` |
| **head** | `split/12-listen-notify` |
| **open** | https://github.com/nguyenvu1619/distributed-task-queue/compare/split/11-worker-hardening...split/12-listen-notify?expand=1 |

## Title

```
notify: wake workers on publish instead of waiting out the poll interval
```

## Description

---

`pg_notify` rides in the RETURNING clause of whatever statement made the row pullable; `PgNotifier` holds one LISTEN connection and turns those into wake-ups.

**Check:**
- The channel name is built in **two places** — the SQL literal in `job.repository.ts` and `JOB_CHANNEL_PREFIX` in `notifier.ts`. If they drift the wake-up silently never lands and everything falls back to polling.
- Riding in RETURNING costs no extra round trip, and NOTIFY is transactional — so it fires exactly on the caller's COMMIT, and never on rollback.
- Holds **one pool connection** for the process lifetime; budget it in `max`. Must be off (`notify: false`) behind PgBouncer in transaction mode.
- Latency only — the poll interval stays the floor. A dropped connection costs speed, never delivery.
