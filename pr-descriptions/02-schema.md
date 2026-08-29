# PR02 — schema: rebuild queue/job tables for leases, shards and group limits

| | |
|---|---|
| **base** | `split/01-tooling` |
| **head** | `split/02-schema` |
| **open** | https://github.com/nguyenvu1619/distributed-task-queue/compare/split/01-tooling...split/02-schema?expand=1 |

## Title

```
schema: rebuild queue/job tables for leases, shards and group limits
```

## Description

---

Rewrites migration 000001 (the old `queue`/`job` tables were a Go port and no longer match the repositories) and adds 000002, swapping broad indexes for partial ones.

**Check:**
- 000001 is **edited in place**, not superseded. Any database that already ran it will not pick this up — fine now, wrong once there is a deployment.
- `job`→`jobs`, `queue`→`queues`, `lease_token`→`lease_seq`, `group_id` now nullable, `queue_permits` → `queue_shards` + `group_queue_limits`.
- `lease_duration` stays BIGINT nanoseconds (Go leftover); repos divide by 1e6 on read. Keep?
