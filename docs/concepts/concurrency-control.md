# Concurrency control

Three different settings limit how many jobs run at once. They are easy to
confuse:

| Setting               | Set on                          | Limits                                                         | Enforced by |
| --------------------- | ------------------------------- | -------------------------------------------------------------- | ----------- |
| Worker `concurrency`  | each `WorkerService`            | jobs one worker process runs at once                           | that process |
| Queue `concurrency`   | the queue, in `createQueue`     | `PROCESSING` jobs in the whole queue, across all workers       | the database |
| Group `concurrency`   | the job, in `publishJob`        | `PROCESSING` jobs of one group in one queue                    | the database, on the coordination path only |

## Worker concurrency (slots)

A `WorkerService` with `concurrency: N` runs N independent loops, called
slots, in one process. Each slot pulls a job, runs the handler, records the
outcome and pulls again. The capacity of a fleet is the sum of its slots.

A slot holds a database connection only while a pull or settle statement runs,
not while your handler runs.

## Queue concurrency

`concurrency: N` with N > 0 caps the number of `PROCESSING` jobs in the queue
at exactly N, however many workers and processes are pulling. Use it to
protect a downstream system that can only take N requests at once.

The N slots are spread over up to 32 counter rows (`queue_shards`), so that
concurrent pulls do not all queue on one row. A pull takes a slot from any
shard that has one free. Settling the job gives the slot back, and the reaper
gives back the slots of crashed jobs.

A pull skips shard rows that another pull has locked at that instant. With
very small caps (few shard rows) and many workers, a pull can therefore come
back empty while a slot is free. The worker polls again after its
`pollInterval`.

`concurrency: 0` means no cap. The cap is fixed when the queue is created.

## Group concurrency

A group caps concurrency per key within a queue: per tenant, per customer, or
per external account, so that one busy customer cannot take every worker.

```ts
await jobService.publishJob({
  queueId: queue.id,
  idempotencyKey: `sync:${tenantId}:${batchId}`,
  payload: JSON.stringify({ tenantId, batchId }),
  group: { id: `tenant:${tenantId}`, concurrency: 2 }, // at most 2 at once for this tenant
});
```

Rules:

- **Group caps are only enforced on the coordination path.** Create the queue
  with `requiresGroupId: true` (or a queue `concurrency` above 0). On a queue
  with `concurrency: 0` and `requiresGroupId: false`, the group is stored on
  the job but its cap is **silently ignored**.
- **A group is identified by queue and group id.** The same group id on two
  queues is two independent caps.
- **The first publish that names a group sets its cap, permanently.** Later
  publishes that pass a different `concurrency` for the same group are
  ignored, and there is no API to change or remove a cap.
- **Always pass a positive `concurrency`.** If the group has no cap yet and
  the job passes `concurrency: 0`, no cap is created and the job is never
  handed out. It also blocks the jobs behind it; see the next section.
- **Jobs without a group are not limited by any group cap**, even on a queue
  with `requiresGroupId: true`. Despite its name, that setting does not reject
  jobs without a group; it turns group enforcement on.

### Head-of-line blocking

On the coordination path, a pull only looks at the single oldest due job. If
that job cannot be admitted, because its group is at its cap or has no cap at
all, the pull returns nothing, even when jobs from other groups are waiting
behind it.

So one saturated group at the front of the queue stalls every worker on that
queue until the group frees a slot. If one group can publish a burst of jobs
while others are waiting, its burst delays everyone. This is a
[known limitation](../known-limitations.md#group-caps).

## Which path does my queue use?

| Queue `concurrency` | `requiresGroupId` | Path         | Queue cap | Group caps |
| ------------------- | ----------------- | ------------ | --------- | ---------- |
| `0`                 | `false`           | fast         | none      | ignored    |
| `0`                 | `true`            | coordination | none      | enforced   |
| greater than `0`    | `false`           | coordination | enforced  | enforced   |
| greater than `0`    | `true`            | coordination | enforced  | enforced   |

## Choosing settings

- **No downstream limit, no tenants:** `concurrency: 0`,
  `requiresGroupId: false`. This is the fast path; scale by adding worker
  slots.
- **A downstream that takes at most N requests at once:** queue
  `concurrency: N`.
- **Fairness between tenants:** `requiresGroupId: true` and a group per
  tenant. Keep head-of-line blocking in mind.
- **More worker slots than the queue cap** is harmless: the extra slots get
  empty pulls and poll again. It does not break the cap.

On the coordination path, every pull that is refused because the queue or a
group is full logs a warning. While a queue or group stays full, that is one
warning per slot per poll interval. Pass the repository a logger that filters
it; see [Logging](../guides/production.md#logging).
