# Queues

A queue is a named stream of jobs with its own retry budget, lease length and
concurrency settings. Workers pull from one queue each.

## Creating a queue

```ts
const queue = await queueService.createQueue({
  name: 'emails',
  maxAttempts: 5,
  leaseDuration: 120_000,
  concurrency: 0,
  requiresGroupId: false,
});
```

| Option            | Type    | Required             | Meaning |
| ----------------- | ------- | -------------------- | ------- |
| `name`            | string  | yes                  | Unique name of the queue. |
| `maxAttempts`     | number  | yes                  | How many times a job may be leased before it is discarded. `1` means no retries. |
| `leaseDuration`   | number  | yes                  | Milliseconds a worker may hold a job before the reaper may take it back. |
| `concurrency`     | number  | yes                  | Cap on `PROCESSING` jobs across the whole queue. `0` means no cap. Must be `0` or a positive integer. |
| `requiresGroupId` | boolean | no, default `false`  | Enforces group caps even when `concurrency` is `0`. It does not make groups mandatory. |

The result is a `Queue`. Pass its `id` to everything else: publishing, workers,
lookups.

`concurrency` and `requiresGroupId` together decide which execution path the
queue uses. See [Concurrency control](../concepts/concurrency-control.md#which-path-does-my-queue-use).

## Calling createQueue on every startup

If a queue with the same name exists, `createQueue` returns it instead of
failing, and logs a warning. Concurrent calls from several processes resolve to
the same single queue. So you can call `createQueue` at startup in every
process.

The existing queue keeps the configuration it was created with. **The values
you pass are not applied**, so changing them in your code has no effect on a
queue that already exists. If that matters, check the result:

```ts
if (queue.leaseDuration !== 120_000 || queue.maxAttempts !== 5) {
  throw new Error(`queue "${queue.name}" exists with a different configuration`);
}
```

## Choosing leaseDuration

The lease has to outlast your slowest handler run, plus the time to record its
outcome. If a handler is still running when the lease expires, the reaper can
hand the job to another worker, and the job then runs twice at the same time.
A lease cannot be extended from inside a handler.

A longer lease has a cost: when a worker crashes, its job waits until the
lease expires before it runs again. Choose a value comfortably above your
slowest expected run, and split work that can take arbitrarily long into
smaller jobs.

## Choosing maxAttempts

`maxAttempts` counts leases: every run, including runs that ended with an
expired lease. Retries happen immediately, with no backoff, so a high
`maxAttempts` does not buy time for a downstream system to recover. For that,
use [JobSnooze](workers.md#snooze-try-again-later).

## Looking up queues

```ts
const queue = await queueService.getQueue(queueId); // throws QueueNotFoundError
const queues = await queueService.getAllQueues();
```

There is no lookup by name. `createQueue` returns the existing queue for a
name, so it doubles as one.

`QueueRepository` caches queues by id in memory for the life of the process.
Queue configuration cannot change through the API, so the cache does not go
stale, unless you change or delete rows by hand.

## Changing or deleting a queue

The API cannot change or delete a queue. To change the configuration, create a
queue under a new name, point producers at it, and let workers drain the old
one.

If you delete a row from `queues` by hand, its jobs and counters are deleted
with it (`ON DELETE CASCADE`). Restart the processes that had the queue cached.
