# Workers

## Starting a worker

```ts
import { WorkerService } from 'distributed-task-queue';

const worker = new WorkerService(jobService, {
  queueId: queue.id,
  concurrency: 10,
  pollInterval: 500,
  handler: async (job, payload) => {
    // your code
  },
});

await worker.start();
```

| Option         | Type                           | Default         | Meaning |
| -------------- | ------------------------------ | --------------- | ------- |
| `queueId`      | number                         | required        | Queue to pull from. `start()` rejects with `QueueNotFoundError` if it does not exist. |
| `handler`      | `(job, payload) => Promise<void>` | required     | Your code. See [Writing a handler](#writing-a-handler). |
| `concurrency`  | number                         | `1`             | Slots: how many jobs this worker runs at once. |
| `pollInterval` | number (ms)                    | `1000`          | How long a slot waits after finding no due job, or after an error, before it pulls again. |
| `settleRetry`  | `SettleRetryOptions`           | see [below](#recording-outcomes-through-database-errors) | How long to keep trying to record an outcome through database errors. |

`start()` looks the queue up, starts the slots and resolves. The slots keep
running until you call `stop()`. Each slot loops: pull a job, run the handler,
record the outcome, pull again.

A worker serves one queue. For several queues, create one worker per queue;
they can share the same `JobService` and pool.

## Writing a handler

```ts
type JobHandler<T = any> = (job: Job, payload: T) => Promise<void>;
```

- **`payload`** is `JSON.parse(job.payload)`. Annotate it to type it:
  `async (job, payload: { userId: number }) => …`. This is a type annotation,
  not validation.
- **`job`** is the leased job. Useful fields: `id`, `idempotencyKey`,
  `attempts` (1 on the first run), `groupId`, `metadata`, `createdAt` and
  `leaseExpiresAt`. See [Job](../reference/api.md#job).
- **If `job.payload` is not valid JSON**, the handler is not called and the
  job is failed for good on its first pull, as if it had thrown
  `NonRetryable`: every retry would parse the same bytes. The worker logs one
  line with the job's id and `idempotencyKey`.

How the handler ends decides what happens to the job:

| The handler…                  | The job is…                                                                |
| ----------------------------- | -------------------------------------------------------------------------- |
| returns                       | completed and deleted                                                      |
| throws `JobSnooze(ms)`        | put back as `PENDING`, due after `ms`; the attempt is refunded             |
| throws `NonRetryable`         | failed and deleted now, whatever attempts remain                           |
| throws anything else          | failed: due again at once if attempts remain, otherwise deleted            |

For example, a webhook sender:

```ts
import { JobSnooze, NonRetryable, WorkerService } from 'distributed-task-queue';

type WebhookPayload = { url: string; body: unknown };

const worker = new WorkerService(jobService, {
  queueId: queue.id,
  concurrency: 10,
  handler: async (job, payload: WebhookPayload) => {
    const res = await fetch(payload.url, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'idempotency-key': job.idempotencyKey },
      body: JSON.stringify(payload.body),
    });

    if (res.status === 429) {
      const seconds = Number(res.headers.get('retry-after')) || 60;
      throw new JobSnooze(seconds * 1000); // come back later, no attempt spent
    }
    if (res.status >= 400 && res.status < 500) {
      throw new NonRetryable(`rejected with ${res.status}`); // retrying cannot help
    }
    if (!res.ok) {
      throw new Error(`failed with ${res.status}`); // retried while attempts remain
    }
  },
});
```

Throw the signal object itself. If you wrap a `JobSnooze` or `NonRetryable` in
another error, the worker sees an ordinary failure.

Handlers can run more than once for the same job, so make them safe to repeat.
See [Delivery guarantees](../concepts/job-lifecycle.md#delivery-guarantees).

## Snooze: try again later

```ts
throw new JobSnooze(30_000);
```

The job goes back to `PENDING`, becomes due after 30 seconds, and releases its
queue and group slots. The attempt it was using is refunded. When it comes
back, any worker can pick it up, under a new lease.

`retryAfterMs` must be a finite number of milliseconds.

Because snoozes are refunded, a job that snoozes every time never runs out of
attempts. Bound it yourself, for example by the job's age:

```ts
const MAX_AGE_MS = 24 * 60 * 60 * 1000;

const worker = new WorkerService(jobService, {
  queueId: queue.id,
  handler: async (job, payload) => {
    if (!(await partnerApiIsUp())) {
      if (Date.now() - job.createdAt.getTime() > MAX_AGE_MS) {
        throw new NonRetryable('partner API has been down for a day, giving up');
      }
      throw new JobSnooze(5 * 60_000);
    }
    await callPartnerApi(payload);
  },
});
```

## Fail for good: NonRetryable

```ts
throw new NonRetryable('invoice was deleted');
```

Use it when retrying cannot succeed: invalid input, a resource that no longer
exists, a request the downstream rejected for good. The job is deleted
immediately, even if attempts remain. The worker logs it like any other
failure.

## Long-running jobs and leases

A lease is set when the job is pulled (the queue's `leaseDuration`) and cannot
be extended. If a handler is still running when its lease expires, the reaper
can return the job to `PENDING`, and another worker can start it while the
first one is still busy. When the first handler finishes, its outcome is
rejected and the worker logs:

```text
[Worker] Slot 0 no longer holds job 42; letting it go
```

To avoid it:

- Set the queue's `leaseDuration` well above your slowest run.
- Split work that can take arbitrarily long into smaller jobs.
- In loops over many items, check `job.leaseExpiresAt` and stop before it
  passes. It comes from the database clock, so leave a margin for clock
  difference.

## Recording outcomes through database errors

After the handler ends, the worker records the outcome; this is called
settling the job. If the database is briefly unreachable, the worker retries
the settle with exponential backoff and jitter. Without the retry, one dropped
connection would leave the job `PROCESSING` until its lease expired, and then
it would run again.

```ts
const worker = new WorkerService(jobService, {
  queueId: queue.id,
  handler,
  settleRetry: { deadlineMs: 60_000 },
});
```

| Option             | Default | Meaning |
| ------------------ | ------- | ------- |
| `deadlineMs`       | `30000` | No retry starts later than this long after the first attempt. `0` turns retries off. |
| `attemptTimeoutMs` | `5000`  | Abandons one attempt's statement after this long. |
| `baseDelayMs`      | `100`   | Upper bound of the random delay before the first retry. It doubles for each retry. |
| `maxDelayMs`       | `2000`  | Cap on that upper bound. |

Waiting for a free pool connection is bounded by the pool's
`connectionTimeoutMillis`, not by `attemptTimeoutMs`.

**Retried:** errors that did not come from the database server (refused or
reset connections, timeouts, connections that ended mid-statement), and server
errors in SQLSTATE classes `08` (connection exception), `40` (serialization
failure, deadlock), `53` (insufficient resources, such as too many
connections) and `57` (operator intervention: shutdown, statement timeout),
plus `25006` (read-only transaction, as after a failover to a standby) and
`55P03` (lock not available).

**Not retried:** any other server error, and `LeaseLostError`, which means
another worker owns the job or an earlier attempt already landed.

Retrying is safe because every settle is fenced by the lease. If an attempt
timed out on the client but committed on the server, the next attempt fails
with `LeaseLostError`, and the worker treats that as done.

When the deadline passes, the worker logs the error and moves on. The job
stays `PROCESSING` until its lease expires; then the reaper returns it to the
queue, and it runs again, using one more attempt.

## Stopping a worker

```ts
await worker.stop();
```

`stop()` stops pulling new jobs and waits for every slot to finish the job in
hand: the handler, then its settle, including settle retries (bounded by
`settleRetry.deadlineMs`). It resolves when all slots have exited.

There is no timeout on the handler. If a handler never returns, `stop()` never
resolves. If your platform kills the process after a grace period (Kubernetes
waits 30 seconds by default), make handlers finish well within it. A job cut
off mid-run is recovered by the reaper once its lease expires.

A full shutdown sequence is in
[Running in production](production.md#graceful-shutdown).

## Logging

`WorkerService` writes to the console, and this cannot be redirected yet:

| Level   | Line |
| ------- | ---- |
| `log`   | `[Worker] Starting N slot(s) for queue Q`, and `[Worker] Stopped (queue Q)` |
| `error` | `[Worker] Slot S failed job J:` with the error, for every failed job, `NonRetryable` included |
| `warn`  | `[Worker] Slot S could not settle job J (attempt N), retrying in Dms: …` |
| `warn`  | `[Worker] Slot S no longer holds job J; letting it go` |
| `error` | `[Worker] Slot S could not settle job J:` with the error, when a settle is given up |
| `error` | `[Worker] Slot S error:` when a pull fails, for example while the database is down |

Snoozes are not logged.

## Writing your own worker loop

If `WorkerService` does not fit, for example because you want a non-JSON
payload or your own scheduling, use the primitives on `JobService`. The
`…Direct` methods take the `Queue` object, which saves a lookup on every call.

```ts
import { JobSnooze, LeaseLostError, NonRetryable, Queue } from 'distributed-task-queue';

/** Runs one job if one is due. Returns false when the queue had nothing. */
async function processNext(queue: Queue): Promise<boolean> {
  const job = await jobService.pullJobDirect(queue);
  if (!job) return false;

  let settle: Promise<void>;
  try {
    await handle(job);
    settle = jobService.completeJobDirect(job.id, job.lockSeq, queue);
  } catch (err) {
    settle =
      err instanceof JobSnooze
        ? jobService.snoozeJobDirect(job.id, job.lockSeq, queue, err.retryAfterMs)
        : jobService.failJobDirect(job.id, job.lockSeq, queue, err instanceof NonRetryable);
  }

  try {
    await settle;
  } catch (err) {
    // The lease expired and another worker owns the job, or it was already
    // settled. Either way it is no longer ours.
    if (!(err instanceof LeaseLostError)) throw err;
  }
  return true;
}
```

Your loop is responsible for what `WorkerService` would do for you:

- **`LeaseLostError` on a settle** means you no longer own the job. Let it go.
- **Transient errors on a settle** need retrying, or the job runs again after
  its lease expires.
- **`completeJob` and `failJob`** (without `Direct`) read the job first. For a
  job that is already gone, they throw `JobNotFoundError` instead of
  `LeaseLostError`. Treat both the same way, or use the `…Direct` variants.
- **Polling:** sleep when `processNext` returns `false`, so an empty queue is
  not polled in a tight loop.
