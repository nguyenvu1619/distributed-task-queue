# Errors

## Errors the library throws

They all extend `TaskQueueError`, which extends `Error`, so
`err instanceof TaskQueueError` matches any of them. Each error's `name` is
its class name.

| Class                   | Thrown by | Meaning | What to do |
| ----------------------- | --------- | ------- | ---------- |
| `QueueNotFoundError`    | `getQueue`, `pullJob`, `WorkerService.start`, and `completeJob`/`failJob` without `Direct` | No queue has that id. | Check the id, or create the queue first. |
| `JobNotFoundError`      | `getJob`, and `completeJob`/`failJob` without `Direct` | No live job has that id. It may have been completed or discarded already. | When settling, treat it like `LeaseLostError`. |
| `LeaseLostError`        | every complete, fail and snooze call | The `lockSeq` you passed does not hold the job's lease: the lease expired and the job was handed out again, the job was already settled, or it was never pulled. | Stop working on the job. Do not assume your outcome was recorded. |
| `QueueCreateRaceError`  | `createQueue` | Another process created a queue with this name and deleted it again during the call. | Retry. |
| `PublishInvariantError` | `publishJob`, `publishJobs` | The publish statement returned no row for one of its inputs. This guards an internal invariant and is not expected to happen. | Report it as a bug. |

```ts
import { LeaseLostError, JobNotFoundError } from 'distributed-task-queue';

try {
  await jobService.completeJob(job.id, job.lockSeq);
} catch (err) {
  if (err instanceof LeaseLostError || err instanceof JobNotFoundError) {
    // Not ours any more. Another worker owns the job, or it is already done.
  } else {
    throw err;
  }
}
```

## Signals your handler throws

These are instructions to `WorkerService`, not failures of the library. They
extend `JobSignal`, which extends `Error`, and are not `TaskQueueError`s.

| Class                                | Effect |
| ------------------------------------ | ------ |
| `new JobSnooze(retryAfterMs, message?)` | The job goes back to `PENDING`, due after `retryAfterMs` milliseconds. The attempt is refunded. |
| `new NonRetryable(message?)`         | The job is failed and deleted now, whatever attempts remain. |

Throwing `JobSignal` itself, or any other value, counts as an ordinary failure.
See [Writing a handler](../guides/workers.md#writing-a-handler).

## Other errors

- **`RangeError`** from `publishJob` or `publishJobs` when a `delayMs` is not a
  finite number. Nothing is published.
- **Database errors** from node-postgres pass through unchanged. Errors sent by
  the server carry the PostgreSQL SQLSTATE in `err.code`. For example,
  publishing to a queue id that does not exist fails with `'23503'`
  (foreign-key violation). Connection failures, such as a refused connection,
  a dropped connection or a checkout timeout, arrive as plain `Error`s without
  a SQLSTATE.
