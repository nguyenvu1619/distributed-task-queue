# Known limitations

Current behaviour that may surprise you, with a workaround where one exists.

## Group caps

- **Ignored on the fast path.** On a queue with `concurrency: 0` and
  `requiresGroupId: false`, a job's group is stored but its cap is not
  enforced. *Workaround:* create the queue with `requiresGroupId: true`.
- **A group without a cap blocks its queue.** If a job names a group that has
  no cap yet and passes `concurrency: 0`, no cap is created, and on the
  coordination path the job is never handed out. Because a pull only looks at
  the oldest due job, every job behind it on that queue is blocked too.
  *Workaround:* always pass a positive `concurrency` with a group.
- **Head-of-line blocking.** When the oldest due job's group is at its cap, a
  pull returns nothing, even if jobs from other groups are waiting. One
  saturated group at the front of a queue stalls the whole queue until the
  group frees a slot.
- **Caps cannot be changed.** The first publish that names a group fixes its
  cap; later values are ignored, and there is no API to change or remove it.

See [Group concurrency](concepts/concurrency-control.md#group-concurrency).

## Retries

- **No backoff between attempts.** A failed job is due again at once and keeps
  its place at the front of the line, so a job failing on a temporary problem
  can use up its attempts within moments.
- **Snoozes are not attempts.** `JobSnooze` delays a job without spending an
  attempt, so "retry with a delay, give up after N tries" cannot be expressed
  with `maxAttempts`. *Workaround:* bound snoozes yourself, for example by
  `job.createdAt`; see [Snooze](guides/workers.md#snooze-try-again-later).

## Leases

- **A lease cannot be extended.** A handler that runs longer than the queue's
  `leaseDuration` can be reclaimed while it runs, and the job then runs twice
  at the same time. *Workaround:* a generous `leaseDuration`, and splitting
  long work into smaller jobs.
- **Handlers have no timeout or cancellation.** `WorkerService.stop()` waits
  for a handler that never returns indefinitely.

## History and visibility

- **No job history and no dead-letter queue.** Completed and failed jobs are
  deleted. *Workaround:* record what you need in the handler; see
  [No history](concepts/job-lifecycle.md#no-history).
- **Silent discards by the reaper.** A job whose last attempt ends with an
  expired lease is deleted without a log line naming it.

## Queues

- **Configuration is fixed at creation.** There is no API to change or delete
  a queue, and `createQueue` with an existing name ignores the configuration
  you pass.

## Workers

- **Polling only.** On an idle queue, a new job waits up to one
  `pollInterval` before a worker picks it up. There is no `LISTEN/NOTIFY`.
- **`WorkerService` logs only to the console.** It does not accept a logger.
- **The `Logger` type is not exported.** Any object with `debug`, `info`,
  `warn` and `error` methods works.

## Scheduling

- **No recurring jobs.** There are one-off delays (`delayMs`), but no cron
  schedules. *Workaround:* run your own scheduler that publishes jobs with
  keys derived from the schedule, such as `report:2026-11-06`. Two firings
  then share one job while it is queued or running, but a firing that comes
  after the job has finished creates a new one.

## Process-wide side effects

- **`BIGINT` values become numbers for your queries too.** See
  [BIGINT values come back as numbers](guides/production.md#bigint-values-come-back-as-numbers).
