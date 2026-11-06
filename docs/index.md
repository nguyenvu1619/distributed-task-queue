# distributed-task-queue

A job queue for Node.js and TypeScript that runs entirely on PostgreSQL.

Jobs are rows in your database. Producers insert them, workers lease them with
`SELECT ... FOR UPDATE SKIP LOCKED`, and a reaper takes back jobs whose worker
died. There is no broker to run: if you have PostgreSQL, you have a queue.

## Why use it

- **No extra infrastructure.** No Redis, RabbitMQ or SQS to operate. Producers,
  workers and reapers on any number of machines coordinate through row locks.
- **Enqueue in the same transaction as your business write.** The job commits
  with the order row that justifies it, or not at all. No outbox table needed.
  See [Publishing inside your transaction](guides/publishing-jobs.md#publishing-inside-your-transaction).
- **One job per idempotency key.** Publishing a key that a live job already
  holds returns that job instead of creating a second one or raising an error.
- **Fenced leases.** A worker that lost its lease (crash, pause, slow handler)
  cannot complete or fail a job that another worker now owns.
- **Concurrency caps enforced by the database**, per queue and per group (for
  example, per tenant), across every worker process.
- **Delayed jobs, snooze, non-retryable failures**, and retries of the final
  database write through transient connection errors.

## When it is not a good fit

- **High-volume throughput.** Every job costs several writes to one table:
  an insert, at least one update, and a delete. Throughput is bounded by what
  your database can absorb. Benchmark before relying on it for heavy traffic.
- **You need job history, a dead-letter queue, cron schedules or a dashboard.**
  None of these are built in. See [Known limitations](known-limitations.md).
- **Your queue and your business data live in different databases.**
  Transactional publish only works inside one database.

## Delivery guarantees

A job is delivered **at least once**. It runs again if its worker crashes, if
its lease expires while the handler is still running, or if the worker cannot
record the outcome before giving up. Write handlers that are safe to run twice.
See [Delivery guarantees](concepts/job-lifecycle.md#delivery-guarantees).

## Contents

**Getting started**

- [Installation](getting-started/installation.md): requirements, installing,
  creating the database schema
- [Quick start](getting-started/quick-start.md): a producer, a worker and a
  reaper in one file

**Concepts**

- [How it works](concepts/how-it-works.md): tables, roles, leases and fencing
- [Job lifecycle](concepts/job-lifecycle.md): states, attempts, retries,
  ordering, delivery guarantees
- [Concurrency control](concepts/concurrency-control.md): worker slots, queue
  caps, group caps

**Guides**

- [Queues](guides/queues.md): creating queues and choosing their settings
- [Publishing jobs](guides/publishing-jobs.md): deduplication, delays, batches,
  transactional publish
- [Workers](guides/workers.md): handlers, snooze and non-retryable errors,
  settle retries, shutdown, custom loops
- [Crash recovery](guides/crash-recovery.md): the reaper
- [Running in production](guides/production.md): connection pools, logging,
  shutdown, monitoring

**Reference**

- [API](reference/api.md)
- [Errors](reference/errors.md)
- [Database schema](reference/database-schema.md)

**[Known limitations](known-limitations.md)**
