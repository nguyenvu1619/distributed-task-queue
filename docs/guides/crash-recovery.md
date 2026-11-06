# Crash recovery

## Why you need a reaper

When a worker dies mid-job (a crash, an out-of-memory kill, a deploy, a lost
network), its job stays `PROCESSING` with a lease that nobody will settle.
Pulls only look at `PENDING` jobs, so nothing else will ever touch it. The
reaper is what puts such jobs back.

Without a running reaper:

- jobs whose worker died are never run again;
- on the coordination path, they keep holding their queue and group slots, so
  each crash permanently shrinks the queue's capacity, until nothing can run.

## Running the reaper

```ts
import { ReaperService } from 'distributed-task-queue';

const reaper = new ReaperService(jobRepo, {
  interval: 30_000, // ms between passes
  batchSize: 100,   // most jobs reclaimed per pass
});

reaper.start();      // runs a pass now, then one every `interval`

// on shutdown
await reaper.stop(); // waits for a pass in flight
```

| Option      | Default    | Meaning |
| ----------- | ---------- | ------- |
| `interval`  | `30000`    | Milliseconds between the end of one pass and the start of the next. |
| `batchSize` | `100`      | Most expired jobs handled per pass. |
| `logger`    | console    | Where its log lines go. See [Logging](production.md#logging). |

`0` for `interval` or `batchSize` means the default. One reaper covers every
queue in the database.

## What a pass does

A pass handles up to `batchSize` jobs that are `PROCESSING` with an expired
lease. In a single statement, it:

1. gives back their queue and group slots;
2. deletes the jobs that have used all their attempts, because an expired
   lease counts as an attempt;
3. returns the others to `PENDING`. They are due at once and keep their
   original place in line.

`runOnce()` runs one pass and resolves with the ids of the jobs it returned to
the queue. Jobs it deleted are not included. A pass that returned jobs logs
`[reaper] Recovered N jobs`; a pass with nothing to do logs nothing. A pass
that fails, for example while the database is down, logs
`[reaper] Process error:` with the error, and the next pass runs on schedule.

## Where to run it

Several reapers can run at once safely: a pass skips rows another pass has
locked, so each expired job is handled once. The simplest setup is one reaper
in every worker process, so recovery runs as long as any worker does.

Alternatively, run one in a dedicated process, or call `runOnce()` from a
scheduled task.

## How long recovery takes

A job whose worker died becomes available again after its lease expires, plus
up to one reaper interval. The worst case is `leaseDuration + interval`. You
can lower either one, but a shorter lease also leaves handlers less time.

If more than `batchSize` leases expire at once, the rest wait for the
following passes.

## Jobs that kill their worker

A job that crashes its worker every time, for example by exhausting memory,
uses one attempt per crash, and the reaper deletes it when its attempts run
out. It cannot loop forever. Nothing is logged about which job was deleted.

## Workers that come back

If the "dead" worker was only slow (a long pause, a network partition) and
reports back after its job was reclaimed, its settle is rejected with
`LeaseLostError`, so it cannot overwrite the outcome of the job's new owner.
`WorkerService` logs this and moves on. The side effects of its run have
already happened, though. See
[Delivery guarantees](../concepts/job-lifecycle.md#delivery-guarantees).
