import { Pool, PoolClient } from 'pg';

import { Logger, consoleLogger } from '../../domain/logger';
import { JobWakeup } from '../../domain/worker';

/**
 * Prefix of the channel a queue's arrivals are announced on.
 *
 * The producing half of this lives in SQL — `pg_notify('tq_job_' || queue_id,
 * '')` inside JobRepository — so the two spellings have to agree or the wake-up
 * simply never lands and every worker quietly falls back to its poll interval.
 */
export const JOB_CHANNEL_PREFIX = 'tq_job_';

export function jobChannel(queueId: number): string {
  return `${JOB_CHANNEL_PREFIX}${queueId}`;
}

const INITIAL_RECONNECT_DELAY = 250;
const MAX_RECONNECT_DELAY = 10_000;

/** A subscription to one channel. Closing it never closes the connection. */
export interface QueueWatch extends JobWakeup {
  close(): void;
}

/**
 * One promise shared by every waiter, swapped for a fresh one each time it
 * fires.
 *
 * A slot captures `next()` *before* it pulls, so a notification that lands
 * while the pull is in flight resolves the promise the slot is about to await
 * rather than being missed in the gap between the two.
 */
class Broadcast implements JobWakeup {
  private wake!: () => void;
  private pending: Promise<void> = new Promise<void>((resolve) => {
    this.wake = resolve;
  });

  next(): Promise<void> {
    return this.pending;
  }

  fire(): void {
    const wake = this.wake;
    this.pending = new Promise<void>((resolve) => {
      this.wake = resolve;
    });
    wake();
  }
}

/**
 * Turns `NOTIFY` into worker wake-ups.
 *
 * LISTEN is session state, so this holds one connection of its own for as long
 * as anything is subscribed — budget for it in the pool's `max`. It is a
 * latency optimisation and nothing more: the poll loop still runs on its own
 * interval, so a connection that drops, a channel that never arrives, or a
 * pooler that swallows LISTEN entirely costs delivery *speed*, never delivery.
 */
export class PgNotifier {
  private client: PoolClient | null = null;
  private connecting: Promise<void> | null = null;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private reconnectDelay = INITIAL_RECONNECT_DELAY;
  private closed = false;
  private readonly channels = new Map<string, Set<Broadcast>>();
  private readonly released = new WeakSet<PoolClient>();

  constructor(
    private readonly pool: Pool,
    private readonly logger: Logger = consoleLogger
  ) {}

  /**
   * Subscribes to `channel`. The connection is opened on the first watch and
   * kept until the last one closes — callers get a usable watch immediately,
   * before the LISTEN has round-tripped.
   */
  watch(channel: string): QueueWatch {
    const broadcast = new Broadcast();

    let watchers = this.channels.get(channel);
    if (!watchers) {
      watchers = new Set();
      this.channels.set(channel, watchers);
    }
    watchers.add(broadcast);

    void this.listen(channel);

    let closed = false;
    return {
      next: () => broadcast.next(),
      close: () => {
        if (closed) {
          return;
        }
        closed = true;

        const current = this.channels.get(channel);
        if (current) {
          current.delete(broadcast);
          if (current.size === 0) {
            this.channels.delete(channel);
            void this.unlisten(channel);
          }
        }

        // Anything parked on this watch would otherwise wait for a promise that
        // can no longer fire — release it and let the poll loop take over.
        broadcast.fire();
      },
    };
  }

  isConnected(): boolean {
    return this.client !== null;
  }

  /**
   * Drops the connection and releases every watcher. Must run before the pool
   * is ended: a checked-out client keeps `pool.end()` waiting for ever.
   */
  async close(): Promise<void> {
    if (this.closed) {
      return;
    }
    this.closed = true;

    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }

    // Let an in-flight connect finish, otherwise it installs its client just
    // after we let go of ours and the pool is left holding it.
    await this.connecting?.catch(() => undefined);

    for (const watchers of this.channels.values()) {
      for (const watcher of watchers) {
        watcher.fire();
      }
    }
    this.channels.clear();

    const client = this.client;
    this.client = null;
    if (client) {
      this.releaseQuietly(client);
    }
  }

  private async listen(channel: string): Promise<void> {
    const client = await this.connect();
    // The watch may already have been closed while the connection was opening.
    if (!client || !this.channels.has(channel)) {
      return;
    }

    try {
      await client.query(`LISTEN ${quoteIdentifier(channel)}`);
    } catch (error) {
      // Dropping schedules a reconnect, which re-LISTENs everything.
      this.drop(client, error);
    }
  }

  private async unlisten(channel: string): Promise<void> {
    const client = this.client;
    if (!client) {
      return;
    }

    try {
      await client.query(`UNLISTEN ${quoteIdentifier(channel)}`);
    } catch (error) {
      // Nothing is broken by a channel we keep listening to: the extra
      // notifications land on a set with no watchers and are dropped.
      this.logger.debug(`Failed to UNLISTEN ${channel}`, error);
    }
  }

  private async connect(): Promise<PoolClient | null> {
    if (this.closed) {
      return null;
    }
    if (this.client) {
      return this.client;
    }
    if (!this.connecting) {
      this.connecting = this.open().finally(() => {
        this.connecting = null;
      });
    }

    // Never rejects. Callers reach this from `void`-ed background paths — a
    // wake-up that failed to arm must not take the process down with it.
    try {
      await this.connecting;
    } catch (error) {
      this.logger.warn('Could not open the job notification listener', error);
      return null;
    }
    return this.client;
  }

  private async open(): Promise<void> {
    let client: PoolClient;
    try {
      client = await this.pool.connect();
    } catch (error) {
      this.scheduleReconnect(error);
      return;
    }

    if (this.closed) {
      this.releaseQuietly(client);
      return;
    }

    client.on('notification', (message) => {
      const watchers = this.channels.get(message.channel);
      if (!watchers) {
        return;
      }
      for (const watcher of watchers) {
        watcher.fire();
      }
    });
    client.on('error', (error) => this.drop(client, error));
    client.on('end', () => this.drop(client));

    // Installed before the LISTENs so a failure part-way through still goes
    // through drop() and gets a reconnect scheduled.
    this.client = client;

    try {
      for (const channel of this.channels.keys()) {
        await client.query(`LISTEN ${quoteIdentifier(channel)}`);
      }
    } catch (error) {
      this.drop(client, error);
      return;
    }

    this.reconnectDelay = INITIAL_RECONNECT_DELAY;
  }

  private drop(client: PoolClient, error?: unknown): void {
    const wasCurrent = this.client === client;
    if (wasCurrent) {
      this.client = null;
    }
    this.releaseQuietly(client);

    if (!wasCurrent || this.closed || this.channels.size === 0) {
      return;
    }
    this.scheduleReconnect(error);
  }

  private scheduleReconnect(error?: unknown): void {
    if (this.closed || this.reconnectTimer) {
      return;
    }

    const delay = this.reconnectDelay;
    this.reconnectDelay = Math.min(delay * 2, MAX_RECONNECT_DELAY);
    if (error) {
      this.logger.warn(
        `Job notification listener lost; polling until it is back (retry in ${delay}ms)`,
        error
      );
    }

    const timer = setTimeout(() => {
      this.reconnectTimer = null;
      if (this.closed || this.channels.size === 0) {
        return;
      }
      void this.reconnect();
    }, delay);
    // A pure latency optimisation must not be the reason a process stays alive.
    timer.unref?.();
    this.reconnectTimer = timer;
  }

  private async reconnect(): Promise<void> {
    const client = await this.connect();
    if (!client) {
      return;
    }

    // Whatever was published while the connection was down was never delivered
    // and never will be. Wake every watcher once so those jobs are picked up
    // now rather than at the next poll.
    for (const watchers of this.channels.values()) {
      for (const watcher of watchers) {
        watcher.fire();
      }
    }
  }

  private releaseQuietly(client: PoolClient): void {
    if (this.released.has(client)) {
      return;
    }
    this.released.add(client);
    try {
      // `true` destroys the connection instead of handing a session that is
      // still LISTENing back to the pool.
      client.release(true);
    } catch (error) {
      this.logger.debug('Notification client was already released', error);
    }
  }
}

function quoteIdentifier(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}
