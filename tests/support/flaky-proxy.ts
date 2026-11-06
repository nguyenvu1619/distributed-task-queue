import * as net from 'net';

/**
 * A TCP proxy between a pool and Postgres that can take the link down on
 * demand. Dropping real sockets makes node-postgres raise the same errors a
 * connection reset or a failover raises in production — nothing is stubbed, so
 * the code under test sees exactly what it would see there.
 */
export interface FlakyProxy {
  /** Point a pool at 127.0.0.1 on this port. */
  readonly port: number;
  /** Connections turned away since the last cut(). */
  readonly refused: number;
  /** Drops every open connection and refuses new ones until restore(). */
  cut(): void;
  restore(): void;
  close(): Promise<void>;
}

export async function startFlakyProxy(target: { host: string; port: number }): Promise<FlakyProxy> {
  const open = new Set<net.Socket>();
  let down = false;
  let refused = 0;

  const server = net.createServer((client) => {
    if (down) {
      refused += 1;
      client.destroy();
      return;
    }

    const upstream = net.connect(target.port, target.host);
    const teardown = () => {
      for (const socket of [client, upstream]) {
        socket.destroy();
        open.delete(socket);
      }
    };
    for (const socket of [client, upstream]) {
      open.add(socket);
      socket.on('error', teardown);
      socket.on('close', teardown);
    }
    client.pipe(upstream);
    upstream.pipe(client);
  });

  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as net.AddressInfo;

  return {
    port,
    get refused() {
      return refused;
    },
    cut() {
      down = true;
      refused = 0;
      for (const socket of open) socket.destroy();
      open.clear();
    },
    restore() {
      down = false;
    },
    close() {
      for (const socket of open) socket.destroy();
      return new Promise((resolve) => server.close(() => resolve()));
    },
  };
}
