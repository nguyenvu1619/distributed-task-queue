import { ChildProcess, spawn } from 'child_process';
import * as path from 'path';

import { pgConfig } from './harness';

export interface PulledAnnouncement {
  id: string;
  lockSeq: number | string;
  leaseExpiresAt: string;
}

export interface CrashWorker {
  child: ChildProcess;
  pid: number;
  /** Resolves with the job the child leased, or rejects if it never got one. */
  pulled: Promise<PulledAnnouncement>;
  /** SIGKILL the child and wait for the OS to reap it. */
  kill(): Promise<void>;
}

const FIXTURE = path.resolve(__dirname, '..', 'fixtures', 'crash-worker.ts');

/**
 * Spawns a real OS process that leases a job and then hangs. Nothing is mocked:
 * killing it leaves exactly the state a crashed worker leaves behind — a
 * PROCESSING row with a live lease and nobody left to settle it.
 */
export function spawnCrashWorker(queueId: number | string): CrashWorker {
  const cfg = pgConfig();

  const child = spawn(
    process.execPath,
    ['-r', 'ts-node/register/transpile-only', FIXTURE],
    {
      env: {
        ...process.env,
        TS_NODE_TRANSPILE_ONLY: 'true',
        QUEUE_ID: String(queueId),
        DATABASE_HOST: cfg.host,
        DATABASE_PORT: String(cfg.port),
        DATABASE_USER: cfg.user,
        DATABASE_PASS: cfg.password,
        DATABASE_NAME: cfg.database,
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    }
  );

  let stdout = '';
  let stderr = '';

  const pulled = new Promise<PulledAnnouncement>((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`crash worker never leased a job.\nstdout: ${stdout}\nstderr: ${stderr}`)),
      30_000
    );
    timer.unref?.();

    child.stdout!.on('data', (chunk) => {
      stdout += chunk.toString();
      for (const line of stdout.split('\n')) {
        if (line.startsWith('PULLED ')) {
          clearTimeout(timer);
          resolve(JSON.parse(line.slice('PULLED '.length)));
          return;
        }
        if (line.startsWith('EMPTY') || line.startsWith('ERROR')) {
          clearTimeout(timer);
          reject(new Error(`crash worker failed: ${line}\nstderr: ${stderr}`));
          return;
        }
      }
    });

    child.stderr!.on('data', (chunk) => {
      stderr += chunk.toString();
    });

    child.on('error', (err) => {
      clearTimeout(timer);
      reject(err);
    });

    child.on('exit', (code, signal) => {
      if (signal === 'SIGKILL') return; // expected — that is the whole point
      clearTimeout(timer);
      reject(new Error(`crash worker exited early (code ${code}).\nstdout: ${stdout}\nstderr: ${stderr}`));
    });
  });

  return {
    child,
    pid: child.pid!,
    pulled,
    kill: () =>
      new Promise<void>((resolve) => {
        if (child.exitCode !== null || child.signalCode !== null) return resolve();
        child.once('exit', () => resolve());
        child.kill('SIGKILL');
      }),
  };
}
