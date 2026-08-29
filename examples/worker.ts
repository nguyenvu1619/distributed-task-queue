/**
 * Consuming jobs.
 *
 *   npm run example:worker
 *
 * Ctrl-C (or SIGTERM) drains in-flight handlers and exits.
 */
import * as dotenv from 'dotenv';
import { TaskQueue } from '../src/index';

dotenv.config();

interface EmailJob {
  to: string;
  subject: string;
}

const tq = TaskQueue.create({
  host: process.env.DATABASE_HOST || 'localhost',
  port: parseInt(process.env.DATABASE_PORT || '5432', 10),
  user: process.env.DATABASE_USER || 'user',
  password: process.env.DATABASE_PASS || 'password',
  database: process.env.DATABASE_NAME || 'queue',
});

const emails = tq.defineQueue<EmailJob>('emails', {
  concurrency: 10,
  maxAttempts: 3,
  leaseDuration: '30s',
});

async function main(): Promise<void> {
  await emails.work(
    async (email, ctx) => {
      ctx.log.info(`sending to ${email.to} (attempt ${ctx.attempt}/${ctx.maxAttempts})`);
      await new Promise((resolve) => setTimeout(resolve, 100));
      // Throwing here fails the job; it is retried until maxAttempts is spent.
    },
    { concurrency: 4, pollInterval: '250ms' }
  );

  // Recovers jobs held by workers that died mid-run. Run at least one per deployment.
  tq.startReaper({ interval: '30s' });

  console.log('worker running — Ctrl-C to stop');

  const shutdown = async (signal: string): Promise<void> => {
    console.log(`\n${signal} received, draining...`);
    await tq.close({ timeout: '20s' });
    process.exit(0);
  };

  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
