/**
 * Publishing jobs.
 *
 *   npm run example:producer
 *
 * Run `npm run example:worker` in another terminal to consume them.
 */
import * as dotenv from 'dotenv';
import { TaskQueue } from '../src/index';

dotenv.config();

export interface EmailJob {
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

// Declared once and shared. The queue is created on first use if it is missing.
export const emails = tq.defineQueue<EmailJob>('emails', {
  concurrency: 10,
  maxAttempts: 3,
  leaseDuration: '30s',
});

async function main(): Promise<void> {
  await tq.migrate();

  const one = await emails.publish({ to: 'ada@example.com', subject: 'Hello' });
  console.log(`published job ${one.id}`);

  // Same key twice returns the first job rather than enqueueing a duplicate.
  const key = 'welcome-user-42';
  await emails.publish({ to: 'grace@example.com', subject: 'Welcome' }, { idempotencyKey: key });
  const again = await emails.publish(
    { to: 'grace@example.com', subject: 'Welcome' },
    { idempotencyKey: key }
  );
  console.log(`second publish deduplicated: ${again.deduplicated}`);

  // A batch goes out in one round trip.
  const batch = await emails.publishMany(
    Array.from({ length: 5 }, (_, i) => ({
      to: `user${i}@example.com`,
      subject: `Digest #${i}`,
    }))
  );
  console.log(`published ${batch.length} more`);

  const stats = await emails.stats();
  console.log(`queue depth: ${stats.pending} pending, ${stats.processing} processing`);

  await tq.close();
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error);
    process.exit(1);
  });
}
