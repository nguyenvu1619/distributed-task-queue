/**
 * Publishing inside your own transaction — the reason to back a queue with the
 * same database your data lives in.
 *
 *   npx ts-node examples/transactional-publish.ts
 *
 * The job row and the business row commit together or not at all, so there is
 * no window where one exists without the other. No outbox table, no relay
 * process, no dual-write to reconcile.
 */
import * as dotenv from 'dotenv';
import { TaskQueue } from '../src/index';

dotenv.config();

const tq = TaskQueue.create({
  host: process.env.DATABASE_HOST || 'localhost',
  port: parseInt(process.env.DATABASE_PORT || '5432', 10),
  user: process.env.DATABASE_USER || 'user',
  password: process.env.DATABASE_PASS || 'password',
  database: process.env.DATABASE_NAME || 'queue',
});

const confirmations = tq.defineQueue<{ orderId: string }>('order-confirmations');

async function countJobs(): Promise<number> {
  const queueId = await confirmations.id();
  const { rows } = await tq.pool.query(
    'SELECT count(*)::int AS n FROM jobs WHERE queue_id = $1',
    [queueId]
  );
  return rows[0].n;
}

async function main(): Promise<void> {
  await tq.migrate();
  await tq.pool.query(
    'CREATE TABLE IF NOT EXISTS demo_orders (id TEXT PRIMARY KEY, total NUMERIC NOT NULL)'
  );
  await tq.pool.query('TRUNCATE demo_orders');

  // ---- committed ---------------------------------------------------------
  const good = `order-${Date.now()}`;
  await tq.transaction(async (tx) => {
    await tx.query('INSERT INTO demo_orders (id, total) VALUES ($1, $2)', [good, 42.0]);
    await confirmations.publish({ orderId: good }, { tx, idempotencyKey: `confirm-${good}` });
  });

  console.log('after commit:');
  console.log(`  orders: ${(await tq.pool.query('SELECT * FROM demo_orders')).rowCount}`);
  console.log(`  jobs:   ${await countJobs()}`);

  // ---- rolled back -------------------------------------------------------
  const bad = `order-${Date.now()}-bad`;
  try {
    await tq.transaction(async (tx) => {
      await tx.query('INSERT INTO demo_orders (id, total) VALUES ($1, $2)', [bad, 99.0]);
      await confirmations.publish({ orderId: bad }, { tx, idempotencyKey: `confirm-${bad}` });
      throw new Error('payment declined');
    });
  } catch (error) {
    console.log(`\nrolled back: ${(error as Error).message}`);
  }

  console.log('after rollback:');
  console.log(`  orders: ${(await tq.pool.query('SELECT * FROM demo_orders')).rowCount} (unchanged)`);
  console.log(`  jobs:   ${await countJobs()} (unchanged — the job never existed)`);

  await tq.pool.query('DROP TABLE demo_orders');
  await tq.close();
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
