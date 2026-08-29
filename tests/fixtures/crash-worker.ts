/**
 * A worker that pulls exactly one job, announces it on stdout, and then hangs
 * forever so the parent test can SIGKILL it mid-flight.
 *
 * Contract with the parent:
 *   stdout `PULLED <json>`  — a job was leased; {id, lockSeq, leaseExpiresAt}
 *   stdout `EMPTY`          — no job became available before the timeout
 *   stdout `ERROR <text>`   — something went wrong
 *
 * It must never settle the job and must never exit on its own.
 */
import { createPool } from '../../src/repository/postgresql/connection';
import { JobRepository } from '../../src/repository/postgresql/job.repository';
import { QueueRepository } from '../../src/repository/postgresql/queue.repository';

const say = (line: string) => process.stdout.write(`${line}\n`);

async function main(): Promise<void> {
  const queueId = parseInt(process.env.QUEUE_ID || '', 10);
  if (!Number.isFinite(queueId)) throw new Error('QUEUE_ID is required');

  const pool = createPool({
    host: process.env.DATABASE_HOST || 'localhost',
    port: parseInt(process.env.DATABASE_PORT || '5432', 10),
    user: process.env.DATABASE_USER || 'user',
    password: process.env.DATABASE_PASS || 'password',
    database: process.env.DATABASE_NAME || 'queue',
    max: 2,
  });

  const jobRepo = new JobRepository(pool);
  const queue = await new QueueRepository(pool).getById(queueId);

  const deadline = Date.now() + 20_000;
  while (Date.now() < deadline) {
    const job = await jobRepo.pullJob(queue);
    if (job) {
      say(
        `PULLED ${JSON.stringify({
          id: String(job.id),
          lockSeq: job.lockSeq,
          leaseExpiresAt: job.leaseExpiresAt,
        })}`
      );
      // Hold the lease and never release it. The parent will SIGKILL us.
      setInterval(() => undefined, 1_000);
      return;
    }
    await new Promise((r) => setTimeout(r, 25));
  }

  say('EMPTY');
  await pool.end();
}

main().catch((err) => {
  say(`ERROR ${err instanceof Error ? err.message : String(err)}`);
  process.exit(1);
});
