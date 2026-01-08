import { createPool, JobService, QueueService, JobRepository, QueueRepository } from '../src/index';
import * as dotenv from 'dotenv';

dotenv.config();

async function main() {
  // Create database connection pool
  const pool = createPool({
    host: process.env.DATABASE_HOST || 'localhost',
    port: parseInt(process.env.DATABASE_PORT || '5432', 10),
    user: process.env.DATABASE_USER || 'user',
    password: process.env.DATABASE_PASS || 'password',
    database: process.env.DATABASE_NAME || 'queue',
  });

  // Initialize repositories and services
  const jobRepo = new JobRepository(pool);
  const queueRepo = new QueueRepository(pool);
  const queueService = new QueueService(queueRepo);
  const jobService = new JobService(jobRepo, queueRepo);

  try {
    // Create a queue (or get existing one)
    let queue;
    try {
      queue = await queueService.getQueue(1);
      console.log('Using existing queue:', queue.name);
    } catch (error) {
      console.log('Creating new queue...');
      queue = await queueService.createQueue({
        name: 'example-queue',
        maxAttempts: 3,
        leaseDuration: 30000, // 30 seconds in milliseconds
        concurrency: 10,
      });
      console.log('Queue created:', queue);
    }

    // Publish a job
    const job = await jobService.publishJob({
      idempotencyKey: `job-${Date.now()}`,
      payload: JSON.stringify({
        task: 'process-data',
        data: { userId: 123, action: 'send-email' },
      }),
      queueId: queue.id,
      groupId: 'group-123',
      metadata: {
        consumerId: 'producer-1',
      },
    });

    console.log('Job published:', job);
  } catch (error) {
    console.error('Error:', error);
  } finally {
    await pool.end();
  }
}

main();

