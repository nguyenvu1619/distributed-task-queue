import { createPool, JobService, QueueService, JobRepository, QueueRepository } from '../src/index';
import * as dotenv from 'dotenv';

dotenv.config();

async function processJob(payload: any): Promise<void> {
  // Simulate job processing
  console.log('Processing job with payload:', payload);
  await new Promise((resolve) => setTimeout(resolve, 1000)); // Simulate work
  console.log('Job processed successfully');
}

async function workerLoop(queueId: number) {
  const pool = createPool({
    host: process.env.DATABASE_HOST || 'localhost',
    port: parseInt(process.env.DATABASE_PORT || '5432', 10),
    user: process.env.DATABASE_USER || 'user',
    password: process.env.DATABASE_PASS || 'password',
    database: process.env.DATABASE_NAME || 'queue',
  });

  const jobRepo = new JobRepository(pool);
  const queueRepo = new QueueRepository(pool);
  const queueService = new QueueService(queueRepo);
  const jobService = new JobService(jobRepo, queueRepo);

  console.log(`Worker started for queue ${queueId}`);

  while (true) {
    try {
      // Pull a job from the queue
      const job = await jobService.pullJob(queueId);

      if (!job) {
        // No jobs available, wait a bit before trying again
        await new Promise((resolve) => setTimeout(resolve, 1000));
        continue;
      }

      try {
        // Process the job
        const payload = JSON.parse(job.payload);
        console.log(`[Worker] Processing job ${job.id}:`, payload);

        await processJob(payload);

        // Mark job as completed
        await jobService.completeJob(job.id, job.lockToken);
        console.log(`[Worker] Job ${job.id} completed`);
      } catch (error) {
        console.error(`[Worker] Job ${job.id} processing failed:`, error);
        // Mark job as failed
        try {
          await jobService.failJob(job.id, job.lockToken);
          console.log(`[Worker] Job ${job.id} marked as failed`);
        } catch (failError) {
          console.error(`[Worker] Failed to mark job ${job.id} as failed:`, failError);
        }
      }
    } catch (error) {
      console.error('[Worker] Error:', error);
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
  }
}

// Start worker for queue ID 1
const queueId = parseInt(process.argv[2] || '1', 10);
workerLoop(queueId).catch(console.error);

