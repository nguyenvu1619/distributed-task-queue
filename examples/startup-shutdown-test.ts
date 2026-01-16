import {
  createPool,
  JobService,
  QueueService,
  JobRepository,
  QueueRepository,
} from '../src/index';
import * as dotenv from 'dotenv';

dotenv.config();

interface StartupShutdownMetrics {
  startupTimeMs: number;
  shutdownTimeMs: number;
  totalTimeMs: number;
  connectionPoolInitMs: number;
  workerInitMs: number;
}

class StartupShutdownTest {
  private metrics: StartupShutdownMetrics = {
    startupTimeMs: 0,
    shutdownTimeMs: 0,
    totalTimeMs: 0,
    connectionPoolInitMs: 0,
    workerInitMs: 0,
  };

  async run() {
    console.log('🚀 Starting Startup/Shutdown Performance Test');
    console.log('═'.repeat(80));
    console.log('');
    console.log('This test measures the time taken to:');
    console.log('  1. Initialize database connection pool');
    console.log('  2. Set up worker infrastructure');
    console.log('  3. Gracefully shutdown all resources');
    console.log('');
    console.log('─'.repeat(80));

    const totalStartTime = Date.now();

    try {
      // Step 1: Measure connection pool initialization
      console.log('📊 Measuring connection pool initialization...');
      const poolStartTime = Date.now();
      
      const pool = createPool({
        host: process.env.DATABASE_HOST || 'localhost',
        port: parseInt(process.env.DATABASE_PORT || '5432', 10),
        user: process.env.DATABASE_USER || 'user',
        password: process.env.DATABASE_PASS || 'password',
        database: process.env.DATABASE_NAME || 'queue',
        max: 10, // Minimal pool for startup test
      });

      // Test connection
      await pool.query('SELECT 1');
      
      this.metrics.connectionPoolInitMs = Date.now() - poolStartTime;
      console.log(`✅ Connection pool initialized in ${this.metrics.connectionPoolInitMs}ms`);
      console.log('─'.repeat(80));

      // Step 2: Measure worker infrastructure setup
      console.log('👷 Measuring worker infrastructure setup...');
      const workerStartTime = Date.now();

      const jobRepo = new JobRepository(pool);
      const queueRepo = new QueueRepository(pool);
      const queueService = new QueueService(queueRepo);
      const jobService = new JobService(jobRepo, queueRepo);

      // Create a test queue
      const randomId = Math.random().toString(36).substring(2, 15);
      const queue = await queueService.createQueue({
        name: `startup-test-${randomId}`,
        maxAttempts: 3,
        leaseDuration: 30000,
        concurrency: 0,
        requiresGroupId: false, // Use fast path (single query, no transactions)
      });

      // Simulate worker initialization (without actually processing jobs)
      const numWorkers = 5;
      const workers: Promise<void>[] = [];
      
      for (let i = 0; i < numWorkers; i++) {
        workers.push(this.initializeWorker(i, jobService, queue.id));
      }

      await Promise.all(workers);

      this.metrics.workerInitMs = Date.now() - workerStartTime;
      console.log(`✅ Worker infrastructure initialized in ${this.metrics.workerInitMs}ms`);
      console.log(`   (${numWorkers} workers ready)`);
      console.log('─'.repeat(80));

      this.metrics.startupTimeMs = Date.now() - totalStartTime;

      // Step 3: Measure shutdown
      console.log('🧹 Measuring graceful shutdown...');
      const shutdownStartTime = Date.now();

      // Clean up test queue
      await pool.query('DELETE FROM jobs WHERE queue_id = $1', [queue.id]);
      await pool.query('DELETE FROM queues WHERE id = $1', [queue.id]);

      // Close connection pool
      await pool.end();

      this.metrics.shutdownTimeMs = Date.now() - shutdownStartTime;
      console.log(`✅ Shutdown completed in ${this.metrics.shutdownTimeMs}ms`);
      console.log('─'.repeat(80));

      this.metrics.totalTimeMs = Date.now() - totalStartTime;

      // Print results
      this.printResults();

    } catch (error) {
      console.error('❌ Startup/Shutdown test failed:', error);
      throw error;
    }
  }

  private async initializeWorker(
    workerId: number,
    jobService: JobService,
    queueId: number
  ): Promise<void> {
    // Simulate worker initialization by attempting to pull a job
    // This tests the full worker setup path without actually processing jobs
    try {
      await jobService.pullJob(queueId);
    } catch (error) {
      // Expected - no jobs available, but worker is initialized
    }
  }

  private printResults() {
    console.log('');
    console.log('═'.repeat(80));
    console.log('📊 STARTUP/SHUTDOWN TEST RESULTS');
    console.log('═'.repeat(80));
    console.log('');

    console.log('Initialization Times:');
    console.log(`  Connection Pool:     ${this.metrics.connectionPoolInitMs}ms`);
    console.log(`  Worker Infrastructure: ${this.metrics.workerInitMs}ms`);
    console.log(`  Total Startup:       ${this.metrics.startupTimeMs}ms`);
    console.log('');

    console.log('Shutdown Time:');
    console.log(`  Graceful Shutdown:   ${this.metrics.shutdownTimeMs}ms`);
    console.log('');

    console.log('Overall Performance:');
    console.log(`  Total Time:          ${this.metrics.totalTimeMs}ms`);
    console.log('');

    // Comparison with Graphile Worker
    console.log('Reference (Graphile Worker on AMD Ryzen 3900):');
    console.log(`  Startup/Shutdown:    110ms`);
    console.log('');

    // Performance assessment
    const performanceRating = this.getPerformanceRating();
    console.log(`Performance Rating:    ${performanceRating.emoji} ${performanceRating.text}`);
    console.log('');
    console.log('═'.repeat(80));
  }

  private getPerformanceRating(): { emoji: string; text: string } {
    if (this.metrics.totalTimeMs < 150) {
      return { emoji: '🚀', text: 'Excellent (< 150ms)' };
    } else if (this.metrics.totalTimeMs < 300) {
      return { emoji: '✅', text: 'Good (< 300ms)' };
    } else if (this.metrics.totalTimeMs < 500) {
      return { emoji: '⚠️', text: 'Fair (< 500ms)' };
    } else {
      return { emoji: '❌', text: 'Needs Optimization (> 500ms)' };
    }
  }
}

// Main execution
async function main() {
  const test = new StartupShutdownTest();
  await test.run();
  process.exit(0);
}

main().catch((error) => {
  console.error('Fatal error:', error);
  process.exit(1);
});
