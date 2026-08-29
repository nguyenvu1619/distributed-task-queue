import {
  createPool,
  JobService,
  QueueService,
  JobRepository,
  QueueRepository,
  Job,
} from '../src/index';
import * as dotenv from 'dotenv';

dotenv.config();

interface LatencyMetrics {
  numJobs: number;
  minLatencyMs: number;
  maxLatencyMs: number;
  averageLatencyMs: number;
  p50LatencyMs: number;
  p95LatencyMs: number;
  p99LatencyMs: number;
  measurements: number[];
}

class LatencyTest {
  private pool = createPool({
    host: process.env.DATABASE_HOST || 'localhost',
    port: parseInt(process.env.DATABASE_PORT || '5432', 10),
    user: process.env.DATABASE_USER || 'user',
    password: process.env.DATABASE_PASS || 'password',
    database: process.env.DATABASE_NAME || 'queue',
    max: 20, // Small pool for latency test
  });

  private jobRepo = new JobRepository(this.pool);
  private queueRepo = new QueueRepository(this.pool);
  private queueService = new QueueService(this.queueRepo);
  private jobService = new JobService(this.jobRepo, this.queueRepo);

  private measurements: number[] = [];

  async run() {
    const numJobs = parseInt(process.env.NUM_JOBS || '100', 10);
    const numWorkers = parseInt(process.env.NUM_WORKERS || '2', 10);

    console.log('🚀 Starting Latency Performance Test');
    console.log('═'.repeat(80));
    console.log('');
    console.log('This test measures the time between job creation and execution.');
    console.log('It uses minimal processing time to isolate queue latency.');
    console.log('');
    console.log('Configuration:');
    console.log(`  Number of Jobs:      ${numJobs}`);
    console.log(`  Number of Workers:   ${numWorkers}`);
    console.log('');
    console.log('─'.repeat(80));

    try {
      // Step 1: Create queue
      console.log('📦 Creating test queue...');
      const randomId = Math.random().toString(36).substring(2, 15);
      const queue = await this.queueService.createQueue({
        name: `latency-test-${randomId}`,
        maxAttempts: 3,
        leaseDuration: 30000,
        concurrency: 0, // No concurrency limit for maximum performance
        requiresGroupId: false, // Use fast path (single query, no transactions)
      });
      console.log(`✅ Queue created: ID=${queue.id}`);
      console.log('─'.repeat(80));

      // Step 2: Start workers first (before publishing jobs)
      console.log(`👷 Starting ${numWorkers} workers...`);
      const workerPromises: Promise<void>[] = [];
      const completedJobs = new Set<number>();
      const jobCreationTimes = new Map<number, number>();

      for (let i = 0; i < numWorkers; i++) {
        workerPromises.push(
          this.startWorker(i, queue.id, numJobs, completedJobs, jobCreationTimes)
        );
      }
      console.log(`✅ Workers started and waiting for jobs`);
      console.log('─'.repeat(80));

      // Small delay to ensure workers are ready
      await new Promise((resolve) => setTimeout(resolve, 100));

      // Step 3: Publish jobs and measure latency
      console.log(`📤 Publishing ${numJobs} jobs and measuring latency...`);
      const publishStartTime = Date.now();

      for (let i = 0; i < numJobs; i++) {
        const creationTime = Date.now();
        const randomId = Math.random().toString(36).substring(2, 15);
        
        const job = await this.jobService.publishJob({
          idempotencyKey: `latency-test-job-${creationTime}-${i}-${randomId}`,
          payload: JSON.stringify({
            jobNumber: i,
            creationTime,
          }),
          queueId: queue.id,
        });

        jobCreationTimes.set(job.id, creationTime);

        // Progress indicator
        if ((i + 1) % 10 === 0) {
          process.stdout.write(`   Published ${i + 1}/${numJobs} jobs...\r`);
        }
      }

      const publishDuration = Date.now() - publishStartTime;
      console.log(`\n✅ Published ${numJobs} jobs in ${publishDuration}ms`);
      console.log('─'.repeat(80));

      // Step 4: Wait for all jobs to complete
      console.log('⏳ Waiting for all jobs to complete...');
      await this.waitForCompletion(numJobs, completedJobs);
      console.log('✅ All jobs completed');
      console.log('─'.repeat(80));

      // Step 5: Calculate and print results
      const metrics = this.calculateMetrics();
      this.printResults(metrics);

      // Cleanup
      console.log('🧹 Cleaning up...');
      await this.pool.end();
      console.log('✅ Cleanup complete');

    } catch (error) {
      console.error('❌ Latency test failed:', error);
      await this.pool.end();
      throw error;
    }
  }

  private async startWorker(
    workerId: number,
    queueId: number,
    totalJobs: number,
    completedJobs: Set<number>,
    jobCreationTimes: Map<number, number>
  ): Promise<void> {
    while (completedJobs.size < totalJobs) {
      try {
        // Pull a job
        const job = await this.jobService.pullJob(queueId);
        if (!job) {
          // No jobs available, wait a bit
          await new Promise((resolve) => setTimeout(resolve, 5));
          continue;
        }

        const executionStartTime = Date.now();
        const creationTime = jobCreationTimes.get(job.id);

        if (creationTime) {
          // Calculate latency: time from job creation to execution
          const latency = executionStartTime - creationTime;
          this.measurements.push(latency);
        }

        // Minimal processing (just complete the job)
        await this.jobService.completeJob(job.id, job.lockSeq!);
        completedJobs.add(job.id);

        // Progress indicator
        if (completedJobs.size % 10 === 0) {
          process.stdout.write(`   Completed ${completedJobs.size}/${totalJobs} jobs...\r`);
        }
      } catch (error) {
        // Error pulling or processing job, continue
        await new Promise((resolve) => setTimeout(resolve, 5));
      }
    }
  }

  private async waitForCompletion(
    totalJobs: number,
    completedJobs: Set<number>
  ): Promise<void> {
    return new Promise((resolve) => {
      const checkInterval = setInterval(() => {
        if (completedJobs.size >= totalJobs) {
          clearInterval(checkInterval);
          console.log(); // New line after progress indicator
          resolve();
        }
      }, 100);
    });
  }

  private calculateMetrics(): LatencyMetrics {
    const sorted = [...this.measurements].sort((a, b) => a - b);
    
    return {
      numJobs: this.measurements.length,
      minLatencyMs: sorted[0] || 0,
      maxLatencyMs: sorted[sorted.length - 1] || 0,
      averageLatencyMs: sorted.reduce((sum, lat) => sum + lat, 0) / sorted.length,
      p50LatencyMs: sorted[Math.floor(sorted.length * 0.5)] || 0,
      p95LatencyMs: sorted[Math.floor(sorted.length * 0.95)] || 0,
      p99LatencyMs: sorted[Math.floor(sorted.length * 0.99)] || 0,
      measurements: sorted,
    };
  }

  private printResults(metrics: LatencyMetrics) {
    console.log('');
    console.log('═'.repeat(80));
    console.log('📊 LATENCY TEST RESULTS');
    console.log('═'.repeat(80));
    console.log('');

    console.log('Job Statistics:');
    console.log(`  Total Jobs Measured: ${metrics.numJobs}`);
    console.log('');

    console.log('Latency (time from job creation to execution):');
    console.log(`  Min:                 ${metrics.minLatencyMs.toFixed(2)}ms`);
    console.log(`  Max:                 ${metrics.maxLatencyMs.toFixed(2)}ms`);
    console.log(`  Average:             ${metrics.averageLatencyMs.toFixed(2)}ms`);
    console.log(`  P50 (Median):        ${metrics.p50LatencyMs.toFixed(2)}ms`);
    console.log(`  P95:                 ${metrics.p95LatencyMs.toFixed(2)}ms`);
    console.log(`  P99:                 ${metrics.p99LatencyMs.toFixed(2)}ms`);
    console.log('');

    // Comparison with Graphile Worker
    console.log('Reference (Graphile Worker on AMD Ryzen 3900):');
    console.log(`  Min:                 2.39ms`);
    console.log(`  Max:                 12.09ms`);
    console.log(`  Average:             2.66ms`);
    console.log('');

    // Performance assessment
    const performanceRating = this.getPerformanceRating(metrics);
    console.log(`Performance Rating:    ${performanceRating.emoji} ${performanceRating.text}`);
    console.log('');

    // Distribution analysis
    console.log('Latency Distribution:');
    this.printDistribution(metrics.measurements);
    console.log('');
    console.log('═'.repeat(80));
  }

  private getPerformanceRating(metrics: LatencyMetrics): { emoji: string; text: string } {
    if (metrics.averageLatencyMs < 5) {
      return { emoji: '🚀', text: 'Excellent (< 5ms avg)' };
    } else if (metrics.averageLatencyMs < 10) {
      return { emoji: '✅', text: 'Good (< 10ms avg)' };
    } else if (metrics.averageLatencyMs < 20) {
      return { emoji: '⚠️', text: 'Fair (< 20ms avg)' };
    } else {
      return { emoji: '❌', text: 'Needs Optimization (> 20ms avg)' };
    }
  }

  private printDistribution(measurements: number[]) {
    const buckets = [
      { label: '< 5ms', max: 5 },
      { label: '5-10ms', max: 10 },
      { label: '10-20ms', max: 20 },
      { label: '20-50ms', max: 50 },
      { label: '50-100ms', max: 100 },
      { label: '> 100ms', max: Infinity },
    ];

    const counts = buckets.map(() => 0);
    measurements.forEach((latency) => {
      for (let i = 0; i < buckets.length; i++) {
        if (latency < buckets[i].max || i === buckets.length - 1) {
          counts[i]++;
          break;
        }
      }
    });

    const total = measurements.length;
    buckets.forEach((bucket, i) => {
      const count = counts[i];
      const percentage = ((count / total) * 100).toFixed(1);
      const barLength = Math.floor((count / total) * 40);
      const bar = '█'.repeat(barLength);
      console.log(`  ${bucket.label.padEnd(12)} ${bar} ${count} (${percentage}%)`);
    });
  }
}

// Main execution
async function main() {
  const test = new LatencyTest();
  await test.run();
  process.exit(0);
}

main().catch((error) => {
  console.error('Fatal error:', error);
  process.exit(1);
});
