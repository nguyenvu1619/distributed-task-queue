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

interface BenchmarkConfig {
  numJobs: number;
  numWorkers: number;
  jobProcessingTimeMs: number;
}

interface BenchmarkMetrics {
  totalJobs: number;
  completedJobs: number;
  failedJobs: number;
  startTime: number;
  endTime?: number;
  firstJobCompletedAt?: number;
  totalDurationMs?: number;
  jobsPerSecond?: number;
  averageLatencyMs?: number;
  minLatencyMs?: number;
  maxLatencyMs?: number;
  p50LatencyMs?: number;
  p95LatencyMs?: number;
  p99LatencyMs?: number;
}

interface JobMetrics {
  jobId: number;
  createdAt: number;
  startedAt: number;
  completedAt: number;
  latencyMs: number;
}

class PerformanceTest {
  private pool = createPool({
    host: process.env.DATABASE_HOST || 'localhost',
    port: parseInt(process.env.DATABASE_PORT || '5432', 10),
    user: process.env.DATABASE_USER || 'user',
    password: process.env.DATABASE_PASS || 'password',
    database: process.env.DATABASE_NAME || 'queue',
    max: 150, // Increase connection pool for 100 workers + overhead
  });

  private jobRepo = new JobRepository(this.pool);
  private queueRepo = new QueueRepository(this.pool);
  private queueService = new QueueService(this.queueRepo);
  private jobService = new JobService(this.jobRepo, this.queueRepo);

  private metrics: BenchmarkMetrics = {
    totalJobs: 0,
    completedJobs: 0,
    failedJobs: 0,
    startTime: 0,
  };

  private jobMetrics: JobMetrics[] = [];
  private workerStats: Map<number, { pulled: number; completed: number; failed: number }> = new Map();

  async run(config: BenchmarkConfig) {
    console.log('🚀 Starting Performance Test');
    console.log('Configuration:', config);
    console.log('─'.repeat(80));

    try {
      // Step 1: Create queue
      console.log('📦 Creating queue...');
      const randomId = Math.random().toString(36).substring(2, 15);
      const queue = await this.queueService.createQueue({
        name: `perf-test-${randomId}`,
        maxAttempts: 3,
        leaseDuration: 30000, // 30 seconds
        concurrency: 0, // No concurrency limit for maximum performance
        requiresGroupId: false, // Use fast path (single query, no transactions)
      });
      console.log(`✅ Queue created: ID=${queue.id}`);
      console.log('─'.repeat(80));

      // Step 2: Publish jobs
      console.log(`📤 Publishing ${config.numJobs} jobs...`);
      const publishStartTime = Date.now();
      await this.publishJobs(queue.id, config.numJobs);
      const publishDuration = Date.now() - publishStartTime;
      console.log(`✅ Published ${config.numJobs} jobs in ${publishDuration}ms`);
      console.log(`   Publishing rate: ${(config.numJobs / (publishDuration / 1000)).toFixed(2)} jobs/sec`);
      console.log('─'.repeat(80));

      // Step 3: Start workers
      console.log(`👷 Starting ${config.numWorkers} workers...`);
      this.metrics.startTime = Date.now();
      this.metrics.totalJobs = config.numJobs;

      const workerPromises: Promise<void>[] = [];
      for (let i = 0; i < config.numWorkers; i++) {
        this.workerStats.set(i, { pulled: 0, completed: 0, failed: 0 });
        workerPromises.push(this.startWorker(i, queue.id, config.jobProcessingTimeMs));
      }

      console.log(`✅ ${config.numWorkers} workers started`);
      console.log('─'.repeat(80));

      // Wait for all jobs to complete
      await this.waitForCompletion(config.numJobs);

      // Calculate final metrics
      this.calculateFinalMetrics();

      // Print results
      this.printResults();

      // Cleanup: Stop workers (they'll exit naturally once no jobs are found)
      console.log('🧹 Cleaning up...');
      await this.pool.end();
      console.log('✅ Cleanup complete');

    } catch (error) {
      console.error('❌ Performance test failed:', error);
      await this.pool.end();
      throw error;
    }
  }

  private async publishJobs(queueId: number, numJobs: number): Promise<void> {
    const batchSize = 5;
    const promises: Promise<Job>[] = [];

    for (let i = 0; i < numJobs; i++) {
      const randomId = Math.random().toString(36).substring(2, 15);
      const jobPromise = this.jobService.publishJob({
        idempotencyKey: `perf-test-job-${Date.now()}-${i}-${randomId}`,
        payload: JSON.stringify({
          jobNumber: i,
          timestamp: Date.now(),
          data: `Sample data for job ${i}`,
        }),
        
        queueId,
      });

      promises.push(jobPromise);

      // Process in batches to avoid overwhelming the database
      if (promises.length >= batchSize || i === numJobs - 1) {
        await Promise.all(promises);
        promises.length = 0;
        
        // Progress indicator
        if ((i + 1) % 1000 === 0) {
          process.stdout.write(`   Published ${i + 1}/${numJobs} jobs...\r`);
        }
      }
    }
    console.log(); // New line after progress indicator
  }

  private async startWorker(
    workerId: number,
    queueId: number,
    processingTimeMs: number
  ): Promise<void> {
    while (this.metrics.completedJobs + this.metrics.failedJobs < this.metrics.totalJobs) {
      try {
        // Pull a job
        const job = await this.jobService.pullJob(queueId);
        if (!job) {
          // No jobs available, wait a bit
          await new Promise((resolve) => setTimeout(resolve, 10));
          continue;
        }

        const stats = this.workerStats.get(workerId)!;
        stats.pulled++;

        const startedAt = Date.now();

        try {
          // Simulate job processing
          await this.processJob(job, processingTimeMs);

          // Mark job as completed
          await this.jobService.completeJob(job.id, job.lockSeq!);

          const completedAt = Date.now();
          stats.completed++;
          this.metrics.completedJobs++;

          if (!this.metrics.firstJobCompletedAt) {
            this.metrics.firstJobCompletedAt = completedAt;
          }

          // Record job metrics
          this.jobMetrics.push({
            jobId: job.id,
            createdAt: job.createdAt.getTime(),
            startedAt,
            completedAt,
            latencyMs: completedAt - job.createdAt.getTime(),
          });

          // Progress indicator
          if (this.metrics.completedJobs % 100 === 0) {
            const elapsed = Date.now() - this.metrics.startTime;
            const rate = (this.metrics.completedJobs / (elapsed / 1000)).toFixed(2);
            process.stdout.write(
              `   Progress: ${this.metrics.completedJobs}/${this.metrics.totalJobs} ` +
              `(${rate} jobs/sec)\r`
            );
          }
        } catch (error) {
          console.error(`Worker ${workerId} failed to process job ${job.id}:`, error);
          try {
            await this.jobService.failJob(job.id, job.lockSeq!);
            stats.failed++;
            this.metrics.failedJobs++;
          } catch (failError) {
            console.error(`Worker ${workerId} failed to mark job ${job.id} as failed:`, failError);
          }
        }
      } catch (error) {
        // Error pulling job, continue
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
    }
  }

  private async processJob(job: Job, processingTimeMs: number): Promise<void> {
    // Simulate job processing
    if (processingTimeMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, processingTimeMs));
    }
  }

  private async waitForCompletion(totalJobs: number): Promise<void> {
    return new Promise((resolve) => {
      const checkInterval = setInterval(() => {
        if (this.metrics.completedJobs + this.metrics.failedJobs >= totalJobs) {
          clearInterval(checkInterval);
          console.log(); // New line after progress indicator
          resolve();
        }
      }, 100);
    });
  }

  private calculateFinalMetrics() {
    this.metrics.endTime = Date.now();
    this.metrics.totalDurationMs = this.metrics.endTime - this.metrics.startTime;
    this.metrics.jobsPerSecond = this.metrics.completedJobs / (this.metrics.totalDurationMs / 1000);

    // Calculate latency statistics
    if (this.jobMetrics.length > 0) {
      const latencies = this.jobMetrics.map((m) => m.latencyMs).sort((a, b) => a - b);
      this.metrics.averageLatencyMs =
        latencies.reduce((sum, lat) => sum + lat, 0) / latencies.length;
      this.metrics.minLatencyMs = latencies[0];
      this.metrics.maxLatencyMs = latencies[latencies.length - 1];
      this.metrics.p50LatencyMs = latencies[Math.floor(latencies.length * 0.5)];
      this.metrics.p95LatencyMs = latencies[Math.floor(latencies.length * 0.95)];
      this.metrics.p99LatencyMs = latencies[Math.floor(latencies.length * 0.99)];
    }
  }

  private printResults() {
    console.log('');
    console.log('═'.repeat(80));
    console.log('📊 PERFORMANCE TEST RESULTS');
    console.log('═'.repeat(80));
    console.log('');

    console.log('Job Statistics:');
    console.log(`  Total Jobs:      ${this.metrics.totalJobs}`);
    console.log(`  Completed Jobs:  ${this.metrics.completedJobs} ✅`);
    console.log(`  Failed Jobs:     ${this.metrics.failedJobs} ❌`);
    console.log(`  Success Rate:    ${((this.metrics.completedJobs / this.metrics.totalJobs) * 100).toFixed(2)}%`);
    console.log('');

    console.log('Time Statistics:');
    console.log(`  Total Duration:  ${this.metrics.totalDurationMs}ms (${(this.metrics.totalDurationMs! / 1000).toFixed(2)}s)`);
    if (this.metrics.firstJobCompletedAt) {
      const timeToFirstJob = this.metrics.firstJobCompletedAt - this.metrics.startTime;
      console.log(`  Time to First Job: ${timeToFirstJob}ms`);
    }
    console.log('');

    console.log('Throughput:');
    console.log(`  Jobs/Second:     ${this.metrics.jobsPerSecond?.toFixed(2)}`);
    console.log(`  Jobs/Minute:     ${(this.metrics.jobsPerSecond! * 60).toFixed(2)}`);
    console.log('');

    console.log('Latency (time from job creation to completion):');
    console.log(`  Min:             ${this.metrics.minLatencyMs?.toFixed(2)}ms`);
    console.log(`  Max:             ${this.metrics.maxLatencyMs?.toFixed(2)}ms`);
    console.log(`  Average:         ${this.metrics.averageLatencyMs?.toFixed(2)}ms`);
    console.log(`  P50 (Median):    ${this.metrics.p50LatencyMs}ms`);
    console.log(`  P95:             ${this.metrics.p95LatencyMs}ms`);
    console.log(`  P99:             ${this.metrics.p99LatencyMs}ms`);
    console.log('');

    console.log('Worker Statistics:');
    let totalPulled = 0;
    let totalCompleted = 0;
    let totalFailed = 0;
    this.workerStats.forEach((stats) => {
      totalPulled += stats.pulled;
      totalCompleted += stats.completed;
      totalFailed += stats.failed;
    });
    console.log(`  Total Pulls:     ${totalPulled}`);
    console.log(`  Avg per Worker:  ${(totalPulled / this.workerStats.size).toFixed(2)}`);
    console.log(`  Most Active:     ${Math.max(...Array.from(this.workerStats.values()).map(s => s.pulled))} pulls`);
    console.log(`  Least Active:    ${Math.min(...Array.from(this.workerStats.values()).map(s => s.pulled))} pulls`);
    console.log('');
    console.log('═'.repeat(80));
  }
}

// Main execution
async function main() {
  const config: BenchmarkConfig = {
    numJobs: parseInt(process.env.NUM_JOBS || '100000', 10),
    numWorkers: parseInt(process.env.NUM_WORKERS || '1000', 10),
    jobProcessingTimeMs: parseInt(process.env.JOB_PROCESSING_TIME_MS || '0', 10),
  };

  const test = new PerformanceTest();
  await test.run(config);
  process.exit(0);
}

main().catch((error) => {
  console.error('Fatal error:', error);
  process.exit(1);
});

