// Benchmarks drive the repositories and services directly, below the public
// TaskQueue facade, so they measure the storage layer rather than the client.
import { createPool } from '../src/repository/postgresql/connection';
import { JobRepository } from '../src/repository/postgresql/job.repository';
import { QueueRepository } from '../src/repository/postgresql/queue.repository';
import { JobService } from '../src/services/job.service';
import { QueueService } from '../src/services/queue.service';
import { WorkerService } from '../src/services/worker.service';
import { Job } from '../src/domain/job';
import { Pool } from 'pg';
import * as dotenv from 'dotenv';

dotenv.config();

interface BenchmarkConfig {
  numJobs: number;
  numWorkers: number;
  workerConcurrency: number;
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
  // Shared pool for queue setup and job publishing only
  private static readonly SHARED_POOL_MAX = 24;
  // Reserve a few connections for Postgres internals (autovacuum, etc.)
  private static readonly PG_RESERVED = 10;

  private pool = createPool({
    host: process.env.DATABASE_HOST || 'localhost',
    port: parseInt(process.env.DATABASE_PORT || '5432', 10),
    user: process.env.DATABASE_USER || 'user',
    password: process.env.DATABASE_PASS || 'password',
    database: process.env.DATABASE_NAME || 'queue',
    max: PerformanceTest.SHARED_POOL_MAX,
  });

  private jobRepo = new JobRepository(this.pool);
  private queueRepo = new QueueRepository(this.pool);
  private queueService = new QueueService(this.queueRepo);
  private jobService = new JobService(this.jobRepo, this.queueRepo);

  private workerPools: Pool[] = [];
  private workers: WorkerService[] = [];

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

      // Step 3: Start workers — each with its own pool and WorkerService
      console.log(`👷 Starting ${config.numWorkers} workers (concurrency=${config.workerConcurrency} each)...`);
      this.metrics.startTime = Date.now();
      this.metrics.totalJobs = config.numJobs;

      for (let i = 0; i < config.numWorkers; i++) {
        const stats = { pulled: 0, completed: 0, failed: 0 };
        this.workerStats.set(i, stats);

        // Cap per-worker pool so total connections stay under Postgres's max_connections:
        // sharedPool + reserved + numWorkers * perWorkerMax <= pgMaxConnections
        const pgMaxConnections = parseInt(process.env.PG_MAX_CONNECTIONS || '200', 10);
        const perWorkerMax = Math.max(
          Math.floor((pgMaxConnections - PerformanceTest.SHARED_POOL_MAX - PerformanceTest.PG_RESERVED) / config.numWorkers),
          1
        );
        const workerPool = createPool({
          host: process.env.DATABASE_HOST || 'localhost',
          port: parseInt(process.env.DATABASE_PORT || '5432', 10),
          user: process.env.DATABASE_USER || 'user',
          password: process.env.DATABASE_PASS || 'password',
          database: process.env.DATABASE_NAME || 'queue',
          max: perWorkerMax,
        });
        this.workerPools.push(workerPool);

        const workerJobRepo = new JobRepository(workerPool);
        const workerQueueRepo = new QueueRepository(workerPool);
        const workerJobService = new JobService(workerJobRepo, workerQueueRepo);

        const worker = new WorkerService(workerJobService, {
          queueId: queue.id,
          concurrency: config.workerConcurrency,
          pollInterval: 10,
          handler: async (job: Job, _payload: unknown) => {
            stats.pulled++;
            const startedAt = Date.now();

            await this.processJob(job, config.jobProcessingTimeMs);

            const completedAt = Date.now();
            stats.completed++;
            this.metrics.completedJobs++;

            if (!this.metrics.firstJobCompletedAt) {
              this.metrics.firstJobCompletedAt = completedAt;
            }

            this.jobMetrics.push({
              jobId: job.id,
              createdAt: job.createdAt.getTime(),
              startedAt,
              completedAt,
              latencyMs: completedAt - job.createdAt.getTime(),
            });

            if (this.metrics.completedJobs % 100 === 0) {
              const elapsed = Date.now() - this.metrics.startTime;
              const rate = (this.metrics.completedJobs / (elapsed / 1000)).toFixed(2);
              process.stdout.write(
                `   Progress: ${this.metrics.completedJobs}/${this.metrics.totalJobs} ` +
                `(${rate} jobs/sec)\r`
              );
            }
          },
        });

        this.workers.push(worker);
        worker.start();
      }

      console.log(`✅ ${config.numWorkers} workers started`);
      console.log('─'.repeat(80));

      // Wait for all jobs to complete
      await this.waitForCompletion(config.numJobs);

      // Calculate final metrics
      this.calculateFinalMetrics();

      // Print results
      this.printResults();

      // Stop all workers and close their pools
      console.log('🧹 Cleaning up...');
      await Promise.all(this.workers.map((w) => w.stop()));
      await Promise.all(this.workerPools.map((p) => p.end()));
      await this.pool.end();
      console.log('✅ Cleanup complete');

    } catch (error) {
      console.error('❌ Performance test failed:', error);
      await Promise.all(this.workers.map((w) => w.stop()));
      await Promise.all(this.workerPools.map((p) => p.end()));
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

  private async processJob(_job: Job, processingTimeMs: number): Promise<void> {
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
    numWorkers: parseInt(process.env.NUM_WORKERS || '4', 10),
    workerConcurrency: parseInt(process.env.WORKER_CONCURRENCY || '24', 10),
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

