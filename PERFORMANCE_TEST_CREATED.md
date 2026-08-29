# Performance Testing Framework - Implementation Summary

## Overview

A comprehensive performance testing framework has been created for the distributed task queue system. This framework allows you to benchmark throughput, latency, concurrency handling, and worker distribution.

## Files Created

### 1. Main Performance Test
**File**: `examples/performance-test.ts`

A complete performance testing suite that:
- Creates a test queue with configurable concurrency
- Publishes a large number of jobs in batches
- Spawns multiple concurrent workers
- Tracks detailed metrics (throughput, latency percentiles, worker stats)
- Provides real-time progress updates
- Outputs comprehensive results

**Features**:
- Configurable via environment variables
- Automatic cleanup and connection management
- Detailed latency analysis (P50, P95, P99)
- Worker distribution statistics
- Job-by-job metrics tracking

### 2. NPM Scripts
**File**: `package.json` (updated)

Added convenient scripts:
```bash
npm run perf:test       # Default: 10k jobs, 100 workers
npm run perf:small      # Small: 1k jobs, 10 workers
npm run perf:medium     # Medium: 5k jobs, 50 workers
npm run perf:large      # Large: 10k jobs, 100 workers
npm run perf:extreme    # Extreme: 50k jobs, 200 workers
```

### 3. Documentation

**`examples/PERFORMANCE_TESTING.md`**
- Complete guide to running performance tests
- Configuration parameter explanations
- Understanding test results
- Performance tips and optimization strategies
- Troubleshooting common issues

**`examples/PERFORMANCE_SUMMARY.md`**
- Quick reference guide
- Test scenarios explained
- Expected performance benchmarks
- Optimization tips
- Cleanup procedures

**`README.md`** (updated)
- Added Performance Testing section
- Quick start commands
- Reference to detailed documentation

### 4. Helper Scripts

**`examples/quick-perf-test.sh`**
- Interactive script with database connection check
- Configurable via environment variables
- User-friendly output

**`examples/run-benchmark-suite.sh`**
- Runs multiple test scenarios automatically
- Saves results to timestamped file
- Generates summary comparison
- Includes 5 predefined test scenarios

### 5. Analysis Tools

**`examples/analyze-performance.sql`**
- 20+ SQL queries for performance analysis
- Job statistics and distribution
- Queue and shard utilization
- Latency percentiles from database
- Real-time monitoring queries
- System health checks
- Cleanup queries

## How to Use

### Quick Start

1. **Setup database**:
   ```bash
   docker-compose up -d postgres
   npm run migrate:up
   ```

2. **Run default test**:
   ```bash
   npm run perf:test
   ```

3. **View results** in the console output

### Custom Configuration

```bash
NUM_JOBS=10000 \
NUM_WORKERS=100 \
QUEUE_CONCURRENCY=1000 \
JOB_PROCESSING_TIME_MS=0 \
npm run perf:test
```

### Run Complete Benchmark Suite

```bash
./examples/run-benchmark-suite.sh
```

This runs 5 different test scenarios and saves results to a timestamped file.

## Key Metrics Tracked

### Throughput
- Jobs per second
- Jobs per minute
- Publishing rate

### Latency (End-to-End)
- Average latency
- P50 (Median)
- P95 (95th percentile)
- P99 (99th percentile)
- Min/Max latency

### Job Statistics
- Total jobs
- Completed jobs
- Failed jobs
- Success rate
- Time to first job

### Worker Statistics
- Total pulls across all workers
- Average pulls per worker
- Most/least active worker
- Work distribution balance

### System Statistics
- Database connection usage
- Queue shard utilization
- Group concurrency limits
- Processing time distribution

## Example Output

```
🚀 Starting Performance Test
Configuration: {
  numJobs: 10000,
  numWorkers: 100,
  queueConcurrency: 1000,
  jobProcessingTimeMs: 0
}
────────────────────────────────────────────────────────
📦 Creating queue...
✅ Queue created: ID=1, Concurrency=1000
────────────────────────────────────────────────────────
📤 Publishing 10000 jobs...
✅ Published 10000 jobs in 5432ms
   Publishing rate: 1841.62 jobs/sec
────────────────────────────────────────────────────────
👷 Starting 100 workers...
✅ 100 workers started
────────────────────────────────────────────────────────

════════════════════════════════════════════════════════
📊 PERFORMANCE TEST RESULTS
════════════════════════════════════════════════════════

Job Statistics:
  Total Jobs:      10000
  Completed Jobs:  10000 ✅
  Failed Jobs:     0 ❌
  Success Rate:    100.00%

Time Statistics:
  Total Duration:  11207ms (11.21s)
  Time to First Job: 142ms

Throughput:
  Jobs/Second:     892.31
  Jobs/Minute:     53538.60

Latency (time from job creation to completion):
  Average:         6234.56ms
  P50 (Median):    6152ms
  P95:             7890ms
  P99:             8234ms

Worker Statistics:
  Total Pulls:     10000
  Avg per Worker:  100.00
  Most Active:     156 pulls
  Least Active:    67 pulls

════════════════════════════════════════════════════════
```

## Configuration Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `NUM_JOBS` | 10000 | Number of jobs to publish |
| `NUM_WORKERS` | 100 | Number of concurrent workers |
| `QUEUE_CONCURRENCY` | 1000 | Maximum concurrent jobs in queue |
| `JOB_PROCESSING_TIME_MS` | 0 | Simulated job processing time |

## Test Scenarios

### 1. Maximum Throughput Test
```bash
NUM_JOBS=10000 NUM_WORKERS=100 QUEUE_CONCURRENCY=10000 npm run perf:test
```
Tests absolute maximum throughput with no artificial limits.

### 2. Concurrency Limited Test
```bash
NUM_JOBS=10000 NUM_WORKERS=100 QUEUE_CONCURRENCY=50 npm run perf:test
```
Tests behavior when queue concurrency is the bottleneck.

### 3. Realistic Workload Test
```bash
NUM_JOBS=5000 NUM_WORKERS=50 JOB_PROCESSING_TIME_MS=100 npm run perf:test
```
Simulates real-world conditions with job processing time.

### 4. Stress Test
```bash
NUM_JOBS=50000 NUM_WORKERS=200 npm run perf:extreme
```
Pushes system to limits to identify bottlenecks.

## Database Monitoring

While tests run, monitor with SQL queries:

```bash
psql -h localhost -U user -d queue
\i examples/analyze-performance.sql
```

Key monitoring queries:
- Real-time job status distribution
- Queue shard utilization
- Latency percentiles
- Throughput over time
- Worker distribution
- System health (connections, table sizes)

## Expected Performance

### Ideal Conditions
- **Throughput**: 1000-2000 jobs/sec
- **Latency P50**: 50-200ms
- **Latency P99**: 200-500ms

### Typical Conditions
- **Throughput**: 500-1000 jobs/sec
- **Latency P50**: 100-300ms
- **Latency P99**: 500-1000ms

## Next Steps

1. **Run baseline tests** on your hardware
2. **Record results** for future comparison
3. **Experiment with different configurations**
4. **Optimize based on bottlenecks identified**
5. **Set up continuous performance testing** in CI/CD

## Additional Resources

- Full guide: `examples/PERFORMANCE_TESTING.md`
- Quick reference: `examples/PERFORMANCE_SUMMARY.md`
- SQL queries: `examples/analyze-performance.sql`
- Source code: `examples/performance-test.ts`

## Cleanup

Remove test data:
```bash
psql -h localhost -U user -d queue -c "
DELETE FROM jobs WHERE queue_id IN (
  SELECT id FROM queues WHERE name LIKE 'perf-test-%'
);
DELETE FROM queues WHERE name LIKE 'perf-test-%';
"
```

Or reset everything:
```bash
npm run migrate:down && npm run migrate:up
```

