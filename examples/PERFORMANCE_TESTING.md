# Performance Testing Guide

This guide explains how to run performance tests for the distributed task queue system.

## Overview

The performance testing suite includes three types of tests, inspired by [Graphile Worker's perfTest approach](https://github.com/graphile/worker/tree/main/perfTest):

1. **Startup/Shutdown Test** - Measures worker initialization and shutdown times
2. **Latency Test** - Measures the time between job creation and execution
3. **Load Test** - Measures throughput and performance under load

Each test type serves a specific purpose and can be run individually or as part of a complete test suite.

## Performance Optimization: Dual Strategy Approach

The system implements **two job processing strategies** to achieve optimal performance:

### Fast Path (concurrency: 0, requiresGroupId: false)
- **Single-query operations** - No transaction overhead
- **Expected latency:** ~3-5ms (close to Graphile Worker's 2.66ms)
- **Use case:** Simple queues without grouping or coordination requirements
- **Condition:** BOTH `concurrency === 0` AND `requiresGroupId === false`
- **Operations per job:** 2 queries total (1 pull + 1 complete)

### Full Coordination Path (concurrency > 0 OR requiresGroupId: true)
- **Multi-query transactions** with full ACID guarantees
- **Expected latency:** ~15-20ms
- **Use case:** Queues requiring group limits and sharding coordination
- **Operations per job:** 7-12 queries total (4-7 pull + 3-5 complete)

### Performance Comparison

| Metric | Fast Path | Full Path | Improvement |
|--------|-----------|-----------|-------------|
| Pull Job Latency | ~3-5ms | ~17ms | **70-80% faster** |
| Complete Job Latency | ~2-3ms | ~12ms | **75-85% faster** |
| Total Job Latency | ~5-8ms | ~29ms | **72-83% faster** |
| Queries per Job | 2 | 7-12 | **83% reduction** |

### How It Works

The system automatically selects the appropriate strategy based on BOTH the queue's `concurrency` and `requiresGroupId` fields:

```typescript
// Fast path example (performance tests use this)
// Requires BOTH: concurrency === 0 AND requiresGroupId === false
const queue = await queueService.createQueue({
  name: 'my-fast-queue',
  maxAttempts: 3,
  leaseDuration: 30000,
  concurrency: 0,              // No concurrency limit
  requiresGroupId: false,      // No group coordination
});

// Full coordination path examples
// Triggered by: concurrency > 0 OR requiresGroupId === true

// Example 1: With concurrency control
const queue = await queueService.createQueue({
  name: 'my-concurrency-queue',
  maxAttempts: 3,
  leaseDuration: 30000,
  concurrency: 100,            // Has concurrency limit → Full path
  requiresGroupId: false,
});

// Example 2: With group coordination
const queue = await queueService.createQueue({
  name: 'my-group-queue',
  maxAttempts: 3,
  leaseDuration: 30000,
  concurrency: 0,
  requiresGroupId: true,       // Needs group coordination → Full path
});
```

### Query Breakdown

**Fast Path - Pull Job:**
```sql
-- Single atomic UPDATE with subquery
UPDATE jobs SET status = 'PROCESSING', ...
WHERE id = (SELECT id FROM jobs WHERE status = 'PENDING' ... FOR UPDATE SKIP LOCKED)
RETURNING *;
```

**Fast Path - Complete Job:**
```sql
-- Single UPDATE
UPDATE jobs SET status = 'COMPLETED', completed_at = now(), ...
WHERE id = $1 AND lease_seq = $2
RETURNING *;
```

**Full Path - Pull Job:**
```sql
BEGIN;
SELECT ... FROM queue_shards FOR UPDATE SKIP LOCKED;  -- If concurrency > 0
SELECT ... FROM jobs FOR UPDATE SKIP LOCKED;
UPDATE group_queue_limits ...;  -- If group exists
UPDATE queue_shards ...;  -- If concurrency > 0
UPDATE jobs ...;
COMMIT;
```

**Full Path - Complete Job:**
```sql
BEGIN;
UPDATE jobs ...;
UPDATE queue_shards ...;  -- If exists
UPDATE group_queue_limits ...;  -- If group exists
COMMIT;
```

### Benefits

1. **Dramatic latency reduction** for simple use cases
2. **Better database scalability** (fewer queries, less locking)
3. **Backward compatible** (existing queues continue working)
4. **Configurable per queue** (choose strategy based on needs)
5. **Maintains full features** for complex coordination scenarios

### When to Use Each Strategy

**Use Fast Path when:**
- You don't need concurrency limits (set `concurrency: 0`)
- You don't need group coordination (set `requiresGroupId: false`)
- You want minimum latency (~3-5ms)
- You want maximum throughput

**Use Full Coordination Path when:**
- You need concurrency limits (set `concurrency > 0`), OR
- You need group-based rate limiting (set `requiresGroupId: true`)
- Latency ~15-20ms is acceptable

**Decision Table:**

| Concurrency | requiresGroupId | Path Used | Reason |
|-------------|-----------------|-----------|---------|
| 0 | false | **Fast** | ✅ No limits, no groups |
| 0 | true | Full | Needs group coordination |
| > 0 | false | Full | Needs concurrency control |
| > 0 | true | Full | Needs both |

## Prerequisites

1. Ensure PostgreSQL is running and accessible
2. Run migrations to set up the database schema:
   ```bash
   npm run migrate:up
   ```
3. Configure your database connection in `.env` file (see `example.env`)

## Running Performance Tests

### Quick Start

Run all performance tests (recommended):
```bash
npm run perfTest
# or
npm run perf:all
```

This will run all three test types in sequence:
1. Startup/Shutdown Test
2. Latency Test
3. Load Test

### Individual Test Types

#### 1. Startup/Shutdown Test

Measures the time taken to initialize the worker infrastructure and gracefully shut down:

```bash
npm run perf:startup
```

**What it measures:**
- Database connection pool initialization time
- Worker infrastructure setup time
- Total startup time
- Graceful shutdown time

**Expected performance:**
- Excellent: < 150ms total
- Good: < 300ms total
- Fair: < 500ms total

**Reference:** Graphile Worker achieves ~110ms on AMD Ryzen 3900 with M.2 SSD

**Note:** This test uses the fast path (`concurrency: 0` AND `requiresGroupId: false`) for optimal performance.

#### 2. Latency Test

Measures the time between job creation and job execution (queue latency):

```bash
npm run perf:latency
```

**What it measures:**
- Minimum latency
- Maximum latency
- Average latency
- P50, P95, P99 latency percentiles
- Latency distribution across buckets

**Configuration:**
```bash
NUM_JOBS=100 NUM_WORKERS=2 npm run perf:latency
```

**Expected performance:**
- Excellent: < 5ms average
- Good: < 10ms average
- Fair: < 20ms average

**Reference:** Graphile Worker achieves 2.66ms average (min: 2.39ms, max: 12.09ms)

**Note:** This test uses the fast path (`concurrency: 0` AND `requiresGroupId: false`) which should achieve latency close to Graphile Worker's performance (~3-5ms vs 2.66ms).

#### 3. Load Test

Measures throughput and performance under load (default: 10,000 jobs, 100 workers):

```bash
npm run perf:load
# or
npm run perf:test
```

### Predefined Test Scenarios

#### Small Test (1,000 jobs, 10 workers)
```bash
npm run perf:small
```

#### Medium Test (5,000 jobs, 50 workers)
```bash
npm run perf:medium
```

#### Large Test (10,000 jobs, 100 workers)
```bash
npm run perf:large
```

#### Extreme Test (50,000 jobs, 200 workers)
```bash
npm run perf:extreme
```

### Custom Configuration

You can customize the test parameters using environment variables:

```bash
NUM_JOBS=10000 \
NUM_WORKERS=100 \
JOB_PROCESSING_TIME_MS=0 \
npm run perf:test
```

#### Configuration Parameters

- `NUM_JOBS`: Number of jobs to publish (default: 10,000)
- `NUM_WORKERS`: Number of concurrent workers (default: 100)
- `JOB_PROCESSING_TIME_MS`: Simulated job processing time in milliseconds (default: 0)

**Note:** Tests use `concurrency: 0` (no limit) and `requiresGroupId: false` (fast path) for maximum performance.

## Understanding the Results

The performance test outputs detailed metrics:

### Job Statistics
- **Total Jobs**: Number of jobs published
- **Completed Jobs**: Jobs successfully completed
- **Failed Jobs**: Jobs that failed processing
- **Success Rate**: Percentage of successful jobs

### Time Statistics
- **Total Duration**: Total time from starting workers to completing all jobs
- **Time to First Job**: Time until the first job was completed

### Throughput
- **Jobs/Second**: Average number of jobs processed per second
- **Jobs/Minute**: Average number of jobs processed per minute

### Latency (End-to-End)
- **Min**: Minimum time from job creation to completion
- **Max**: Maximum time from job creation to completion
- **Average**: Mean time from job creation to completion
- **P50 (Median)**: 50th percentile latency
- **P95**: 95th percentile latency
- **P99**: 99th percentile latency

### Worker Statistics
- **Total Pulls**: Total number of job pulls across all workers
- **Avg per Worker**: Average pulls per worker
- **Most/Least Active**: Distribution of work across workers

## Test Type Comparison

| Test Type | Purpose | Duration | Jobs | Workers |
|-----------|---------|----------|------|---------|
| Startup/Shutdown | Infrastructure initialization | ~1s | 0 | 5 |
| Latency | Queue latency measurement | ~5s | 100 | 2 |
| Load | Throughput under load | ~10-30s | 10,000 | 100 |

## Example Outputs

### Startup/Shutdown Test Output

```
🚀 Starting Startup/Shutdown Performance Test
════════════════════════════════════════════════════════════════════════════════

This test measures the time taken to:
  1. Initialize database connection pool
  2. Set up worker infrastructure
  3. Gracefully shutdown all resources

────────────────────────────────────────────────────────────────────────────────
📊 Measuring connection pool initialization...
✅ Connection pool initialized in 45ms
────────────────────────────────────────────────────────────────────────────────
👷 Measuring worker infrastructure setup...
✅ Worker infrastructure initialized in 78ms
   (5 workers ready)
────────────────────────────────────────────────────────────────────────────────
🧹 Measuring graceful shutdown...
✅ Shutdown completed in 23ms
────────────────────────────────────────────────────────────────────────────────

════════════════════════════════════════════════════════════════════════════════
📊 STARTUP/SHUTDOWN TEST RESULTS
════════════════════════════════════════════════════════════════════════════════

Initialization Times:
  Connection Pool:     45ms
  Worker Infrastructure: 78ms
  Total Startup:       123ms

Shutdown Time:
  Graceful Shutdown:   23ms

Overall Performance:
  Total Time:          146ms

Reference (Graphile Worker on AMD Ryzen 3900):
  Startup/Shutdown:    110ms

Performance Rating:    🚀 Excellent (< 150ms)

════════════════════════════════════════════════════════════════════════════════
```

### Latency Test Output

```
🚀 Starting Latency Performance Test
════════════════════════════════════════════════════════════════════════════════

This test measures the time between job creation and execution.
It uses minimal processing time to isolate queue latency.

Configuration:
  Number of Jobs:      100
  Number of Workers:   2

────────────────────────────────────────────────────────────────────────────────
📦 Creating test queue...
✅ Queue created: ID=1
────────────────────────────────────────────────────────────────────────────────
👷 Starting 2 workers...
✅ Workers started and waiting for jobs
────────────────────────────────────────────────────────────────────────────────
📤 Publishing 100 jobs and measuring latency...
✅ Published 100 jobs in 234ms
────────────────────────────────────────────────────────────────────────────────
⏳ Waiting for all jobs to complete...
✅ All jobs completed
────────────────────────────────────────────────────────────────────────────────

════════════════════════════════════════════════════════════════════════════════
📊 LATENCY TEST RESULTS
════════════════════════════════════════════════════════════════════════════════

Job Statistics:
  Total Jobs Measured: 100

Latency (time from job creation to execution):
  Min:                 3.45ms
  Max:                 15.23ms
  Average:             4.67ms
  P50 (Median):        4.12ms
  P95:                 8.34ms
  P99:                 12.56ms

Reference (Graphile Worker on AMD Ryzen 3900):
  Min:                 2.39ms
  Max:                 12.09ms
  Average:             2.66ms

Performance Rating:    🚀 Excellent (< 5ms avg)

Latency Distribution:
  < 5ms        ████████████████████████████████████ 72 (72.0%)
  5-10ms       ████████████ 24 (24.0%)
  10-20ms      ██ 4 (4.0%)
  20-50ms       0 (0.0%)
  50-100ms      0 (0.0%)
  > 100ms       0 (0.0%)

════════════════════════════════════════════════════════════════════════════════
```

### Load Test Output

```
🚀 Starting Performance Test
Configuration: {
  numJobs: 10000,
  numWorkers: 100,
  jobProcessingTimeMs: 0
}
────────────────────────────────────────────────────────────────────────────────
📦 Creating queue...
✅ Queue created: ID=1
────────────────────────────────────────────────────────────────────────────────
📤 Publishing 10000 jobs...
✅ Published 10000 jobs in 5432ms
   Publishing rate: 1841.62 jobs/sec
────────────────────────────────────────────────────────────────────────────────
👷 Starting 100 workers...
✅ 100 workers started
────────────────────────────────────────────────────────────────────────────────
   Progress: 10000/10000 (892.31 jobs/sec)

════════════════════════════════════════════════════════════════════════════════
📊 PERFORMANCE TEST RESULTS
════════════════════════════════════════════════════════════════════════════════

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
  Min:             5123.45ms
  Max:             8456.78ms
  Average:         6234.56ms
  P50 (Median):    6152ms
  P95:             7890ms
  P99:             8234ms

Worker Statistics:
  Total Pulls:     10000
  Avg per Worker:  100.00
  Most Active:     156 pulls
  Least Active:    67 pulls

════════════════════════════════════════════════════════════════════════════════
```

## Running the Complete Test Suite

To run all tests and get a comprehensive performance report:

```bash
npm run perfTest
```

This will execute:
1. Startup/Shutdown Test (measures initialization performance)
2. Latency Test (measures queue latency with 100 jobs)
3. Load Test (measures throughput with 10,000 jobs)

You can also run specific test types:

```bash
npm run perfTest startup   # Run only startup test
npm run perfTest latency   # Run only latency test
npm run perfTest load      # Run only load test
npm run perfTest all       # Run all tests (same as npm run perfTest)
```

### Help and Usage

```bash
npm run perfTest -- --help
```

## Benchmark Comparison

### Reference System (Graphile Worker)
- **Hardware:** 12-core AMD Ryzen 3900, M.2 SSD
- **Startup/Shutdown:** 110ms
- **Jobs per Second:** 11,851
- **Average Latency:** 2.66ms (min: 2.39ms, max: 12.09ms)

### Your Results
Run the tests to establish your baseline performance metrics. Performance will vary based on:
- CPU cores and speed
- Disk I/O (HDD vs SSD vs NVMe)
- PostgreSQL configuration
- Network latency (if database is remote)
- System load and available resources

## Performance Tips

### Database Connection Pool
The test automatically configures the connection pool size to `max: 150` to handle 100 workers plus overhead. Adjust this in the code if you use more workers.

### Testing Different Scenarios

1. **High Throughput, Low Latency** (No processing time):
   ```bash
   NUM_JOBS=10000 NUM_WORKERS=100 JOB_PROCESSING_TIME_MS=0 npm run perf:test
   ```

2. **Realistic Workload** (With processing time):
   ```bash
   NUM_JOBS=5000 NUM_WORKERS=50 JOB_PROCESSING_TIME_MS=100 npm run perf:test
   ```

3. **Stress Test** (Maximum load):
   ```bash
   NUM_JOBS=50000 NUM_WORKERS=200 npm run perf:test
   ```

## Monitoring During Tests

While the test is running, you can monitor PostgreSQL performance:

```sql
-- Monitor active connections
SELECT count(*) FROM pg_stat_activity;

-- Monitor table sizes
SELECT
  schemaname,
  tablename,
  pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE tablename IN ('jobs', 'queues', 'queue_shards', 'group_queue_limits');

-- Monitor job status distribution
SELECT status, COUNT(*) FROM jobs GROUP BY status;

-- Monitor running jobs per shard
SELECT queue_id, shard_no, running, max_running 
FROM queue_shards 
ORDER BY queue_id, shard_no;
```

## Troubleshooting

### Too Many Database Connections
If you get "too many connections" errors:
1. Reduce `NUM_WORKERS`
2. Increase PostgreSQL's `max_connections` setting
3. Adjust the connection pool size in the test code

### Memory Issues
If the process runs out of memory:
1. Reduce `NUM_JOBS` and run multiple test iterations
2. Increase Node.js heap size: `NODE_OPTIONS="--max-old-space-size=4096" npm run perf:test`

### Slow Performance
1. Ensure PostgreSQL has proper indexes (migrations create them automatically)
2. Check PostgreSQL configuration (shared_buffers, work_mem, etc.)
3. Monitor disk I/O and CPU usage
4. Consider using a faster disk (SSD) for PostgreSQL data directory

## Cleanup

After running tests, you can clean up test data:

```sql
-- Remove all test queues and jobs
DELETE FROM jobs WHERE queue_id IN (
  SELECT id FROM queues WHERE name LIKE 'perf-test-%' 
    OR name LIKE 'startup-test-%'
    OR name LIKE 'latency-test-%'
);
DELETE FROM queues WHERE name LIKE 'perf-test-%' 
  OR name LIKE 'startup-test-%'
  OR name LIKE 'latency-test-%';
```

Or reset the entire database:
```bash
npm run migrate:down
npm run migrate:up
```

