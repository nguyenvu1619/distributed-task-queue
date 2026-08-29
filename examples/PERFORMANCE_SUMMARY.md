# Performance Test Summary

This document provides a quick overview of how to run and interpret performance tests for the distributed task queue system.

## Quick Start

### 1. Setup Database
```bash
# Start PostgreSQL
docker-compose up -d postgres

# Run migrations
npm run migrate:up
```

### 2. Run Performance Test

**Default test (10,000 jobs, 100 workers):**
```bash
npm run perf:test
```

**Quick predefined tests:**
```bash
npm run perf:small      # 1,000 jobs, 10 workers
npm run perf:medium     # 5,000 jobs, 50 workers
npm run perf:large      # 10,000 jobs, 100 workers
npm run perf:extreme    # 50,000 jobs, 200 workers
```

**Custom test:**
```bash
NUM_JOBS=10000 NUM_WORKERS=100 npm run perf:test
```

### 3. Analyze Results

The test automatically prints comprehensive results including:
- **Throughput**: Jobs/second and jobs/minute
- **Latency**: Average, P50, P95, P99
- **Success Rate**: Percentage of completed jobs
- **Worker Distribution**: Work balance across workers

## Understanding Test Scenarios

### Test 1: Maximum Throughput (No Concurrency Limit)
```bash
NUM_JOBS=10000 \
NUM_WORKERS=100 \
QUEUE_CONCURRENCY=10000 \
JOB_PROCESSING_TIME_MS=0 \
npm run perf:test
```
**Purpose**: Test the absolute maximum throughput of the system with no artificial bottlenecks.

### Test 2: Limited Concurrency
```bash
NUM_JOBS=10000 \
NUM_WORKERS=100 \
QUEUE_CONCURRENCY=50 \
JOB_PROCESSING_TIME_MS=0 \
npm run perf:test
```
**Purpose**: Test how the system handles concurrency limits. Only 50 jobs can run simultaneously despite having 100 workers.

### Test 3: Realistic Workload
```bash
NUM_JOBS=5000 \
NUM_WORKERS=50 \
QUEUE_CONCURRENCY=100 \
JOB_PROCESSING_TIME_MS=100 \
npm run perf:test
```
**Purpose**: Simulate real-world conditions where jobs take time to process (100ms each).

### Test 4: Stress Test
```bash
NUM_JOBS=50000 \
NUM_WORKERS=200 \
QUEUE_CONCURRENCY=2000 \
npm run perf:test
```
**Purpose**: Push the system to its limits to identify bottlenecks and failure points.

## Key Metrics Explained

### Throughput
- **Jobs/Second**: Number of jobs completed per second
- **Higher is better**
- Typical values: 500-2000 jobs/sec (depending on hardware and job complexity)

### Latency
- **P50 (Median)**: 50% of jobs complete within this time
- **P95**: 95% of jobs complete within this time
- **P99**: 99% of jobs complete within this time
- **Lower is better**
- Includes time from job creation to completion

### Success Rate
- **Percentage of jobs that completed successfully**
- Should be 100% for simple tests
- Lower values indicate system issues or job processing errors

### Worker Distribution
- **Shows how evenly work is distributed across workers**
- Good distribution: All workers pull similar numbers of jobs
- Poor distribution: Some workers idle while others are overloaded

## Database Monitoring

While tests run, monitor the database:

```bash
# Connect to PostgreSQL
psql -h localhost -U user -d queue

# Run monitoring queries
\i examples/analyze-performance.sql
```

Key queries:
- Job status distribution
- Queue shard utilization
- Latency percentiles
- Throughput over time

## Expected Performance

### Ideal Conditions (Fast hardware, no job processing time)
- **Throughput**: 1000-2000 jobs/second
- **Latency P50**: 50-200ms
- **Latency P99**: 200-500ms

### Typical Conditions (Moderate hardware, realistic jobs)
- **Throughput**: 500-1000 jobs/second
- **Latency P50**: 100-300ms
- **Latency P99**: 500-1000ms

### Limited Resources (Slow hardware, complex jobs)
- **Throughput**: 100-500 jobs/second
- **Latency P50**: 500-1000ms
- **Latency P99**: 1000-2000ms

## Optimization Tips

### 1. Database Tuning
```sql
-- PostgreSQL configuration recommendations
shared_buffers = 256MB              -- 25% of system RAM
effective_cache_size = 1GB          -- 50-75% of system RAM
work_mem = 16MB                     -- Per connection
maintenance_work_mem = 64MB
max_connections = 200               -- Adjust based on workers
```

### 2. Connection Pool Sizing
- Set pool size to: `NUM_WORKERS + 20` (for overhead)
- Too large: Wastes resources
- Too small: Workers wait for connections

### 3. Queue Concurrency
- Set to expected concurrent load
- Too high: No limit, may overwhelm system
- Too low: Artificial bottleneck

### 4. Worker Count
- Optimal: 2-4x number of CPU cores
- Too many: Context switching overhead
- Too few: Underutilized system

### 5. Batch Processing
- For bulk operations, publish jobs in batches
- Use transactions for consistency
- Reduces database round trips

## Troubleshooting

### Problem: Low Throughput
**Symptoms**: Jobs/second much lower than expected

**Possible Causes**:
1. Database connection pool too small
2. PostgreSQL not tuned properly
3. Slow disk I/O
4. Network latency between app and database
5. Too many workers competing for resources

**Solutions**:
- Increase connection pool size
- Tune PostgreSQL settings
- Use SSD for PostgreSQL data
- Reduce worker count
- Check database CPU/memory usage

### Problem: High Latency
**Symptoms**: P99 latency very high

**Possible Causes**:
1. Database queries slow (missing indexes)
2. Lock contention
3. Too many jobs in PROCESSING state
4. Lease expiration causing re-processing

**Solutions**:
- Verify indexes exist (migrations create them)
- Reduce concurrency to decrease contention
- Increase lease duration
- Monitor `queue_shards` for hot spots

### Problem: Uneven Worker Distribution
**Symptoms**: Some workers process many jobs, others few

**Possible Causes**:
1. Lock contention on popular shards
2. Group concurrency limits
3. Fast workers starving slow workers

**Solutions**:
- This is expected behavior with `SKIP LOCKED`
- Ensure adequate queue concurrency
- Monitor group limits aren't too restrictive

### Problem: Database Connection Errors
**Symptoms**: "too many connections" errors

**Possible Causes**:
1. Connection pool too large
2. Connections not being released
3. PostgreSQL max_connections too low

**Solutions**:
- Reduce NUM_WORKERS
- Increase PostgreSQL max_connections
- Check for connection leaks in code

## Cleanup After Tests

Remove test data:

```bash
# Connect to database
psql -h localhost -U user -d queue

# Remove test queues and jobs
DELETE FROM jobs WHERE queue_id IN (
  SELECT id FROM queues WHERE name LIKE 'perf-test-%'
);
DELETE FROM queues WHERE name LIKE 'perf-test-%';
```

Or reset everything:
```bash
npm run migrate:down
npm run migrate:up
```

## Continuous Performance Testing

### CI/CD Integration
```yaml
# Example GitHub Actions workflow
- name: Run Performance Test
  run: |
    docker-compose up -d postgres
    npm run migrate:up
    npm run perf:small
    npm run migrate:down
```

### Baseline Tracking
1. Run tests on consistent hardware
2. Record results (jobs/sec, P99 latency)
3. Compare against baseline with each change
4. Alert if performance degrades >10%

## Additional Resources

- [Full Performance Testing Guide](PERFORMANCE_TESTING.md)
- [SQL Analysis Queries](analyze-performance.sql)
- [Worker Example](worker.ts)
- [Performance Test Source](performance-test.ts)

