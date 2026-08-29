# Graphile Worker perfTest Implementation

This document summarizes the implementation of Graphile Worker's perfTest approach for the distributed task queue system.

## What Was Implemented

Following the [Graphile Worker perfTest methodology](https://github.com/graphile/worker/tree/main/perfTest), we've created a comprehensive performance testing suite with three distinct test types.

## New Files Created

### 1. `startup-shutdown-test.ts`
**Purpose:** Measures worker initialization and shutdown performance

**Key Metrics:**
- Connection pool initialization time
- Worker infrastructure setup time
- Total startup time
- Graceful shutdown time

**Usage:**
```bash
npm run perf:startup
```

**Expected Performance:**
- 🚀 Excellent: < 150ms
- ✅ Good: < 300ms
- ⚠️ Fair: < 500ms

**Reference:** Graphile Worker achieves ~110ms on AMD Ryzen 3900

---

### 2. `latency-test.ts`
**Purpose:** Measures queue latency (time from job creation to execution)

**Key Metrics:**
- Min/Max/Average latency
- P50, P95, P99 percentiles
- Latency distribution across buckets

**Usage:**
```bash
npm run perf:latency

# Custom configuration
NUM_JOBS=100 NUM_WORKERS=2 npm run perf:latency
```

**Expected Performance:**
- 🚀 Excellent: < 5ms average
- ✅ Good: < 10ms average
- ⚠️ Fair: < 20ms average

**Reference:** Graphile Worker achieves 2.66ms average (min: 2.39ms, max: 12.09ms)

---

### 3. `perfTest.ts`
**Purpose:** Unified test runner that orchestrates all test types

**Features:**
- Run individual tests or all tests in sequence
- Consistent output formatting
- Test suite summary with pass/fail status
- Command-line argument support

**Usage:**
```bash
# Run all tests
npm run perfTest
npm run perf:all

# Run specific test
npm run perfTest startup
npm run perfTest latency
npm run perfTest load

# Show help
npm run perfTest -- --help
```

---

## Enhanced Existing Files

### `performance-test.ts` (Load Test)
**Enhancements:**
- Added `minLatencyMs` and `maxLatencyMs` to `BenchmarkMetrics` interface
- Updated `calculateFinalMetrics()` to track min/max latency
- Updated `printResults()` to display min/max latency

**Before:**
```typescript
interface BenchmarkMetrics {
  // ... other fields
  averageLatencyMs?: number;
  p50LatencyMs?: number;
  p95LatencyMs?: number;
  p99LatencyMs?: number;
}
```

**After:**
```typescript
interface BenchmarkMetrics {
  // ... other fields
  averageLatencyMs?: number;
  minLatencyMs?: number;      // NEW
  maxLatencyMs?: number;      // NEW
  p50LatencyMs?: number;
  p95LatencyMs?: number;
  p99LatencyMs?: number;
}
```

---

### `package.json`
**New Scripts:**
```json
{
  "scripts": {
    "perf:startup": "ts-node examples/startup-shutdown-test.ts",
    "perf:latency": "ts-node examples/latency-test.ts",
    "perf:load": "ts-node examples/performance-test.ts",
    "perf:all": "ts-node examples/perfTest.ts all",
    "perfTest": "ts-node examples/perfTest.ts"
  }
}
```

---

### `PERFORMANCE_TESTING.md`
**Major Updates:**
- Added overview of three test types
- Documented each test type with usage examples
- Added expected performance benchmarks
- Included comparison with Graphile Worker reference results
- Added example outputs for each test type
- Added test type comparison table
- Updated cleanup instructions

---

## Test Type Comparison

| Test Type | Purpose | Duration | Jobs | Workers | Key Metrics |
|-----------|---------|----------|------|---------|-------------|
| **Startup/Shutdown** | Infrastructure initialization | ~1s | 0 | 5 | Startup time, shutdown time |
| **Latency** | Queue latency measurement | ~5s | 100 | 2 | Min/max/avg latency, percentiles |
| **Load** | Throughput under load | ~10-30s | 10,000 | 100 | Jobs/sec, throughput, latency |

---

## Architecture

```
examples/
├── perfTest.ts                    # Main test runner (NEW)
├── startup-shutdown-test.ts       # Startup/shutdown test (NEW)
├── latency-test.ts                # Latency test (NEW)
├── performance-test.ts            # Load test (ENHANCED)
├── performance-test-multiprocess.ts
└── PERFORMANCE_TESTING.md         # Documentation (ENHANCED)
```

---

## How It Aligns with Graphile Worker

### Graphile Worker perfTest Structure
```
perfTest/
├── run.js                         # Main runner
├── startup-shutdown.js            # Startup/shutdown test
├── latency.js                     # Latency test
└── load.js                        # Load test
```

### Our Implementation
```
examples/
├── perfTest.ts                    # ✅ Main runner (equivalent)
├── startup-shutdown-test.ts       # ✅ Startup/shutdown test (equivalent)
├── latency-test.ts                # ✅ Latency test (equivalent)
└── performance-test.ts            # ✅ Load test (equivalent)
```

---

## Key Features

### 1. Separation of Concerns
Each test type focuses on a specific aspect of performance:
- **Startup/Shutdown:** Infrastructure overhead
- **Latency:** Queue responsiveness
- **Load:** System throughput

### 2. Benchmark Comparisons
All tests include reference benchmarks from Graphile Worker, making it easy to compare performance.

### 3. Performance Ratings
Tests provide visual feedback on performance:
- 🚀 Excellent
- ✅ Good
- ⚠️ Fair
- ❌ Needs Optimization

### 4. Detailed Metrics
Each test provides comprehensive metrics:
- **Startup/Shutdown:** Component-level timing breakdown
- **Latency:** Distribution analysis with histograms
- **Load:** Throughput, latency percentiles, worker statistics

### 5. Unified Runner
The `perfTest.ts` runner provides:
- Single command to run all tests
- Individual test execution
- Summary report with pass/fail status
- Total duration tracking

---

## Usage Examples

### Quick Performance Check
```bash
npm run perfTest
```

### Detailed Analysis
```bash
# Check initialization performance
npm run perf:startup

# Check queue latency
npm run perf:latency

# Check throughput under load
npm run perf:load
```

### Custom Configuration
```bash
# Latency test with more jobs
NUM_JOBS=1000 npm run perf:latency

# Load test with custom parameters
NUM_JOBS=50000 NUM_WORKERS=200 npm run perf:load
```

---

## Benefits

1. **Industry Alignment:** Follows established patterns from Graphile Worker, a mature job queue library
2. **Targeted Testing:** Run only the test you need for specific diagnostics
3. **Better Diagnostics:** Separate concerns (initialization vs. latency vs. throughput)
4. **Easier Benchmarking:** Direct comparison with published Graphile Worker results
5. **Comprehensive Coverage:** All aspects of performance are tested

---

## Reference Performance (Graphile Worker)

**System:** 12-core AMD Ryzen 3900, M.2 SSD, PostgreSQL local

| Metric | Value |
|--------|-------|
| Startup/Shutdown | 110ms |
| Jobs per Second | 11,851 |
| Average Latency | 2.66ms |
| Min Latency | 2.39ms |
| Max Latency | 12.09ms |

---

## Next Steps

1. Run the tests to establish your baseline:
   ```bash
   npm run perfTest
   ```

2. Compare your results with the Graphile Worker reference

3. Identify bottlenecks using the appropriate test:
   - Slow startup? → `npm run perf:startup`
   - High latency? → `npm run perf:latency`
   - Low throughput? → `npm run perf:load`

4. Optimize and re-test to measure improvements

---

## Conclusion

This implementation successfully adopts Graphile Worker's perfTest methodology while maintaining compatibility with the existing performance testing infrastructure. The three-test approach provides comprehensive performance insights and makes it easy to identify and diagnose performance issues.
