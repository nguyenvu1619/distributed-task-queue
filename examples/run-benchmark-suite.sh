#!/bin/bash

# Benchmark Suite Runner
# Runs multiple performance tests and compares results

set -e

echo "╔════════════════════════════════════════════════════════════════════════════╗"
echo "║                    DISTRIBUTED TASK QUEUE BENCHMARK SUITE                  ║"
echo "╚════════════════════════════════════════════════════════════════════════════╝"
echo ""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Results file
RESULTS_FILE="benchmark-results-$(date +%Y%m%d-%H%M%S).txt"

echo -e "${BLUE}Results will be saved to: $RESULTS_FILE${NC}"
echo ""

# Function to run a test
run_test() {
    local test_name=$1
    local num_jobs=$2
    local num_workers=$3
    local concurrency=$4
    
    echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${GREEN}Running: $test_name${NC}"
    echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    
    echo "=== $test_name ===" >> "$RESULTS_FILE"
    echo "Date: $(date)" >> "$RESULTS_FILE"
    echo "Jobs: $num_jobs, Workers: $num_workers, Concurrency: $concurrency" >> "$RESULTS_FILE"
    echo "" >> "$RESULTS_FILE"
    
    NUM_JOBS=$num_jobs \
    NUM_WORKERS=$num_workers \
    QUEUE_CONCURRENCY=$concurrency \
    npm run perf:test 2>&1 | tee -a "$RESULTS_FILE"
    
    echo "" >> "$RESULTS_FILE"
    echo "────────────────────────────────────────────────────────────────────────────" >> "$RESULTS_FILE"
    echo "" >> "$RESULTS_FILE"
    
    # Wait a bit between tests
    sleep 2
}

# Check if database is accessible
echo -e "${BLUE}Checking database connection...${NC}"
if ! npx ts-node -e "
const { createPool } = require('./src/repository/postgresql/connection');
const dotenv = require('dotenv');
dotenv.config();
const pool = createPool({
  host: process.env.DATABASE_HOST || 'localhost',
  port: parseInt(process.env.DATABASE_PORT || '5432', 10),
  user: process.env.DATABASE_USER || 'user',
  password: process.env.DATABASE_PASS || 'password',
  database: process.env.DATABASE_NAME || 'queue',
});
pool.query('SELECT 1').then(() => { console.log('✅ Database OK'); pool.end(); }).catch((err) => { console.error('❌ Database connection failed'); process.exit(1); });
" 2>/dev/null; then
    echo -e "${RED}❌ Cannot connect to database. Exiting.${NC}"
    exit 1
fi

echo ""
echo -e "${GREEN}Starting benchmark suite...${NC}"
echo ""

# Run tests
run_test "Test 1: Small Load" 1000 10 100
run_test "Test 2: Medium Load" 5000 50 500
run_test "Test 3: Large Load" 10000 100 1000
run_test "Test 4: High Concurrency" 5000 100 5000
run_test "Test 5: Limited Concurrency" 10000 100 50

echo ""
echo -e "${GREEN}╔════════════════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║                         BENCHMARK SUITE COMPLETED                          ║${NC}"
echo -e "${GREEN}╚════════════════════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${BLUE}Results saved to: $RESULTS_FILE${NC}"
echo ""
echo -e "${YELLOW}Summary of all tests:${NC}"
grep -A 4 "Throughput:" "$RESULTS_FILE" | grep -E "(Test [0-9]|Jobs/Second)" || echo "No summary available"
echo ""

