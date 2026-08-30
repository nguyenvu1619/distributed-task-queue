#!/bin/bash

# Quick Performance Test Script
# This script runs a performance test with the specified configuration

set -e

echo "🎯 Distributed Task Queue - Performance Test"
echo "============================================="
echo ""

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default values
JOBS=${NUM_JOBS:-10000}
WORKERS=${NUM_WORKERS:-100}
CONCURRENCY=${QUEUE_CONCURRENCY:-1000}
PROCESSING_TIME=${JOB_PROCESSING_TIME_MS:-0}

echo -e "${YELLOW}Configuration:${NC}"
echo "  Jobs:              $JOBS"
echo "  Workers:           $WORKERS"
echo "  Queue Concurrency: $CONCURRENCY"
echo "  Processing Time:   ${PROCESSING_TIME}ms"
echo ""

# Check if .env file exists
if [ ! -f .env ]; then
    echo -e "${YELLOW}Warning: .env file not found. Using default database configuration.${NC}"
    echo "Copy example.env to .env and configure your database connection."
    echo ""
fi

# Check if PostgreSQL is accessible
echo "🔍 Checking database connection..."
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
pool.query('SELECT 1').then(() => { console.log('✅ Database connection OK'); pool.end(); }).catch((err) => { console.error('❌ Database connection failed:', err.message); process.exit(1); });
" 2>/dev/null; then
    echo "❌ Cannot connect to PostgreSQL. Please check your database configuration."
    exit 1
fi

echo ""
echo -e "${GREEN}Starting performance test...${NC}"
echo ""

# Run the performance test
NUM_JOBS=$JOBS \
NUM_WORKERS=$WORKERS \
QUEUE_CONCURRENCY=$CONCURRENCY \
JOB_PROCESSING_TIME_MS=$PROCESSING_TIME \
npm run perf:test

echo ""
echo -e "${GREEN}✅ Performance test completed!${NC}"

