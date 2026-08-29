-- Performance Analysis Queries
-- Run these queries during or after performance tests to analyze system behavior

-- ============================================================================
-- Job Statistics
-- ============================================================================

-- Job status distribution
SELECT 
    status,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM jobs
GROUP BY status
ORDER BY count DESC;

-- Job completion rate over time (5-second buckets)
SELECT 
    date_trunc('second', completed_at) as time_bucket,
    COUNT(*) as completed_jobs,
    ROUND(AVG(EXTRACT(EPOCH FROM (completed_at - created_at)) * 1000)) as avg_latency_ms
FROM jobs
WHERE completed_at IS NOT NULL
GROUP BY time_bucket
ORDER BY time_bucket;

-- Jobs by group
SELECT 
    group_id,
    COUNT(*) as total_jobs,
    COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) as completed,
    COUNT(CASE WHEN status = 'FAILED' THEN 1 END) as failed,
    COUNT(CASE WHEN status = 'PENDING' THEN 1 END) as pending,
    COUNT(CASE WHEN status = 'PROCESSING' THEN 1 END) as processing
FROM jobs
GROUP BY group_id
ORDER BY total_jobs DESC
LIMIT 20;

-- ============================================================================
-- Queue Statistics
-- ============================================================================

-- Queue summary
SELECT 
    q.id,
    q.name,
    q.concurrency,
    q.max_attempts,
    q.lease_duration / 1000000 as lease_duration_ms,
    COUNT(j.id) as total_jobs,
    COUNT(CASE WHEN j.status = 'COMPLETED' THEN 1 END) as completed_jobs,
    COUNT(CASE WHEN j.status = 'FAILED' THEN 1 END) as failed_jobs,
    COUNT(CASE WHEN j.status = 'PENDING' THEN 1 END) as pending_jobs,
    COUNT(CASE WHEN j.status = 'PROCESSING' THEN 1 END) as processing_jobs
FROM queues q
LEFT JOIN jobs j ON j.queue_id = q.id
GROUP BY q.id, q.name, q.concurrency, q.max_attempts, q.lease_duration
ORDER BY q.id DESC;

-- Queue shard distribution
SELECT 
    qs.queue_id,
    qs.shard_no,
    qs.running,
    qs.max_running,
    ROUND(qs.running * 100.0 / NULLIF(qs.max_running, 0), 2) as utilization_pct,
    COUNT(j.id) as jobs_in_shard
FROM queue_shards qs
LEFT JOIN jobs j ON j.queue_id = qs.queue_id AND j.queue_shard_no = qs.shard_no
GROUP BY qs.queue_id, qs.shard_no, qs.running, qs.max_running
ORDER BY qs.queue_id, qs.shard_no;

-- Group queue limits
SELECT 
    gql.queue_id,
    gql.group_id,
    gql.running,
    gql.max_running,
    ROUND(gql.running * 100.0 / NULLIF(gql.max_running, 0), 2) as utilization_pct,
    COUNT(j.id) as total_jobs_in_group
FROM group_queue_limits gql
LEFT JOIN jobs j ON j.queue_id = gql.queue_id AND j.group_id = gql.group_id
GROUP BY gql.queue_id, gql.group_id, gql.running, gql.max_running
ORDER BY gql.queue_id, gql.group_id;

-- ============================================================================
-- Performance Metrics
-- ============================================================================

-- Latency percentiles (in milliseconds)
WITH latencies AS (
    SELECT 
        EXTRACT(EPOCH FROM (completed_at - created_at)) * 1000 as latency_ms
    FROM jobs
    WHERE completed_at IS NOT NULL
)
SELECT 
    ROUND(MIN(latency_ms)) as min_latency_ms,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY latency_ms)) as p25_latency_ms,
    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY latency_ms)) as p50_latency_ms,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY latency_ms)) as p75_latency_ms,
    ROUND(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY latency_ms)) as p90_latency_ms,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms)) as p95_latency_ms,
    ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latency_ms)) as p99_latency_ms,
    ROUND(MAX(latency_ms)) as max_latency_ms,
    ROUND(AVG(latency_ms)) as avg_latency_ms
FROM latencies;

-- Processing time distribution
WITH processing_times AS (
    SELECT 
        EXTRACT(EPOCH FROM (completed_at - updated_at)) * 1000 as processing_time_ms
    FROM jobs
    WHERE completed_at IS NOT NULL AND status = 'COMPLETED'
)
SELECT 
    ROUND(AVG(processing_time_ms)) as avg_processing_time_ms,
    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY processing_time_ms)) as p50_processing_time_ms,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY processing_time_ms)) as p95_processing_time_ms,
    ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY processing_time_ms)) as p99_processing_time_ms
FROM processing_times;

-- Throughput over time (jobs completed per minute)
SELECT 
    date_trunc('minute', completed_at) as time_bucket,
    COUNT(*) as jobs_completed,
    ROUND(AVG(EXTRACT(EPOCH FROM (completed_at - created_at)) * 1000)) as avg_latency_ms
FROM jobs
WHERE completed_at IS NOT NULL
GROUP BY time_bucket
ORDER BY time_bucket DESC
LIMIT 20;

-- ============================================================================
-- System Health
-- ============================================================================

-- Active database connections
SELECT 
    COUNT(*) as total_connections,
    COUNT(CASE WHEN state = 'active' THEN 1 END) as active_connections,
    COUNT(CASE WHEN state = 'idle' THEN 1 END) as idle_connections
FROM pg_stat_activity
WHERE datname = current_database();

-- Table sizes
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS total_size,
    pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) AS table_size,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename) - pg_relation_size(schemaname||'.'||tablename)) AS index_size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- Index usage
SELECT
    schemaname,
    tablename,
    indexname,
    idx_scan as index_scans,
    idx_tup_read as tuples_read,
    idx_tup_fetch as tuples_fetched,
    pg_size_pretty(pg_relation_size(indexrelid)) as index_size
FROM pg_stat_user_indexes
WHERE schemaname = 'public'
ORDER BY idx_scan DESC;

-- ============================================================================
-- Cleanup Queries
-- ============================================================================

-- Remove completed jobs older than 1 hour
-- CAUTION: Uncomment to execute
-- DELETE FROM jobs 
-- WHERE status = 'COMPLETED' 
-- AND completed_at < NOW() - INTERVAL '1 hour';

-- Remove all test queues and their jobs
-- CAUTION: Uncomment to execute
-- DELETE FROM jobs WHERE queue_id IN (
--   SELECT id FROM queues WHERE name LIKE 'perf-test-%'
-- );
-- DELETE FROM queues WHERE name LIKE 'perf-test-%';

-- ============================================================================
-- Real-time Monitoring
-- ============================================================================

-- Run this query repeatedly to monitor job processing in real-time
SELECT 
    q.name as queue_name,
    COUNT(j.id) FILTER (WHERE j.status = 'PENDING') as pending,
    COUNT(j.id) FILTER (WHERE j.status = 'PROCESSING') as processing,
    COUNT(j.id) FILTER (WHERE j.status = 'COMPLETED') as completed,
    COUNT(j.id) FILTER (WHERE j.status = 'FAILED') as failed,
    SUM(qs.running) as total_running,
    q.concurrency as max_concurrency,
    ROUND(SUM(qs.running) * 100.0 / NULLIF(q.concurrency, 0), 2) as utilization_pct
FROM queues q
LEFT JOIN jobs j ON j.queue_id = q.id
LEFT JOIN queue_shards qs ON qs.queue_id = q.id
WHERE q.name LIKE 'perf-test-%'
GROUP BY q.id, q.name, q.concurrency
ORDER BY q.id DESC;

