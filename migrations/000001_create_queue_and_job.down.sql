DROP INDEX IF EXISTS idx_job_queue_id;
DROP INDEX IF EXISTS idx_job_group_id_created_at;
DROP INDEX IF EXISTS idx_job_status_created_at;

DROP TABLE IF EXISTS queue_shards;
DROP TABLE IF EXISTS group_queue_limits;
DROP TABLE IF EXISTS jobs;
DROP TABLE IF EXISTS queues;
