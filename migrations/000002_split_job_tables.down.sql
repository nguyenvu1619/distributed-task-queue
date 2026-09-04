-- Revert partial indexes, restore original broad indexes

DROP INDEX IF EXISTS idx_job_pending_queue;
DROP INDEX IF EXISTS idx_job_processing_lease;
DROP INDEX IF EXISTS idx_job_pending_group;

CREATE INDEX idx_job_queue_id_status ON jobs (queue_id, status, created_at);
CREATE INDEX idx_job_group_id_created_at ON jobs (group_id, created_at);
CREATE INDEX idx_job_queue_id ON jobs (queue_id);
