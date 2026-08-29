-- Replace broad indexes with partial indexes scoped to the relevant status.
-- Smaller index size, faster scans on the hot paths.

-- Drop old non-partial indexes
DROP INDEX IF EXISTS idx_job_queue_id_status;
DROP INDEX IF EXISTS idx_job_group_id_created_at;
DROP INDEX IF EXISTS idx_job_queue_id;

-- Pull queries: WHERE queue_id=$1 AND status='PENDING' ORDER BY created_at
CREATE INDEX idx_job_pending_queue ON jobs (queue_id, created_at)
  WHERE status = 'PENDING';

-- Reaper queries: WHERE status='PROCESSING' AND lease_expires_at <= now()
CREATE INDEX idx_job_processing_lease ON jobs (lease_expires_at)
  WHERE status = 'PROCESSING';

-- Group-based pull queries: WHERE group_id=$1 AND status='PENDING' ORDER BY created_at
CREATE INDEX idx_job_pending_group ON jobs (group_id, created_at)
  WHERE status = 'PENDING';
