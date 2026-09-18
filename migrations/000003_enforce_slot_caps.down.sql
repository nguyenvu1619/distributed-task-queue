-- Drop the admission cap constraints; the caps go back to being maintained
-- solely by the conditional UPDATEs in the pull path.

ALTER TABLE group_queue_limits
  DROP CONSTRAINT IF EXISTS group_queue_limits_running_within_cap;

ALTER TABLE queue_shards
  DROP CONSTRAINT IF EXISTS queue_shards_running_within_cap;
