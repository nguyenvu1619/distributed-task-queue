-- Drops the whole schema. Indexes and constraints go with their tables, so only
-- the tables are named here, in foreign-key order.

DROP TABLE IF EXISTS queue_shards;
DROP TABLE IF EXISTS group_queue_limits;
DROP TABLE IF EXISTS jobs;
DROP TABLE IF EXISTS queues;
