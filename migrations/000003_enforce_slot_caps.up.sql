-- Make the admission caps an invariant of the schema rather than a property the
-- pull query is trusted to maintain.
--
-- `running` is incremented by the admission gate in pullJobWithCoordination and
-- decremented by settle and by the reaper. Every one of those is a conditional
-- UPDATE, so the caps already hold — but nothing *proved* it: an over-admit
-- would raise no error and simply run more jobs than configured. Client-side
-- counters cannot close that gap either, because a worker still holds a job for
-- a round trip after the database has released its slot, so a peak measured in
-- the client legitimately exceeds the cap without the cap ever being broken.
--
-- These constraints are checked inside the statement that writes the counter,
-- so an over-admit fails the pull outright instead of passing unnoticed.
--
-- NOTE: this fixes `running` at or below `max_running` at every commit. A future
-- feature that lowers a cap while jobs are in flight would violate it and needs
-- to drain, or to relax this constraint deliberately.

ALTER TABLE group_queue_limits
  ADD CONSTRAINT group_queue_limits_running_within_cap
  CHECK (running >= 0 AND running <= max_running);

ALTER TABLE queue_shards
  ADD CONSTRAINT queue_shards_running_within_cap
  CHECK (running >= 0 AND running <= max_running);
