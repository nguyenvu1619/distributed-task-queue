// Public API exports
export { JobService } from './services/job.service';
export { QueueService } from './services/queue.service';
export { ReaperService, ReaperServiceOptions } from './services/reaper.service';

export { JobRepository } from './repository/postgresql/job.repository';
export { QueueRepository } from './repository/postgresql/queue.repository';
export { createPool, DatabaseConfig } from './repository/postgresql/connection';

// Domain exports
export * from './domain/job';
export * from './domain/queue';
export * from './domain/errors';

// Re-export Pool from pg for convenience
export { Pool } from 'pg';

