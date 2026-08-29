// ---------------------------------------------------------------------------
// Public API
//
// This is the whole supported surface. Repositories and services are reachable
// by deep import for tests and benchmarks that need to drive the SQL directly,
// but they are internals: their shape is not covered by this package's version.
// ---------------------------------------------------------------------------

export { TaskQueue } from './client/task-queue';
export type { Reaper } from './client/task-queue';
export type { QueueHandle, Worker, ResolvedQueueConfig } from './client/queue-handle';

export { jsonSerializer } from './client/serializer';
export type { Serializer } from './client/serializer';
export { parseDuration } from './client/duration';
export type { Duration } from './client/duration';

export type {
  TaskQueueOptions,
  QueueConfig,
  PublishOptions,
  WorkOptions,
  ReaperOptions,
  CloseOptions,
  JobContext,
  JobGroup,
  TaskHandler,
} from './client/options';

// Job records handed to handlers and returned from publish.
export { JobStatus } from './domain/job';
export type { Job, PublishedJob, Metadata, Group } from './domain/job';
export type { Queue } from './domain/queue';

// Worker plumbing a caller can observe or implement.
export type {
  WorkerErrorEvent,
  WorkerErrorPhase,
  StopOptions,
  StopResult,
} from './domain/worker';
export type { Executor, QueryResultLike } from './domain/executor';
export { consoleLogger, silentLogger } from './domain/logger';
export type { Logger } from './domain/logger';

export {
  InternalServerError,
  NotFoundError,
  ConflictError,
  BadParamInputError,
} from './domain/errors';

// Connection helpers, for callers who want to build or share their own pool.
export { createPool } from './repository/postgresql/connection';
export type { DatabaseConfig } from './repository/postgresql/connection';
export { Pool } from 'pg';
