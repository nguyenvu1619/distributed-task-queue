import { Job } from './job';

export type JobHandler<T = any> = (job: Job, payload: T) => Promise<void>;

export interface WorkerOptions {
  queueId: number;
  concurrency?: number;  // number of concurrent slots, default 1
  pollInterval?: number; // ms to wait when queue is empty, default 1000
  handler: JobHandler;
}
