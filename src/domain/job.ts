export enum JobStatus {
  PENDING = 'PENDING',
  PROCESSING = 'PROCESSING',
  COMPLETED = 'COMPLETED',
  FAILED = 'FAILED',
}

export interface Metadata {
  consumerId?: string;
  lastPullAt?: Date;
  [key: string]: any;
}

export interface Job {
  id: number;
  idempotencyKey: string;
  payload: string;
  status: JobStatus;
  groupId: string | null;
  attempts: number;
  metadata: Metadata;
  createdAt: Date;
  updatedAt: Date;
  completedAt: Date | null;
  leaseExpiresAt: Date | null;
  queueId: number;
  lockSeq: number | null;
  queueShardNo: number | null;
}

export interface Group{
    id: string;
    concurrency: number;
}

export interface CreateJobInput {
  idempotencyKey: string;
  payload: string;
  queueId: number;
  attempts?: number;
  metadata?: Metadata;
  group?: Group;
}


/**
 * What a publish returns. Extends `Job` so it is usable anywhere a `Job` is,
 * and carries whether the row was newly inserted or an existing live job with
 * the same idempotency key was returned instead.
 *
 * Note: an idempotency key is only reserved while the job is alive. Terminal
 * jobs are deleted, which frees the key — dedup protects against a double
 * publish, not against re-publishing work that has already run.
 */
export interface PublishedJob extends Job {
  deduplicated: boolean;
}
