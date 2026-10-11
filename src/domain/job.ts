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
  // todo: update to uuid?
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
  availableAt: Date | null;
  queueId: number;
  /**
   * Fence token for the current lease. 0 means the job has never been leased;
   * every pull takes the next value, so a settle is accepted only when it
   * carries the token of the lease still in force.
   */
  lockSeq: number;
  queueShardNo: number | null;
}

/**
 * What a publish hands back.
 *
 * `deduplicated` distinguishes the two ways a publish succeeds: `false` means
 * this call inserted the row, `true` means a live job already held the key and
 * is being returned instead. A key is only reserved while its job is alive —
 * completed and failed jobs are deleted, which frees the key for reuse — so
 * the dedup window is the job's lifetime, not for ever.
 */
export interface PublishedJob extends Job {
  deduplicated: boolean;
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


