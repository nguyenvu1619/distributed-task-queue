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
  groupId: string;
  attempts: number;
  metadata: Metadata;
  createdAt: Date;
  updatedAt: Date;
  completedAt: Date | null;
  leaseExpiresAt: Date | null;
  queueId: number;
  lockToken: number;
}

export interface CreateJobInput {
  idempotencyKey: string;
  payload: string;
  groupId: string;
  queueId: number;
  attempts?: number;
  metadata?: Metadata;
}

