export enum JobStatus {
  PENDING = 'PENDING',
  PROCESSING = 'PROCESSING',
  COMPLETED = 'COMPLETED',
  FAILED = 'FAILED',
}

export interface Metadata {
  consumer_id?: string;
  last_pull_at?: Date;
  [key: string]: any;
}

export interface Job {
  id: number;
  idempotency_key: string;
  payload: string;
  status: JobStatus;
  group_id: string;
  attempts: number;
  metadata: Metadata;
  created_at: Date;
  updated_at: Date;
  completed_at: Date | null;
  lease_expires_at: Date | null;
  queue_id: number;
  lock_token: number;
}

export interface CreateJobInput {
  idempotency_key: string;
  payload: string;
  group_id: string;
  queue_id: number;
  attempts?: number;
  metadata?: Metadata;
}

