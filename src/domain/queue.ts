export interface Queue {
  id: number;
  name: string;
  max_attempts: number;
  lease_duration: number; // Duration in milliseconds
  created_at: Date;
  updated_at: Date;
}

export interface QueuePermit {
  queue_id: number;
  slot: number;
  lease_token: string | null;
  leased_by: string | null;
  lease_expires_at: Date | null;
  updated_at: Date;
}

export interface CreateQueueInput {
  name: string;
  max_attempts: number;
  lease_duration: number; // Duration in milliseconds
  concurrency: number;
}

