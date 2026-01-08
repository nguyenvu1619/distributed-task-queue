export interface Queue {
  id: number;
  name: string;
  maxAttempts: number;
  leaseDuration: number; // Duration in milliseconds
  createdAt: Date;
  updatedAt: Date;
}

export interface QueuePermit {
  queueId: number;
  slot: number;
  leaseToken: string | null;
  leasedBy: string | null;
  leaseExpiresAt: Date | null;
  updatedAt: Date;
}

export interface CreateQueueInput {
  name: string;
  maxAttempts: number;
  leaseDuration: number; // Duration in milliseconds
  concurrency: number;
}

