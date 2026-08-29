export interface Queue {
  id: number;
  name: string;
  maxAttempts: number;
  leaseDuration: number; // Duration in milliseconds
  createdAt: Date;
  updatedAt: Date;
  concurrency: number | null;
  requiresGroupId: boolean;
}

export interface GroupQueueLimits {
  queueId: number;
  groupId: string;
  maxRunning: number;
  running: number;
  updatedAt: Date;
  createdAt: Date;
}

export interface QueueShards {
    queueId: number;
    shardNo: number;
    maxRunning: number;
    running: number;
    updatedAt: Date;
    createdAt: Date;
}

export interface CreateQueueInput {
  name: string;
  maxAttempts: number;
  leaseDuration: number; // Duration in milliseconds
  concurrency: number;
  requiresGroupId?: boolean; // Defaults to false
}


export const NUMBER_OF_SHARD = 32; 
