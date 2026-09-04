import { QueueRepository } from '../repository/postgresql/queue.repository';
import { Queue, CreateQueueInput } from '../domain/queue';

export class QueueService {
  constructor(private queueRepo: QueueRepository) {}

  async createQueue(input: CreateQueueInput): Promise<Queue> {
    return this.queueRepo.createQueue(input);
  }

  async getQueue(id: number): Promise<Queue> {
    return this.queueRepo.getById(id);
  }

  async getAllQueues(): Promise<Queue[]> {
    return this.queueRepo.getAll();
  }
}

