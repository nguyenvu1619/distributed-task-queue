import { JobRepository } from '../repository/postgresql/job.repository';
import { QueueRepository } from '../repository/postgresql/queue.repository';
import { Job, JobStatus, CreateJobInput, PublishedJob } from '../domain/job';
import { Executor } from '../domain/executor';
import { Queue } from '../domain/queue';

export class JobService {
  constructor(
    private jobRepo: JobRepository,
    private queueRepo: QueueRepository
  ) {}

  async getJob(id: number): Promise<Job> {
    return this.jobRepo.getById(id);
  }

  async publishJob(input: CreateJobInput, executor?: Executor): Promise<PublishedJob> {
    return this.jobRepo.publishJob(input, executor);
  }

  async publishJobs(inputs: CreateJobInput[], executor?: Executor): Promise<PublishedJob[]> {
    return this.jobRepo.publishJobs(inputs, executor);
  }

  async pullJobs(status: JobStatus, limit: number): Promise<Job[]> {
    return this.jobRepo.pullJobs(status, limit);
  }

  async getQueue(queueId: number): Promise<Queue> {
    return this.queueRepo.getById(queueId);
  }

  // Requires a queue lookup on every call — use when the queue object is not cached locally.
  async pullJob(queueId: number): Promise<Job | null> {
    const queue = await this.queueRepo.getById(queueId);
    return this.jobRepo.pullJob(queue);
  }

  // Pass a pre-resolved Queue to skip the per-call queue lookup.
  async pullJobDirect(queue: Queue): Promise<Job | null> {
    return this.jobRepo.pullJob(queue);
  }

  // Requires two extra lookups (job + queue) on every call.
  async completeJob(id: number, lockSeq: number): Promise<Job> {
    const job = await this.jobRepo.getById(id);
    const queue = await this.queueRepo.getById(job.queueId);
    return this.jobRepo.completeJob(id, lockSeq, queue);
  }

  // Pass a pre-resolved Queue to skip both lookups.
  async completeJobDirect(id: number, lockSeq: number, queue: Queue): Promise<Job> {
    return this.jobRepo.completeJob(id, lockSeq, queue);
  }

  // Requires two extra lookups (job + queue) on every call.
  async failJob(id: number, lockSeq: number): Promise<Job> {
    const job = await this.jobRepo.getById(id);
    const queue = await this.queueRepo.getById(job.queueId);
    return this.jobRepo.failJob(id, lockSeq, queue);
  }

  // Pass a pre-resolved Queue to skip both lookups.
  async failJobDirect(id: number, lockSeq: number, queue: Queue): Promise<Job> {
    return this.jobRepo.failJob(id, lockSeq, queue);
  }

  // Removes a job without spending an attempt — the poison-message path.
  async discardJob(id: number, lockSeq: number): Promise<Job> {
    const job = await this.jobRepo.getById(id);
    const queue = await this.queueRepo.getById(job.queueId);
    return this.jobRepo.discardJob(id, lockSeq, queue);
  }

  // Pass a pre-resolved Queue to skip both lookups.
  async discardJobDirect(id: number, lockSeq: number, queue: Queue): Promise<Job> {
    return this.jobRepo.discardJob(id, lockSeq, queue);
  }
}
