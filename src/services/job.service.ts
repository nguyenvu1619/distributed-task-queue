import { JobRepository } from '../repository/postgresql/job.repository';
import { QueueRepository } from '../repository/postgresql/queue.repository';
import { Job, JobStatus, CreateJobInput } from '../domain/job';

export class JobService {
  constructor(
    private jobRepo: JobRepository,
    private queueRepo: QueueRepository
  ) {}

  async getJob(id: number): Promise<Job> {
    return this.jobRepo.getById(id);
  }

  async publishJob(input: CreateJobInput): Promise<Job> {
    return this.jobRepo.publishJob(input);
  }

  async pullJobs(status: JobStatus, limit: number): Promise<Job[]> {
    return this.jobRepo.pullJobs(status, limit);
  }

  async pullJob(queueId: number): Promise<Job | null> {
    const queue = await this.queueRepo.getById(queueId);
    const job = await this.jobRepo.pullJob(queue);
    return job;
  }

  async completeJob(id: number, lockSeq: number): Promise<Job> {
    const job = await this.jobRepo.getById(id);
    const queue = await this.queueRepo.getById(job.queueId);
    return this.jobRepo.completeJob(id, lockSeq, queue);
  }

  async failJob(id: number, lockSeq: number): Promise<Job> {
    const job = await this.jobRepo.getById(id);
    const queue = await this.queueRepo.getById(job.queueId);
    return this.jobRepo.failJob(id, lockSeq, queue);
  }
}

