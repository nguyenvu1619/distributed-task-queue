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
    return this.jobRepo.pullJob(queue);
  }

  async completeJob(id: number, lockToken: number): Promise<Job> {
    const job = await this.jobRepo.getById(id);
    const queue = await this.queueRepo.getById(job.queueId);
    return this.jobRepo.completeJob(id, lockToken, queue);
  }

  async failJob(id: number, lockToken: number): Promise<Job> {
    const job = await this.jobRepo.getById(id);
    const queue = await this.queueRepo.getById(job.queueId);
    return this.jobRepo.failJob(id, lockToken, queue);
  }
}

