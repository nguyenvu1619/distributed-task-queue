import { JobRepository } from '../repository/postgresql/job.repository';
import { QueueRepository } from '../repository/postgresql/queue.repository';
import { Job, JobStatus, CreateJobInput, PublishedJob } from '../domain/job';
import { Queue } from '../domain/queue';
import { Executor } from '../domain/executor';

export class JobService {
  constructor(
    private jobRepo: JobRepository,
    private queueRepo: QueueRepository
  ) {}

  async getJob(id: number): Promise<Job> {
    return this.jobRepo.getById(id);
  }

  /**
   * Publishes a job. Pass `executor` (a `PoolClient` inside BEGIN/COMMIT, or any
   * handle exposing `query(text, values)`) to have the job commit atomically
   * with the caller's own writes. A key a live job already holds comes back as
   * that job with `deduplicated: true` rather than raising — see
   * `JobRepository.publishJobs`.
   */
  async publishJob(input: CreateJobInput, executor?: Executor): Promise<PublishedJob> {
    return this.jobRepo.publishJob(input, executor);
  }

  /**
   * Publishes many jobs in one statement — one round trip, all-or-nothing —
   * returning one result per input, in input order. Same `executor` and same
   * deduplication rules as `publishJob`.
   */
  async publishJobs(inputs: CreateJobInput[], executor?: Executor): Promise<PublishedJob[]> {
    return this.jobRepo.publishJobs(inputs, executor);
  }

  /** Live PENDING/PROCESSING backlog for a queue. */
  async countByStatus(queueId: number): Promise<{ pending: number; processing: number }> {
    return this.jobRepo.countByStatus(queueId);
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
}
