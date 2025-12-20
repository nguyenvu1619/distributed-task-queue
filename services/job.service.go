package job

import (
	"context"
	"distributed-task-queue/domain"
	postgresql "distributed-task-queue/internal/repository/postgresql"
)

type JobService struct {
	jobRepo postgresql.JobRepository
}

func NewJobService(jobRepo postgresql.JobRepository) *JobService {
	return &JobService{jobRepo: jobRepo}
}

func (s *JobService) PublishJob(ctx context.Context, job domain.Job) (domain.Job, error) {
	return s.jobRepo.PublishJob(ctx, job)
}

func (s *JobService) PullJobs(ctx context.Context, status domain.JobStatus, limit int) ([]domain.Job, error) {
	return s.jobRepo.PullJobs(ctx, status, limit)
}
