package job

import (
	"distributed-task-queue/domain"
	postgresql "distributed-task-queue/internal/repository/postgresql"
)

type JobService struct {
	jobRepo   postgresql.JobRepository
	queueRepo postgresql.QueueRepository
}

func NewJobService(jobRepo postgresql.JobRepository, queueRepo postgresql.QueueRepository) *JobService {
	return &JobService{
		jobRepo:   jobRepo,
		queueRepo: queueRepo,
	}
}

func (s *JobService) GetJob(id int64) (domain.Job, error) {
	return s.jobRepo.GetByID(id)
}

func (s *JobService) PublishJob(job *domain.Job) (domain.Job, error) {
	return s.jobRepo.PublishJob(job)
}

func (s *JobService) PullJobs(status domain.JobStatus, limit int) ([]domain.Job, error) {
	return s.jobRepo.PullJobs(status, limit)
}

func (s *JobService) PullJob(queueID int64) (*domain.Job, error) {
	queue, err := s.queueRepo.GetByID(queueID)
	if err != nil {
		return nil, err
	}
	return s.jobRepo.PullJob(queue)
}

func (s *JobService) CompleteJob(id int64, lockToken int64) (domain.Job, error) {
	fetchedJob, fetchedJobErr := s.jobRepo.GetByID(id)
	if fetchedJobErr != nil {
		return domain.Job{}, fetchedJobErr
	}
	queue, err := s.queueRepo.GetByID(fetchedJob.QueueId)
	if err != nil {
		return domain.Job{}, err
	}
	return s.jobRepo.CompleteJob(id, lockToken, queue)
}
