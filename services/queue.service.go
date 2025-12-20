package job

import (
	"context"
	"distributed-task-queue/domain"
	postgresql "distributed-task-queue/internal/repository/postgresql"
)

type QueueService struct {
	queueRepo postgresql.QueueRepository
}

func NewQueueService(queueRepo postgresql.QueueRepository) *QueueService {
	return &QueueService{queueRepo: queueRepo}
}

func (s *QueueService) CreateQueue(ctx context.Context, queue domain.Queue) (domain.Queue, error) {
	return s.queueRepo.CreateQueue(ctx, queue)
}

func (s *QueueService) GetQueue(ctx context.Context, id int64) (domain.Queue, error) {
	return s.queueRepo.GetByID(ctx, id)
}
