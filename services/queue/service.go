package queue

import (
	"distributed-task-queue/domain"
	postgresql "distributed-task-queue/internal/repository/postgresql"
)

type QueueService struct {
	queueRepo postgresql.QueueRepository
}

func NewQueueService(queueRepo postgresql.QueueRepository) *QueueService {
	return &QueueService{queueRepo: queueRepo}
}

func (s *QueueService) CreateQueue(queue domain.Queue) (domain.Queue, error) {
	return s.queueRepo.CreateQueue(queue)
}

func (s *QueueService) GetQueue(id int64) (domain.Queue, error) {
	return s.queueRepo.GetByID(id)
}

func (s *QueueService) GetAllQueues() ([]domain.Queue, error) {
	return s.queueRepo.GetAll()
}
