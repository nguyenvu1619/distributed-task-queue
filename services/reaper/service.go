package reaper

import (
	"context"
	"distributed-task-queue/internal/repository/postgresql"
	"fmt"
	"sync/atomic"
	"time"
)

type ReaperService struct {
	jobRepo   postgresql.JobRepository
	queueRepo postgresql.QueueRepository
	interval  time.Duration
	batchSize int
	running   atomic.Bool
}

func NewReaperService(jobRepo postgresql.JobRepository, queueRepo postgresql.QueueRepository) *ReaperService {
	return &ReaperService{
		jobRepo:   jobRepo,
		queueRepo: queueRepo,
		interval:  30 * time.Second,
		batchSize: 100,
	}
}

func (m *ReaperService) RunOnce() (ids *[]int64, err error) {
	return m.jobRepo.RecoverJobs()
}

func (m *ReaperService) Start(ctx context.Context) {
	fmt.Printf("interval %d", m.interval)
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !m.running.CompareAndSwap(false, true) {
				fmt.Printf("Reaper skip")
				continue
			}
			go func() {
				defer m.running.Store(false)

				// optional per-run timeout so it can’t hang forever
				_, cancel := context.WithTimeout(ctx, 10*time.Second)
				defer cancel()
				ids, err := m.RunOnce()
				if err != nil {
					fmt.Printf("Reaper process error %v", err)
				}
				fmt.Printf("[Reaper] Mark %d jobs", len(*ids))
			}()
		}
	}
}
