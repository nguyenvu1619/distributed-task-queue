package postgresql

import (
	"context"
	"database/sql"
	"distributed-task-queue/domain"
	"log"
)

type JobRepository struct {
	DB  *sql.DB
	ctx context.Context
}

// NewJobRepository will create an implementation of job.Repository
func NewJobRepository(db *sql.DB) *JobRepository {
	return &JobRepository{
		DB:  db,
		ctx: context.Background(),
	}
}

func (m *JobRepository) GetByID(id int64) (res domain.Job, err error) {
	err = m.DB.QueryRowContext(m.ctx, `SELECT id, idempotency_key, payload, status, group_id, queue_id, attempts, metadata, created_at, updated_at, completed_at FROM job WHERE id = $1`, id).Scan(&res.ID, &res.IdempotencyKey, &res.Payload, &res.Status, &res.GroupId, &res.QueueId, &res.Attempts, &res.Metadata, &res.CreatedAt, &res.UpdatedAt, &res.CompletedAt)
	return res, err
}

func (m *JobRepository) PublishJob(job *domain.Job) (res domain.Job, err error) {
	err = m.DB.QueryRowContext(m.ctx, `INSERT INTO job (idempotency_key, payload, status, group_id, queue_id, attempts, metadata) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id, idempotency_key, payload, status, group_id, queue_id, attempts, metadata, created_at, updated_at, completed_at`,
		job.IdempotencyKey, job.Payload, job.Status, job.GroupId, job.QueueId, job.Attempts, job.Metadata).Scan(&res.ID, &res.IdempotencyKey, &res.Payload, &res.Status, &res.GroupId, &res.QueueId, &res.Attempts, &res.Metadata, &res.CreatedAt, &res.UpdatedAt, &res.CompletedAt)
	return res, err
}

func (m *JobRepository) PullJobs(status domain.JobStatus, limit int) (res []domain.Job, err error) {
	rows, err := m.DB.QueryContext(m.ctx, `SELECT id, idempotency_key, payload, status, group_id, queue_id, attempts, metadata, created_at, updated_at, completed_at FROM job WHERE status = $1 ORDER BY created_at LIMIT $2`, status, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var job domain.Job
		err = rows.Scan(&job.ID, &job.IdempotencyKey, &job.Payload, &job.Status, &job.GroupId, &job.QueueId, &job.Attempts, &job.Metadata, &job.CreatedAt, &job.UpdatedAt, &job.CompletedAt)
		if err != nil {
			return nil, err
		}
		res = append(res, job)
	}
	return res, nil
}

func (m *JobRepository) ProcessJob(queue domain.Queue) (res domain.Job, err error) {
	tx, err := m.DB.BeginTx(m.ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		log.Fatal(err)
	}
	// ignore the retry first
	queryErr := tx.QueryRowContext(m.ctx, `SELECT id, idempotency_key, payload, status, group_id, queue_id, attempts, metadata, created_at, updated_at, completed_at FROM job WHERE status = 'PENDING' or (status = 'PROCESSING' and  lease_expired_at < now()) ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED`).Scan(&res.ID, &res.IdempotencyKey, &res.Payload, &res.Status, &res.GroupId, &res.QueueId, &res.Attempts, &res.Metadata, &res.CreatedAt, &res.UpdatedAt, &res.CompletedAt)
	if queryErr != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			log.Fatalf("update failed: %v, unable to back: %v", err, rollbackErr)
		}
		log.Fatal(queryErr)
		return domain.Job{}, err
	}
	tx.ExecContext(m.ctx, "UPDATE job SET lease_expired_at = now() + $1 and lock_token = $2 where id = $3", queue.LeaseDuration.Microseconds(), res.LockToken+1, &res.ID)
	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}
	return res, nil
}

func (m *JobRepository) CompleteJob(id int64, lockToken int64, queue domain.Queue) (res domain.Job, err error) {
	err = m.DB.QueryRowContext(m.ctx, `UPDATE job SET status = 'COMPLETED' where lock_token = $1 and id = $2 returning id, idempotency_key, payload, status, group_id, queue_id, attempts, metadata, created_at, updated_at, completed_at`, lockToken, id).Scan(&res.ID, &res.IdempotencyKey, &res.Payload, &res.Status, &res.GroupId, &res.QueueId, &res.Attempts, &res.Metadata, &res.CreatedAt, &res.UpdatedAt, &res.CompletedAt)
	return res, nil
}
