package postgresql

import (
	"context"
	"distributed-task-queue/domain"
	"errors"
	"log"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type JobRepository struct {
	DB  *pgxpool.Pool
	ctx context.Context
}

// NewJobRepository will create an implementation of job.Repository
func NewJobRepository(db *pgxpool.Pool) *JobRepository {
	return &JobRepository{
		DB:  db,
		ctx: context.Background(),
	}
}

func (m *JobRepository) GetByID(id int64) (res domain.Job, err error) {
	err = m.DB.QueryRow(m.ctx, `SELECT id, idempotency_key, payload, status, group_id, queue_id, attempts, metadata, created_at, updated_at, completed_at FROM job WHERE id = $1`, id).Scan(&res.ID, &res.IdempotencyKey, &res.Payload, &res.Status, &res.GroupId, &res.QueueId, &res.Attempts, &res.Metadata, &res.CreatedAt, &res.UpdatedAt, &res.CompletedAt)
	return res, err
}

func (m *JobRepository) PublishJob(job *domain.Job) (res domain.Job, err error) {
	err = m.DB.QueryRow(m.ctx, `INSERT INTO job (idempotency_key, payload, status, group_id, queue_id, attempts, metadata) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id, idempotency_key, payload, status, group_id, queue_id, attempts, metadata, created_at, updated_at, completed_at`,
		job.IdempotencyKey, job.Payload, domain.JobStatusPending, job.GroupId, job.QueueId, job.Attempts, job.Metadata).Scan(&res.ID, &res.IdempotencyKey, &res.Payload, &res.Status, &res.GroupId, &res.QueueId, &res.Attempts, &res.Metadata, &res.CreatedAt, &res.UpdatedAt, &res.CompletedAt)
	return res, err
}

func (m *JobRepository) PullJobs(status domain.JobStatus, limit int) (res []domain.Job, err error) {
	rows, err := m.DB.Query(m.ctx, `SELECT id, idempotency_key, payload, status, group_id, queue_id, attempts, metadata, created_at, updated_at, completed_at FROM job WHERE status = $1 ORDER BY created_at LIMIT $2`, status, limit)
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

func (m *JobRepository) PullJob(queue domain.Queue) (res *domain.Job, err error) {
	tx, err := m.DB.BeginTx(m.ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		log.Printf("Start transaction error %v", err)
		return nil, err
	}
	defer tx.Rollback(m.ctx)
	// ignore the retry first
	var job domain.Job
	queryErr := tx.QueryRow(m.ctx, `SELECT id, idempotency_key, payload, status, group_id, queue_id, attempts, metadata, created_at, updated_at, completed_at, lease_token FROM job WHERE status = 'PENDING' ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED`).Scan(&job.ID, &job.IdempotencyKey, &job.Payload, &job.Status, &job.GroupId, &job.QueueId, &job.Attempts, &job.Metadata, &job.CreatedAt, &job.UpdatedAt, &job.CompletedAt, &job.LockToken)
	if queryErr != nil {
		if errors.Is(queryErr, pgx.ErrNoRows) {
			return nil, nil
		}
		log.Printf("Query job error %v", queryErr)
		return nil, queryErr
	}
	job.LockToken = job.LockToken + 1
	if _, err := tx.Exec(m.ctx, "UPDATE job SET lease_expires_at = now() + $1::interval, lease_token = $2, status = 'PROCESSING' where id = $3", queue.LeaseDuration.Microseconds(), job.LockToken, &job.ID); err != nil {
		log.Printf("Update job failed %v", err)
		return nil, err
	}
	if err := tx.Commit(m.ctx); err != nil {
		log.Printf("Commit transaction failed %v", err)
		return nil, err
	}
	return &job, nil
}

func (m *JobRepository) CompleteJob(id int64, lockToken int64, queue domain.Queue) (res domain.Job, err error) {
	err = m.DB.QueryRow(m.ctx, `UPDATE job SET status = 'COMPLETED' where lease_token = $1 and id = $2 and status = 'PENDING' returning id, idempotency_key, payload, status, group_id, queue_id, attempts, metadata, created_at, updated_at, completed_at`, lockToken, id).Scan(&res.ID, &res.IdempotencyKey, &res.Payload, &res.Status, &res.GroupId, &res.QueueId, &res.Attempts, &res.Metadata, &res.CreatedAt, &res.UpdatedAt, &res.CompletedAt)
	return res, err
}

func (m *JobRepository) FailJob(id int64, lockToken int64, queue domain.Queue) (res domain.Job, err error) {
	err = m.DB.QueryRow(m.ctx, `UPDATE job SET status = 'FAILED' where lease_token = $1 and id = $2 and status = 'PENDING' returning id, idempotency_key, payload, status, group_id, queue_id, attempts, metadata, created_at, updated_at, completed_at`, lockToken, id).Scan(&res.ID, &res.IdempotencyKey, &res.Payload, &res.Status, &res.GroupId, &res.QueueId, &res.Attempts, &res.Metadata, &res.CreatedAt, &res.UpdatedAt, &res.CompletedAt)
	return res, err
}

func (m *JobRepository) RecoverJobs() (id *[]int64, err error) {
	tx, err := m.DB.BeginTx(m.ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})

	if err != nil {
		log.Printf("[RecoverJobs] start transaction failed %v", err)
		return nil, err
	}
	defer func() {
		if err != nil {
			if rollbackErr := tx.Rollback(m.ctx); rollbackErr != nil {
				log.Printf("[RecoverJobs] rollback failed: %v", rollbackErr)
			}
		}
	}()

	jobIds, err := tx.Query(m.ctx, `WITH cte AS (
	select id
	from job
	where status = 'PROCESSING'
	AND lease_expires_at <= now()
	FOR UPDATE SKIP LOCKED
  	LIMIT 100
	)
	UPDATE job
SET status='PENDING',
lease_expires_at = null,
lease_token = null
WHERE id IN (SELECT id FROM cte)
RETURNING id;
	`)
	if err != nil {
		log.Printf("[RecoverJobs] query failed %v", err)
		return nil, err
	}
	defer jobIds.Close()
	var recoveredIDs []int64
	for jobIds.Next() {
		var jobId int64
		if err := jobIds.Scan(
			&jobId,
		); err != nil {
			return nil, err
		}
		recoveredIDs = append(recoveredIDs, jobId)
	}
	if err := tx.Commit(m.ctx); err != nil {
		log.Printf("[Reaper] Commit transaction failed %v", err)
		return nil, err
	}
	return &recoveredIDs, nil
}
