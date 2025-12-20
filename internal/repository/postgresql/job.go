package postgresql

import (
	"context"
	"database/sql"
	"distributed-task-queue/domain"
)

type JobRepository struct {
	DB *sql.DB
}

// NewJobRepository will create an implementation of job.Repository
func NewJobRepository(db *sql.DB) *JobRepository {
	return &JobRepository{
		DB: db,
	}
}

func (m *JobRepository) GetByID(ctx context.Context, id int64) (res domain.Job, err error) {
	err = m.DB.QueryRowContext(ctx, `SELECT id, idempotency_key, payload, status, group_id, attempts, metadata, created_at, updated_at FROM job WHERE id = $1`, id).Scan(&res.ID, &res.IdempotencyKey, &res.Payload, &res.Status, &res.GroupId, &res.Attempts, &res.Metadata, &res.CreatedAt, &res.UpdatedAt)
	return res, err
}

func (m *JobRepository) PublishJob(ctx context.Context, args ...interface{}) (res domain.Job, err error) {
	err = m.DB.QueryRowContext(ctx, `INSERT INTO job (idempotency_key, payload, status, group_id, attempts, metadata, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id, idempotency_key, payload, status, group_id, attempts, metadata, created_at, updated_at`, args...).Scan(&res.ID, &res.IdempotencyKey, &res.Payload, &res.Status, &res.GroupId, &res.Attempts, &res.Metadata, &res.CreatedAt, &res.UpdatedAt)
	return res, err
}

func (m *JobRepository) PullJobs(ctx context.Context, args ...interface{}) (res []domain.Job, err error) {
	rows, err := m.DB.QueryContext(ctx, `SELECT id, idempotency_key, payload, status, group_id, attempts, metadata, created_at, updated_at FROM job WHERE status = $1 ORDER BY created_at LIMIT $2`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var job domain.Job
		err = rows.Scan(&job.ID, &job.IdempotencyKey, &job.Payload, &job.Status, &job.GroupId, &job.Attempts, &job.Metadata, &job.CreatedAt, &job.UpdatedAt)
		if err != nil {
			return nil, err
		}
		res = append(res, job)
	}
	return res, nil
}
