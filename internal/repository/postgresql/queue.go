package postgresql

import (
	"context"
	"database/sql"
	"distributed-task-queue/domain"
)

type QueueRepository struct {
	DB *sql.DB
}

// NewJobRepository will create an implementation of job.Repository
func NewQueueRepository(db *sql.DB) *QueueRepository {
	return &QueueRepository{
		DB: db,
	}
}

func (m *QueueRepository) getOne(ctx context.Context, query string, args ...interface{}) (res domain.Queue, err error) {
	stmt, err := m.DB.PrepareContext(ctx, query)
	if err != nil {
		return domain.Queue{}, err
	}
	row := stmt.QueryRowContext(ctx, args...)
	res = domain.Queue{}

	err = row.Scan(
		&res.ID,
		&res.Name,
		&res.MaxAttempts,
		&res.LeaseDuration,
		&res.CreatedAt,
		&res.UpdatedAt,
	)
	return res, err
}

func (m *QueueRepository) GetByID(ctx context.Context, id int64) (domain.Queue, error) {
	query := `SELECT id, name, max_attempts, lease_duration, created_at, updated_at FROM queue WHERE id=$1`
	return m.getOne(ctx, query, id)
}

func (m *QueueRepository) CreateQueue(ctx context.Context, args ...interface{}) (res domain.Queue, err error) {
	err = m.DB.QueryRowContext(ctx, `INSERT INTO queue (name, max_attempts, lease_duration, created_at, updated_at) VALUES ($1, $2, $3, $4, $5) RETURNING id, name, max_attempts, lease_duration, created_at, updated_at`, args...).Scan(&res.ID, &res.Name, &res.MaxAttempts, &res.LeaseDuration, &res.CreatedAt, &res.UpdatedAt)
	return res, err
}
