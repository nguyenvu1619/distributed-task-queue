package postgresql

import (
	"context"
	"database/sql"
	"distributed-task-queue/domain"
	"log"

	"github.com/patrickmn/go-cache"
)

type QueueRepository struct {
	DB    *sql.DB
	cache *cache.Cache
}

// NewJobRepository will create an implementation of job.Repository
func NewQueueRepository(db *sql.DB) *QueueRepository {
	return &QueueRepository{
		DB:    db,
		cache: cache.New(cache.NoExpiration, cache.NoExpiration),
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

func (m *QueueRepository) GetAll(ctx context.Context) ([]domain.Queue, error) {
	// assume there are not too many queue
	query := `SELECT id, name, max_attempts, lease_duration, created_at, updated_at FROM queue`
	rows, error := m.DB.QueryContext(ctx, query)
	if error != nil {
		return nil, error
	}
	defer rows.Close()
	queues := make([]domain.Queue, 0)
	for rows.Next() {
		var queue domain.Queue
		if err := rows.Scan(&queue); err != nil {
			return nil, error
		}
		queues = append(queues, queue)
	}

	rerr := rows.Close()
	if rerr != nil {
		return nil, error
	}
	if err := rows.Err(); err != nil {
		return nil, error
	}
	m.cache.Set("queues", queues, cache.NoExpiration)
	return queues, nil

}

func (m *QueueRepository) CreateQueue(ctx context.Context, queue domain.Queue) (res domain.Queue, err error) {
	//add log debug
	log.Println("CreateQueue", queue)
	err = m.DB.QueryRowContext(ctx, `INSERT INTO queue (name, max_attempts, lease_duration, created_at, updated_at) VALUES ($1, $2, $3, $4, $5) RETURNING id, name, max_attempts, lease_duration, created_at, updated_at`,
		queue.Name,
		queue.MaxAttempts,
		queue.LeaseDuration,
		queue.CreatedAt,
		queue.UpdatedAt,
	).Scan(&res.ID, &res.Name, &res.MaxAttempts, &res.LeaseDuration, &res.CreatedAt, &res.UpdatedAt)
	return res, err
}
