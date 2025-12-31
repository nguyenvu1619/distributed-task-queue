package postgresql

import (
	"context"
	"database/sql"
	"distributed-task-queue/domain"
	"fmt"

	"github.com/labstack/echo/v4"
	"github.com/patrickmn/go-cache"
)

type QueueRepository struct {
	DB     *sql.DB
	cache  *cache.Cache
	logger echo.Logger
	ctx    context.Context
}

// NewQueueRepository will create an implementation of queue.Repository
func NewQueueRepository(db *sql.DB, logger echo.Logger) *QueueRepository {
	return &QueueRepository{
		DB:     db,
		cache:  cache.New(cache.NoExpiration, cache.NoExpiration),
		logger: logger,
		ctx:    context.Background(),
	}
}

func (m *QueueRepository) getOne(query string, args ...interface{}) (res domain.Queue, err error) {
	stmt, err := m.DB.PrepareContext(m.ctx, query)
	if err != nil {
		return domain.Queue{}, err
	}
	row := stmt.QueryRowContext(m.ctx, args...)
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

func (m *QueueRepository) GetByID(id int64) (domain.Queue, error) {
	if queueCached, found := m.cache.Get(fmt.Sprintf("queues:%d", id)); found {
		queue := queueCached.(domain.Queue)
		m.logger.Infof("Cache HIT for queue ID: %d", id)
		return queue, nil
	}
	m.logger.Infof("Cache MISS for queue ID: %d, querying database", id)
	query := `SELECT id, name, max_attempts, lease_duration, created_at, updated_at FROM queue WHERE id=$1`
	queue, err := m.getOne(query, id)
	if err == nil {
		m.cache.Set(fmt.Sprintf("queues:%d", id), queue, cache.NoExpiration)
		m.logger.Infof("Cached queue ID: %d", id)
	}
	return queue, err
}

func (m *QueueRepository) GetAll() ([]domain.Queue, error) {
	// assume there are not too many queue
	query := `SELECT id, name, max_attempts, lease_duration, created_at, updated_at FROM queue`
	rows, err := m.DB.QueryContext(m.ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	queues := make([]domain.Queue, 0)
	for rows.Next() {
		var queue domain.Queue
		if err := rows.Scan(
			&queue.ID,
			&queue.Name,
			&queue.MaxAttempts,
			&queue.LeaseDuration,
			&queue.CreatedAt,
			&queue.UpdatedAt,
		); err != nil {
			return nil, err
		}
		queues = append(queues, queue)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	for _, queue := range queues {
		m.cache.Set(fmt.Sprintf("queues:%d", queue.ID), queue, cache.NoExpiration)
	}
	return queues, nil
}

func (m *QueueRepository) CreateQueue(queue domain.Queue) (res domain.Queue, err error) {
	m.logger.Debugf("CreateQueue: %+v", queue)
	err = m.DB.QueryRowContext(m.ctx, `INSERT INTO queue (name, max_attempts, lease_duration, created_at, updated_at) VALUES ($1, $2, $3, $4, $5) RETURNING id, name, max_attempts, lease_duration, created_at, updated_at`,
		queue.Name,
		queue.MaxAttempts,
		queue.LeaseDuration,
		queue.CreatedAt,
		queue.UpdatedAt,
	).Scan(&res.ID, &res.Name, &res.MaxAttempts, &res.LeaseDuration, &res.CreatedAt, &res.UpdatedAt)
	return res, err
}
