package postgresql

import (
	"context"
	"distributed-task-queue/domain"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"
	"github.com/patrickmn/go-cache"
)

type QueueRepository struct {
	DB     *pgxpool.Pool
	cache  *cache.Cache
	logger echo.Logger
	ctx    context.Context
}

// NewQueueRepository will create an implementation of queue.Repository
func NewQueueRepository(db *pgxpool.Pool, logger echo.Logger) *QueueRepository {
	return &QueueRepository{
		DB:     db,
		cache:  cache.New(cache.NoExpiration, cache.NoExpiration),
		logger: logger,
		ctx:    context.Background(),
	}
}

func (m *QueueRepository) getOne(query string, args ...interface{}) (res domain.Queue, err error) {
	row := m.DB.QueryRow(m.ctx, query, args...)
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
	rows, err := m.DB.Query(m.ctx, query)
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

func (m *QueueRepository) CreateQueue(queue domain.Queue, maxConcurrency int) (res domain.Queue, err error) {
	tx, txErr := m.DB.BeginTx(m.ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if txErr != nil {
		log.Printf("[CreateQueue] start transaction failed %v", txErr)
		return domain.Queue{}, txErr
	}
	defer func() {
		if err != nil {
			if rollbackErr := tx.Rollback(m.ctx); rollbackErr != nil {
				log.Printf("[CreateQueue] rollback failed: %v", rollbackErr)
			}
		}
	}()

	err = tx.QueryRow(m.ctx, `INSERT INTO queue (name, max_attempts, lease_duration, created_at, updated_at) VALUES ($1, $2, $3, $4, $5) RETURNING id, name, max_attempts, lease_duration, created_at, updated_at`,
		queue.Name,
		queue.MaxAttempts,
		queue.LeaseDuration,
		queue.CreatedAt,
		queue.UpdatedAt,
	).Scan(&res.ID, &res.Name, &res.MaxAttempts, &res.LeaseDuration, &res.CreatedAt, &res.UpdatedAt)
	if err != nil {
		return domain.Queue{}, err
	}

	var queuePermitBulk []domain.QueuePermit
	for i := range maxConcurrency {
		queuePermit := domain.QueuePermit{
			QueueID:   res.ID,
			Slot:      i,
			UpdatedAt: time.Now(),
		}
		queuePermitBulk = append(queuePermitBulk, queuePermit)
	}
	_, insertPermitError := tx.CopyFrom(
		m.ctx,
		pgx.Identifier{"queue_permits"},
		[]string{"queue_id", "slot", "updated_at"},
		pgx.CopyFromSlice(len(queuePermitBulk), func(i int) ([]any, error) {
			permit := queuePermitBulk[i]
			return []any{permit.QueueID, permit.Slot, permit.UpdatedAt}, nil
		}),
	)

	if insertPermitError != nil {
		log.Printf("[CreateQueue] insert permits failed %v", insertPermitError)
		err = insertPermitError
		return domain.Queue{}, err
	}

	if err = tx.Commit(m.ctx); err != nil {
		return domain.Queue{}, err
	}

	return res, err
}
