package domain

import "time"

type Queue struct {
	ID            int64         `json:"id"`
	Name          string        `json:"name"`
	MaxAttempts   int           `json:"max_attempts"`
	LeaseDuration time.Duration `json:"lease_duration"`
	// DLQInterval   time.Duration `json:"dlq_interval"`
	// DLQId         int64         `json:"dlq_id"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}
