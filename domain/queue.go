package domain

import "time"

// Queue represents a task queue configuration
// @Description Queue configuration for task processing
type Queue struct {
	ID            int64         `json:"id" example:"1"`
	Name          string        `json:"name" example:"default-queue"`
	MaxAttempts   int           `json:"max_attempts" example:"3"`
	LeaseDuration time.Duration `json:"lease_duration" swaggertype:"string" example:"30s"` // Duration in Go format (e.g., "30s", "1m")
	// DLQInterval   time.Duration `json:"dlq_interval"`
	// DLQId         int64         `json:"dlq_id"`
	CreatedAt time.Time `json:"created_at" example:"2024-01-01T00:00:00Z"`
	UpdatedAt time.Time `json:"updated_at" example:"2024-01-01T00:00:00Z"`
}
