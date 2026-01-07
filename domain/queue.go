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

// QueuePermit represents a permit slot for a queue
// @Description Permit slot information for queue concurrency control
type QueuePermit struct {
	QueueID int64 `json:"queue_id" example:"1"`
	Slot    int   `json:"slot" example:"0"`

	LeaseToken     *string    `json:"lease_token,omitempty" example:"550e8400-e29b-41d4-a716-446655440000"`
	LeasedBy       *string    `json:"leased_by,omitempty" example:"worker-123"`
	LeaseExpiresAt *time.Time `json:"lease_expires_at,omitempty" example:"2024-01-01T00:00:00Z"`
	UpdatedAt      time.Time  `json:"updated_at" example:"2024-01-01T00:00:00Z"`
}
