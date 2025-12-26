package domain

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"
)

// Job representing the Job data struct

// JobStatus represents the status of a job
type JobStatus string

const (
	JobStatusPending    JobStatus = "PENDING"
	JobStatusProcessing JobStatus = "PROCESSING"
	JobStatusCompleted  JobStatus = "COMPLETED"
	JobStatusFailed     JobStatus = "FAILED"
)

// Metadata represents job metadata
type Metadata struct {
	ConsumerId string    `json:"consumer_id" example:"consumer-123"`
	LastPullAt time.Time `json:"last_pull_at" example:"2024-01-01T00:00:00Z"`
}

// Scan implements sql.Scanner so Metadata can be read from a JSON/JSONB column.
func (m *Metadata) Scan(src any) error {
	if m == nil {
		return fmt.Errorf("domain.Metadata: Scan on nil receiver")
	}
	if src == nil {
		*m = Metadata{}
		return nil
	}

	var b []byte
	switch v := src.(type) {
	case []byte:
		b = v
	case string:
		b = []byte(v)
	default:
		return fmt.Errorf("domain.Metadata: unsupported Scan type %T", src)
	}

	if len(b) == 0 {
		*m = Metadata{}
		return nil
	}

	return json.Unmarshal(b, m)
}

// Value implements driver.Valuer so Metadata can be written to a JSON/JSONB column.
func (m Metadata) Value() (driver.Value, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// Job represents a task job
// @Description Job information for task processing
type Job struct {
	ID             int64      `json:"id" example:"1"`
	IdempotencyKey string     `json:"idempotency_key" example:"unique-key-123"`
	Payload        string     `json:"payload" example:"{\"task\": \"data\"}"`
	Status         JobStatus  `json:"status" example:"PENDING" enums:"PENDING,PROCESSING,COMPLETED,FAILED"`
	GroupId        string     `json:"group_id" example:"group-123"`
	Attempts       int        `json:"attempts" example:"0"`
	Metadata       Metadata   `json:"metadata"`
	CreatedAt      time.Time  `json:"created_at" example:"2024-01-01T00:00:00Z"`
	UpdatedAt      time.Time  `json:"updated_at" example:"2024-01-01T00:00:00Z"`
	CompletedAt    *time.Time `json:"completed_at,omitempty" example:"2024-01-01T00:00:00Z"`
	QueueId        int64      `json:"queue_id" example:"1"`
}
