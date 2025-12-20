package domain

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"
)

// Job representing the Job data struct

type JobStatus string

const (
	JobStatusPending    JobStatus = "PENDING"
	JobStatusProcessing JobStatus = "PROCESSING"
	JobStatusCompleted  JobStatus = "COMPLETED"
	JobStatusFailed     JobStatus = "FAILED"
)

type Metadata struct {
	ConsumerId string    `json:"consumer_id"`
	LastPullAt time.Time `json:"last_pull_at"`
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

type Job struct {
	ID             int64     `json:"id"`
	IdempotencyKey string    `json:"idempotency_key"`
	Payload        string    `json:"payload"`
	Status         JobStatus `json:"status"`
	GroupId        string    `json:"group_id"`
	Attempts       int       `json:"attempts"`
	Metadata       Metadata  `json:"metadata"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
	CompletedAt    time.Time `json:"completed_at"`
}
