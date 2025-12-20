package domain

type Group struct {
	ID             int64  `json:"id"`
	Name           string `json:"idempotency_key"`
	QueueId        int64  `json:"queue_id"`
	MaxConcurrency int    `json:"max_concurrency"`
}
