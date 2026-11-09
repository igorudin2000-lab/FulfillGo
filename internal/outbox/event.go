package outbox

import (
	"context"
	"encoding/json"
	"time"
)

type Event struct {
	ID          string          `json:"event_id"`
	Type        string          `json:"event_type"`
	AggregateID string          `json:"aggregate_id"`
	OccurredAt  time.Time       `json:"occurred_at"`
	Data        json.RawMessage `json:"data"`
}

type Store interface {
	Claim(context.Context, int, time.Duration) ([]Event, error)
	MarkPublished(context.Context, string) error
	MarkFailed(context.Context, string, error) error
}

type Publisher interface {
	Publish(context.Context, Event) error
	Close() error
}
