package outbox

import (
	"context"
	"log/slog"
	"time"
)

type Relay struct {
	store     Store
	publisher Publisher
	logger    *slog.Logger
	interval  time.Duration
	lease     time.Duration
	batchSize int
}

func NewRelay(store Store, publisher Publisher, logger *slog.Logger, interval time.Duration, batchSize int) *Relay {
	return &Relay{
		store:     store,
		publisher: publisher,
		logger:    logger,
		interval:  interval,
		lease:     30 * time.Second,
		batchSize: batchSize,
	}
}

func (r *Relay) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		if err := r.publishBatch(ctx); err != nil && ctx.Err() == nil {
			r.logger.Error("outbox batch failed", "error", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (r *Relay) publishBatch(ctx context.Context) error {
	events, err := r.store.Claim(ctx, r.batchSize, r.lease)
	if err != nil {
		return err
	}

	for _, event := range events {
		if err := r.publisher.Publish(ctx, event); err != nil {
			r.logger.Error("outbox event publish failed", "event_id", event.ID, "error", err)
			if markErr := r.store.MarkFailed(ctx, event.ID, err); markErr != nil {
				return markErr
			}
			continue
		}
		if err := r.store.MarkPublished(ctx, event.ID); err != nil {
			return err
		}
	}
	return nil
}
