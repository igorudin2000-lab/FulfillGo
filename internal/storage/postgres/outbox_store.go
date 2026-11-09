package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/igor/fulfill-go/internal/outbox"
)

type OutboxStore struct {
	pool *pgxpool.Pool
}

func NewOutboxStore(pool *pgxpool.Pool) *OutboxStore {
	return &OutboxStore{pool: pool}
}

func (s *OutboxStore) Claim(ctx context.Context, limit int, lease time.Duration) ([]outbox.Event, error) {
	rows, err := s.pool.Query(ctx, `
		WITH candidates AS (
			SELECT id
			FROM outbox_events
			WHERE published_at IS NULL
			  AND (locked_until IS NULL OR locked_until < now())
			ORDER BY created_at
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		)
		UPDATE outbox_events AS events
		SET locked_until = now() + $2::interval
		FROM candidates
		WHERE events.id = candidates.id
		RETURNING events.id, events.event_type, events.aggregate_id, events.created_at, events.payload
	`, limit, lease.String())
	if err != nil {
		return nil, fmt.Errorf("claim outbox events: %w", err)
	}
	defer rows.Close()

	events, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (outbox.Event, error) {
		var event outbox.Event
		err := row.Scan(&event.ID, &event.Type, &event.AggregateID, &event.OccurredAt, &event.Data)
		return event, err
	})
	if err != nil {
		return nil, fmt.Errorf("scan outbox events: %w", err)
	}
	return events, nil
}

func (s *OutboxStore) MarkPublished(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE outbox_events
		SET published_at = now(), locked_until = NULL, last_error = NULL
		WHERE id = $1
	`, id)
	if err != nil {
		return fmt.Errorf("mark outbox event published: %w", err)
	}
	return nil
}

func (s *OutboxStore) MarkFailed(ctx context.Context, id string, publishErr error) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE outbox_events
		SET attempts = attempts + 1, last_error = $2, locked_until = NULL
		WHERE id = $1
	`, id, publishErr.Error())
	if err != nil {
		return fmt.Errorf("mark outbox event failed: %w", err)
	}
	return nil
}
