package fulfillment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"

	"github.com/igor/fulfill-go/internal/order"
	"github.com/igor/fulfill-go/internal/outbox"
)

var ErrInvalidEvent = errors.New("invalid event")

type orderCreatedData struct {
	OrderID string `json:"order_id"`
}

type Processor struct {
	pool  *pgxpool.Pool
	delay time.Duration
}

func NewProcessor(pool *pgxpool.Pool, delay time.Duration) *Processor {
	return &Processor{pool: pool, delay: delay}
}

func (p *Processor) Process(ctx context.Context, value []byte) error {
	ctx, span := otel.Tracer("fulfillment-worker").Start(ctx, "process order.created")
	defer span.End()

	var event outbox.Event
	if err := json.Unmarshal(value, &event); err != nil {
		return fmt.Errorf("%w: decode envelope: %v", ErrInvalidEvent, err)
	}
	if event.ID == "" || event.Type != "order.created" {
		return fmt.Errorf("%w: unsupported event %q", ErrInvalidEvent, event.Type)
	}
	var data orderCreatedData
	if err := json.Unmarshal(event.Data, &data); err != nil || data.OrderID == "" {
		return fmt.Errorf("%w: decode order.created data", ErrInvalidEvent)
	}

	if err := p.markProcessing(ctx, data.OrderID); err != nil {
		return err
	}
	timer := time.NewTimer(p.delay)
	select {
	case <-ctx.Done():
		timer.Stop()
		return ctx.Err()
	case <-timer.C:
	}
	return p.complete(ctx, event.ID, data.OrderID)
}

func (p *Processor) markProcessing(ctx context.Context, orderID string) error {
	tag, err := p.pool.Exec(ctx, `
		UPDATE orders
		SET status = $2, updated_at = now()
		WHERE id = $1 AND status = $3
	`, orderID, order.StatusProcessing, order.StatusNew)
	if err != nil {
		return fmt.Errorf("mark order processing: %w", err)
	}
	if tag.RowsAffected() == 0 {
		var exists bool
		if err := p.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM orders WHERE id = $1)`, orderID).Scan(&exists); err != nil {
			return fmt.Errorf("check order existence: %w", err)
		}
		if !exists {
			return fmt.Errorf("order %s does not exist", orderID)
		}
	}
	return nil
}

func (p *Processor) complete(ctx context.Context, eventID, orderID string) error {
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin fulfillment transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx, `
		INSERT INTO processed_events (event_id)
		VALUES ($1)
		ON CONFLICT DO NOTHING
	`, eventID)
	if err != nil {
		return fmt.Errorf("record processed event: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return tx.Commit(ctx)
	}

	var status order.Status
	err = tx.QueryRow(ctx, `SELECT status FROM orders WHERE id = $1 FOR UPDATE`, orderID).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("order %s does not exist", orderID)
	}
	if err != nil {
		return fmt.Errorf("lock order: %w", err)
	}
	if status == order.StatusCompleted || status == order.StatusFailed {
		return tx.Commit(ctx)
	}

	_, err = tx.Exec(ctx, `
		UPDATE inventory AS inventory
		SET reserved = inventory.reserved - items.quantity,
		    updated_at = now()
		FROM order_items AS items
		WHERE items.order_id = $1
		  AND inventory.sku = items.sku
	`, orderID)
	if err != nil {
		return fmt.Errorf("release shipped inventory reservation: %w", err)
	}
	_, err = tx.Exec(ctx, `
		UPDATE orders
		SET status = $2, updated_at = now()
		WHERE id = $1
	`, orderID, order.StatusCompleted)
	if err != nil {
		return fmt.Errorf("complete order: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit fulfillment transaction: %w", err)
	}
	return nil
}
