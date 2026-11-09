package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/igor/fulfill-go/internal/order"
)

type OrderRepository struct {
	pool *pgxpool.Pool
}

func NewOrderRepository(pool *pgxpool.Pool) *OrderRepository {
	return &OrderRepository{pool: pool}
}

func (r *OrderRepository) Create(ctx context.Context, newOrder order.Order) (order.Order, bool, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return order.Order{}, false, fmt.Errorf("begin create order transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Serialize concurrent requests with the same key before checking it.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, newOrder.IdempotencyKey); err != nil {
		return order.Order{}, false, fmt.Errorf("lock idempotency key: %w", err)
	}

	existingID, err := findOrderIDByKey(ctx, tx, newOrder.IdempotencyKey)
	if err != nil {
		return order.Order{}, false, err
	}
	if existingID != "" {
		existing, err := getOrder(ctx, tx, existingID)
		return existing, false, err
	}

	for _, item := range newOrder.Items {
		tag, err := tx.Exec(ctx, `
			UPDATE inventory
			SET available = available - $2,
			    reserved = reserved + $2,
			    updated_at = now()
			WHERE sku = $1 AND available >= $2
		`, item.SKU, item.Quantity)
		if err != nil {
			return order.Order{}, false, fmt.Errorf("reserve inventory for %s: %w", item.SKU, err)
		}
		if tag.RowsAffected() != 1 {
			return order.Order{}, false, fmt.Errorf("%w: sku %s", order.ErrInsufficientStock, item.SKU)
		}
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO orders (id, customer_id, idempotency_key, status, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $5)
	`, newOrder.ID, newOrder.CustomerID, newOrder.IdempotencyKey, newOrder.Status, newOrder.CreatedAt)
	if err != nil {
		return order.Order{}, false, fmt.Errorf("insert order: %w", err)
	}

	batch := &pgx.Batch{}
	for _, item := range newOrder.Items {
		batch.Queue(`
			INSERT INTO order_items (order_id, sku, quantity)
			VALUES ($1, $2, $3)
		`, newOrder.ID, item.SKU, item.Quantity)
	}
	results := tx.SendBatch(ctx, batch)
	if err := results.Close(); err != nil {
		return order.Order{}, false, fmt.Errorf("insert order items: %w", err)
	}

	data, err := json.Marshal(struct {
		OrderID    string       `json:"order_id"`
		CustomerID string       `json:"customer_id"`
		Items      []order.Item `json:"items"`
	}{
		OrderID:    newOrder.ID,
		CustomerID: newOrder.CustomerID,
		Items:      newOrder.Items,
	})
	if err != nil {
		return order.Order{}, false, fmt.Errorf("encode order event: %w", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_events (id, aggregate_id, event_type, payload, created_at)
		VALUES ($1, $2, 'order.created', $3, $4)
	`, uuid.NewString(), newOrder.ID, data, newOrder.CreatedAt)
	if err != nil {
		return order.Order{}, false, fmt.Errorf("insert outbox event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return order.Order{}, false, fmt.Errorf("commit create order transaction: %w", err)
	}
	return newOrder, true, nil
}

func (r *OrderRepository) GetByID(ctx context.Context, id string) (order.Order, error) {
	return getOrder(ctx, r.pool, id)
}

type queryer interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

func findOrderIDByKey(ctx context.Context, query queryer, key string) (string, error) {
	var id string
	err := query.QueryRow(ctx, `SELECT id FROM orders WHERE idempotency_key = $1`, key).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("find order by idempotency key: %w", err)
	}
	return id, nil
}

func getOrder(ctx context.Context, query queryer, id string) (order.Order, error) {
	var result order.Order
	err := query.QueryRow(ctx, `
		SELECT id, customer_id, idempotency_key, status, created_at, updated_at
		FROM orders
		WHERE id = $1
	`, id).Scan(
		&result.ID,
		&result.CustomerID,
		&result.IdempotencyKey,
		&result.Status,
		&result.CreatedAt,
		&result.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return order.Order{}, order.ErrNotFound
	}
	if err != nil {
		return order.Order{}, fmt.Errorf("select order: %w", err)
	}

	rows, err := query.Query(ctx, `
		SELECT sku, quantity
		FROM order_items
		WHERE order_id = $1
		ORDER BY id
	`, id)
	if err != nil {
		return order.Order{}, fmt.Errorf("select order items: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var item order.Item
		if err := rows.Scan(&item.SKU, &item.Quantity); err != nil {
			return order.Order{}, fmt.Errorf("scan order item: %w", err)
		}
		result.Items = append(result.Items, item)
	}
	if err := rows.Err(); err != nil {
		return order.Order{}, fmt.Errorf("iterate order items: %w", err)
	}
	return result, nil
}
