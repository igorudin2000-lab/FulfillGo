//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/igor/fulfill-go/internal/fulfillment"
	"github.com/igor/fulfill-go/internal/order"
	"github.com/igor/fulfill-go/internal/outbox"
	"github.com/igor/fulfill-go/internal/storage/postgres"
)

func TestOrderToFulfillmentFlow(t *testing.T) {
	databaseURL := os.Getenv("INTEGRATION_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set INTEGRATION_DATABASE_URL to run integration tests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	sku := "TEST-" + uuid.NewString()
	orderID := uuid.NewString()
	eventID := uuid.NewString()
	idempotencyKey := uuid.NewString()
	_, err = pool.Exec(ctx, `INSERT INTO inventory (sku, available) VALUES ($1, 10)`, sku)
	if err != nil {
		t.Fatalf("seed inventory: %v", err)
	}
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupContext, `DELETE FROM processed_events WHERE event_id = $1`, eventID)
		_, _ = pool.Exec(cleanupContext, `DELETE FROM outbox_events WHERE aggregate_id = $1`, orderID)
		_, _ = pool.Exec(cleanupContext, `DELETE FROM orders WHERE id = $1`, orderID)
		_, _ = pool.Exec(cleanupContext, `DELETE FROM inventory WHERE sku = $1`, sku)
	})

	repository := postgres.NewOrderRepository(pool)
	now := time.Now().UTC()
	created, isNew, err := repository.Create(ctx, order.Order{
		ID:             orderID,
		CustomerID:     "integration-customer",
		IdempotencyKey: idempotencyKey,
		Status:         order.StatusNew,
		Items:          []order.Item{{SKU: sku, Quantity: 2}},
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	if err != nil {
		t.Fatalf("create order: %v", err)
	}
	if !isNew {
		t.Fatal("order unexpectedly treated as duplicate")
	}

	data, _ := json.Marshal(map[string]any{
		"order_id":    created.ID,
		"customer_id": created.CustomerID,
		"items":       created.Items,
	})
	value, _ := json.Marshal(outbox.Event{
		ID:          eventID,
		Type:        "order.created",
		AggregateID: created.ID,
		OccurredAt:  now,
		Data:        data,
	})
	processor := fulfillment.NewProcessor(pool, 0)
	if err := processor.Process(ctx, value); err != nil {
		t.Fatalf("process event: %v", err)
	}
	if err := processor.Process(ctx, value); err != nil {
		t.Fatalf("process duplicate event: %v", err)
	}

	completed, err := repository.GetByID(ctx, created.ID)
	if err != nil {
		t.Fatalf("get completed order: %v", err)
	}
	if completed.Status != order.StatusCompleted {
		t.Fatalf("status = %q, want %q", completed.Status, order.StatusCompleted)
	}

	var available, reserved int
	if err := pool.QueryRow(ctx, `SELECT available, reserved FROM inventory WHERE sku = $1`, sku).Scan(&available, &reserved); err != nil {
		t.Fatal(err)
	}
	if available != 8 || reserved != 0 {
		t.Fatalf("inventory = available:%d reserved:%d, want available:8 reserved:0", available, reserved)
	}
}
