package order

import (
	"context"
	"errors"
	"testing"
	"time"
)

type repositoryStub struct {
	create func(context.Context, Order) (Order, bool, error)
	get    func(context.Context, string) (Order, error)
}

func (repository repositoryStub) Create(ctx context.Context, value Order) (Order, bool, error) {
	return repository.create(ctx, value)
}

func (repository repositoryStub) GetByID(ctx context.Context, id string) (Order, error) {
	return repository.get(ctx, id)
}

func TestServiceCreate(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.July, 31, 9, 0, 0, 0, time.UTC)
	var captured Order
	repository := repositoryStub{
		create: func(_ context.Context, value Order) (Order, bool, error) {
			captured = value
			return value, true, nil
		},
		get: func(context.Context, string) (Order, error) {
			return Order{}, ErrNotFound
		},
	}
	service := NewService(repository)
	service.now = func() time.Time { return now }
	service.newID = func() string { return "018fbb71-9d67-7c00-b632-8b2b72c9a111" }

	created, isNew, err := service.Create(context.Background(), CreateCommand{
		CustomerID:     "customer-1",
		IdempotencyKey: "request-1",
		Items:          []Item{{SKU: " PHONE-001 ", Quantity: 2}},
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if !isNew {
		t.Fatal("Create() isNew = false, want true")
	}
	if created.Status != StatusNew {
		t.Fatalf("Create() status = %q, want %q", created.Status, StatusNew)
	}
	if captured.Items[0].SKU != "PHONE-001" {
		t.Fatalf("Create() sku = %q, want trimmed value", captured.Items[0].SKU)
	}
	if !created.CreatedAt.Equal(now) || !created.UpdatedAt.Equal(now) {
		t.Fatal("Create() did not set timestamps from the clock")
	}
}

func TestServiceCreateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		command CreateCommand
	}{
		{name: "missing customer", command: CreateCommand{IdempotencyKey: "key", Items: []Item{{SKU: "A", Quantity: 1}}}},
		{name: "missing idempotency key", command: CreateCommand{CustomerID: "customer", Items: []Item{{SKU: "A", Quantity: 1}}}},
		{name: "empty items", command: CreateCommand{CustomerID: "customer", IdempotencyKey: "key"}},
		{name: "zero quantity", command: CreateCommand{CustomerID: "customer", IdempotencyKey: "key", Items: []Item{{SKU: "A"}}}},
		{name: "duplicate sku", command: CreateCommand{CustomerID: "customer", IdempotencyKey: "key", Items: []Item{{SKU: "A", Quantity: 1}, {SKU: "A", Quantity: 2}}}},
	}

	repository := repositoryStub{
		create: func(context.Context, Order) (Order, bool, error) {
			t.Fatal("repository must not be called for invalid input")
			return Order{}, false, nil
		},
		get: func(context.Context, string) (Order, error) {
			return Order{}, ErrNotFound
		},
	}
	service := NewService(repository)

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, _, err := service.Create(context.Background(), test.command)
			if !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("Create() error = %v, want ErrInvalidInput", err)
			}
		})
	}
}

func TestServiceGetRejectsMalformedID(t *testing.T) {
	t.Parallel()

	service := NewService(repositoryStub{
		create: func(context.Context, Order) (Order, bool, error) {
			return Order{}, false, nil
		},
		get: func(context.Context, string) (Order, error) {
			t.Fatal("repository must not be called for malformed id")
			return Order{}, nil
		},
	})

	_, err := service.Get(context.Background(), "not-a-uuid")
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("Get() error = %v, want ErrInvalidInput", err)
	}
}
