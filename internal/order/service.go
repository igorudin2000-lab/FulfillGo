package order

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

type Service struct {
	repository Repository
	now        func() time.Time
	newID      func() string
}

func NewService(repository Repository) *Service {
	return &Service{
		repository: repository,
		now:        func() time.Time { return time.Now().UTC() },
		newID:      func() string { return uuid.NewString() },
	}
}

func (s *Service) Create(ctx context.Context, command CreateCommand) (Order, bool, error) {
	command.CustomerID = strings.TrimSpace(command.CustomerID)
	command.IdempotencyKey = strings.TrimSpace(command.IdempotencyKey)
	if err := validateCreate(command); err != nil {
		return Order{}, false, err
	}

	now := s.now()
	newOrder := Order{
		ID:             s.newID(),
		CustomerID:     command.CustomerID,
		IdempotencyKey: command.IdempotencyKey,
		Status:         StatusNew,
		Items:          append([]Item(nil), command.Items...),
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	return s.repository.Create(ctx, newOrder)
}

func (s *Service) Get(ctx context.Context, id string) (Order, error) {
	if _, err := uuid.Parse(id); err != nil {
		return Order{}, fmt.Errorf("%w: malformed order id", ErrInvalidInput)
	}
	return s.repository.GetByID(ctx, id)
}

func validateCreate(command CreateCommand) error {
	if command.CustomerID == "" {
		return fmt.Errorf("%w: customer_id is required", ErrInvalidInput)
	}
	if command.IdempotencyKey == "" {
		return fmt.Errorf("%w: Idempotency-Key header is required", ErrInvalidInput)
	}
	if len(command.IdempotencyKey) > 128 {
		return fmt.Errorf("%w: Idempotency-Key is too long", ErrInvalidInput)
	}
	if len(command.Items) == 0 {
		return fmt.Errorf("%w: at least one item is required", ErrInvalidInput)
	}

	seen := make(map[string]struct{}, len(command.Items))
	for index, item := range command.Items {
		item.SKU = strings.TrimSpace(item.SKU)
		if item.SKU == "" {
			return fmt.Errorf("%w: items[%d].sku is required", ErrInvalidInput, index)
		}
		if item.Quantity <= 0 {
			return fmt.Errorf("%w: items[%d].quantity must be positive", ErrInvalidInput, index)
		}
		if _, exists := seen[item.SKU]; exists {
			return fmt.Errorf("%w: duplicate sku %q", ErrInvalidInput, item.SKU)
		}
		seen[item.SKU] = struct{}{}
		command.Items[index].SKU = item.SKU
	}
	return nil
}
