package order

import "time"

type Status string

const (
	StatusNew        Status = "new"
	StatusProcessing Status = "processing"
	StatusCompleted  Status = "completed"
	StatusFailed     Status = "failed"
)

type Item struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
}

type Order struct {
	ID             string    `json:"id"`
	CustomerID     string    `json:"customer_id"`
	IdempotencyKey string    `json:"-"`
	Status         Status    `json:"status"`
	Items          []Item    `json:"items"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type CreateCommand struct {
	CustomerID     string
	IdempotencyKey string
	Items          []Item
}
