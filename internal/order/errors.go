package order

import "errors"

var (
	ErrNotFound          = errors.New("order not found")
	ErrInvalidInput      = errors.New("invalid input")
	ErrInsufficientStock = errors.New("insufficient stock")
)
