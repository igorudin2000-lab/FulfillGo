package order

import "context"

type Repository interface {
	Create(context.Context, Order) (created Order, isNew bool, err error)
	GetByID(context.Context, string) (Order, error)
}
