.PHONY: up down logs test test-race test-integration lint load migrate-up migrate-down

up:
	docker compose up --build -d

down:
	docker compose down

logs:
	docker compose logs -f order-service fulfillment-worker

test:
	go test ./...

test-race:
	go test -race ./...

test-integration:
	INTEGRATION_DATABASE_URL='postgres://fulfill:fulfill@localhost:55432/fulfill?sslmode=disable' go test -tags=integration -count=1 ./tests/integration

lint:
	go vet ./...

load:
	docker run --rm --network host -v "$(CURDIR)/loadtest:/scripts:ro" grafana/k6:0.54.0 run /scripts/orders.js

migrate-up:
	docker compose run --rm migrate

migrate-down:
	docker compose run --rm migrate -path=/migrations -database="postgres://fulfill:fulfill@postgres:5432/fulfill?sslmode=disable" down 1
