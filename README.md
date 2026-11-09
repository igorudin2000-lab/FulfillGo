# FulfillGo

FulfillGo — сервис обработки заказов с асинхронным выполнением через Kafka. HTTP API принимает заказ, PostgreSQL атомарно сохраняет его и резервирует товар, transactional outbox публикует событие `order.created`, а конкурентный fulfillment worker завершает обработку.

## Возможности

- REST API на стандартном `net/http`;
- идемпотентное создание заказов по заголовку `Idempotency-Key`;
- атомарное резервирование складских остатков в PostgreSQL;
- transactional outbox для надёжной публикации событий в Kafka;
- Kafka consumer group и конкурентная обработка через worker pool;
- защита от повторной обработки событий;
- повторные попытки с exponential backoff;
- graceful shutdown по `SIGINT` и `SIGTERM`;
- SQL-миграции;
- JSON-логирование через `slog`;
- метрики Prometheus и готовый dashboard Grafana;
- distributed tracing через OpenTelemetry и Jaeger;
- unit-, integration-, race- и нагрузочные тесты;
- локальный запуск всего окружения через Docker Compose.

## Архитектура

```text
Client
  │
  │ POST /v1/orders
  ▼
order-service
  │
  │ SQL transaction
  ├── orders
  ├── order_items
  ├── inventory reservation
  └── outbox_events
          │
          │ outbox relay
          ▼
Kafka: orders.v1
          │
          │ consumer group
          ▼
fulfillment-worker
  ├── worker pool
  ├── processed_events deduplication
  ├── inventory reservation release
  └── order status → completed
```

### Гарантии обработки

Создание заказа, резервирование товара и запись события выполняются в одной транзакции PostgreSQL. Это исключает ситуацию, при которой заказ сохранён, но событие для Kafka потеряно.

Outbox relay публикует сохранённые события по модели **at-least-once delivery**, поэтому Kafka может доставить одно событие повторно. Fulfillment worker сохраняет `event_id` в таблице `processed_events` и применяет изменения только один раз.

Сообщения разных Kafka partitions обрабатываются параллельно. Сообщения одной partition направляются в один worker lane, что сохраняет порядок offsets. Kafka offset фиксируется только после успешной транзакции обработки.

## Стек

| Компонент | Назначение |
| --- | --- |
| Go | HTTP API, outbox relay и fulfillment worker |
| PostgreSQL | Заказы, позиции, остатки, outbox и дедупликация событий |
| Kafka | Асинхронная доставка событий между сервисами |
| Docker Compose | Локальное окружение и запуск инфраструктуры |
| Prometheus | Сбор метрик приложений |
| Grafana | Визуализация метрик |
| OpenTelemetry | Создание и экспорт traces |
| Jaeger | Просмотр traces |
| k6 | Нагрузочное тестирование HTTP API |

## Требования

- Docker с поддержкой Docker Compose;
- `make` — опционально, все команды можно выполнить напрямую.

Для локальной разработки без Docker также потребуется Go 1.24 или новее.

## Быстрый запуск

Запустить весь стек:

```bash
docker compose up --build -d
```

Проверить состояние контейнеров:

```bash
docker compose ps
```

Проверить готовность API:

```bash
curl http://localhost:8080/health/ready
```

Ожидаемый ответ:

```json
{"status":"ready"}
```

Создать заказ:

```bash
curl -i http://localhost:8080/v1/orders \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-order-1' \
  -d '{
    "customer_id": "customer-42",
    "items": [
      {"sku": "PHONE-001", "quantity": 1},
      {"sku": "CASE-001", "quantity": 2}
    ]
  }'
```

Новый заказ возвращается со статусом `201 Created`:

```json
{
  "id": "2335166f-9f1d-4322-9f51-0d86895590e3",
  "customer_id": "customer-42",
  "status": "new",
  "items": [
    {"sku": "PHONE-001", "quantity": 1},
    {"sku": "CASE-001", "quantity": 2}
  ],
  "created_at": "2026-07-31T07:46:22.091503Z",
  "updated_at": "2026-07-31T07:46:22.091503Z"
}
```

Получить состояние заказа:

```bash
curl http://localhost:8080/v1/orders/2335166f-9f1d-4322-9f51-0d86895590e3
```

После обработки worker статус изменится по цепочке:

```text
new → processing → completed
```

Повторный запрос с тем же `Idempotency-Key` вернёт существующий заказ с `200 OK` и не создаст повторный резерв.

## HTTP API

### `POST /v1/orders`

Создаёт заказ и резервирует товары.

Обязательные заголовки:

```http
Content-Type: application/json
Idempotency-Key: <unique-request-key>
```

Тело запроса:

```json
{
  "customer_id": "customer-42",
  "items": [
    {
      "sku": "PHONE-001",
      "quantity": 1
    }
  ]
}
```

| Код | Описание |
| --- | --- |
| `201 Created` | Создан новый заказ |
| `200 OK` | Возвращён заказ, ранее созданный с тем же `Idempotency-Key` |
| `400 Bad Request` | Некорректный JSON, отсутствуют обязательные поля или заголовок |
| `409 Conflict` | Недостаточно товара на складе |
| `500 Internal Server Error` | Внутренняя ошибка сервиса |

### `GET /v1/orders/{id}`

Возвращает заказ и его позиции.

| Код | Описание |
| --- | --- |
| `200 OK` | Заказ найден |
| `400 Bad Request` | Некорректный UUID |
| `404 Not Found` | Заказ не найден |
| `500 Internal Server Error` | Внутренняя ошибка сервиса |

### Служебные endpoints

| Endpoint | Назначение |
| --- | --- |
| `GET /health/live` | Проверка, что order-service запущен |
| `GET /health/ready` | Проверка подключения order-service к PostgreSQL |
| `GET /metrics` | Метрики order-service в формате Prometheus |
| `GET localhost:8081/health/live` | Liveness fulfillment worker |
| `GET localhost:8081/metrics` | Метрики fulfillment worker |

## Тестовые товары

Миграции создают начальные складские остатки:

| SKU | Количество |
| --- | ---: |
| `PHONE-001` | 1000 |
| `CASE-001` | 2000 |
| `CHARGER-001` | 1500 |

## Сервисы и порты

| Сервис | Адрес |
| --- | --- |
| Order API | <http://localhost:8080> |
| Worker metrics | <http://localhost:8081/metrics> |
| PostgreSQL | `localhost:55432` |
| Kafka | `localhost:9092` |
| Prometheus | <http://localhost:9090> |
| Grafana | <http://localhost:3000> |
| Jaeger | <http://localhost:16686> |

Данные для входа в Grafana: `admin` / `admin`.

Host-порт PostgreSQL можно изменить переменной `POSTGRES_PORT`:

```bash
POSTGRES_PORT=5432 docker compose up --build -d
```

## Конфигурация приложений

| Переменная | Значение по умолчанию | Назначение |
| --- | --- | --- |
| `HTTP_ADDRESS` | `:8080` | Адрес HTTP API |
| `METRICS_ADDRESS` | `:8081` | Адрес metrics-сервера worker |
| `DATABASE_URL` | `postgres://fulfill:fulfill@localhost:5432/fulfill?sslmode=disable` | PostgreSQL DSN |
| `KAFKA_BROKERS` | `localhost:9092` | Список Kafka brokers через запятую |
| `KAFKA_TOPIC` | `orders.v1` | Topic событий заказов |
| `KAFKA_CONSUMER_GROUP` | `fulfillment-worker-v1` | Consumer group worker |
| `WORKER_COUNT` | `4` | Количество worker lanes |
| `OUTBOX_INTERVAL` | `500ms` | Интервал опроса outbox |
| `OUTBOX_BATCH_SIZE` | `100` | Максимальный размер outbox batch |
| `FULFILLMENT_DELAY` | `100ms` | Задержка, имитирующая обработку заказа |
| `SHUTDOWN_TIMEOUT` | `15s` | Таймаут graceful shutdown |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | пусто | OTLP HTTP endpoint для traces |

Пример локальной конфигурации находится в `.env.example`. Docker Compose передаёт контейнерам готовые значения автоматически.

## Наблюдаемость

Приложения пишут структурированные JSON-логи в `stdout`. Посмотреть их можно командой:

```bash
docker compose logs -f order-service fulfillment-worker
```

Prometheus собирает метрики обоих приложений. Grafana автоматически получает Prometheus datasource и dashboard `FulfillGo` со следующими показателями:

- количество созданных заказов;
- HTTP requests per second;
- p95 HTTP latency;
- результаты обработки Kafka-сообщений;
- глубина очереди worker pool.

OpenTelemetry Collector принимает traces по OTLP HTTP и экспортирует их в Jaeger.

## Тесты

Unit-тесты и компиляция всех пакетов:

```bash
go test ./...
```

Проверка конкурентного доступа:

```bash
go test -race ./...
```

Статический анализ:

```bash
go vet ./...
```

Integration-тест использует реальный PostgreSQL и проверяет создание заказа, резервирование, смену статуса и идемпотентную повторную обработку события:

```bash
docker compose up -d postgres migrate
INTEGRATION_DATABASE_URL='postgres://fulfill:fulfill@localhost:55432/fulfill?sslmode=disable' \
  go test -tags=integration -count=1 ./tests/integration
```

Нагрузочный тест запускает k6 со скоростью 20 заказов в секунду в течение 30 секунд и проверяет пороги `http_req_failed < 1%` и `p95 < 300 ms`:

```bash
make load
```

Тест расходует остаток `CASE-001`. Для повторного запуска с исходными данными пересоздайте PostgreSQL volume.

## Команды

| Команда | Действие |
| --- | --- |
| `make up` | Собрать и запустить весь стек |
| `make down` | Остановить контейнеры |
| `make logs` | Следить за логами приложений |
| `make test` | Запустить unit-тесты |
| `make test-race` | Запустить тесты с race detector |
| `make test-integration` | Запустить integration-тест |
| `make lint` | Запустить `go vet` |
| `make load` | Запустить нагрузочный тест k6 |
| `make migrate-up` | Применить миграции |
| `make migrate-down` | Откатить последнюю миграцию |

## Структура проекта

```text
fulfill-go/
├── cmd/
│   ├── order-service/          # HTTP API и outbox relay
│   └── fulfillment-worker/     # Kafka consumer и metrics server
├── internal/
│   ├── config/                 # Конфигурация из environment variables
│   ├── fulfillment/            # Обработчик order.created
│   ├── kafka/                  # Kafka producer и consumer
│   ├── observability/          # Prometheus и OpenTelemetry
│   ├── order/                  # Доменная модель и бизнес-правила
│   ├── outbox/                 # Transactional outbox relay
│   ├── storage/postgres/       # PostgreSQL repositories
│   └── transport/http/         # HTTP handlers
├── migrations/                 # SQL-миграции и тестовый inventory
├── tests/integration/          # Integration-тесты PostgreSQL flow
├── deployments/                # Prometheus, Grafana и OTel Collector
├── loadtest/                   # k6-сценарий
├── docker-compose.yml
├── Dockerfile
└── Makefile
```

## Остановка и очистка

Остановить контейнеры, сохранив данные PostgreSQL:

```bash
docker compose down
```

Остановить контейнеры и удалить PostgreSQL volume со всеми заказами:

```bash
docker compose down -v
```