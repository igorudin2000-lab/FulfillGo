# FulfillGo

Учебный сервис обработки заказов для подготовки к стажировке Go-разработчика. Проект показывает не набор разрозненных технологий, а один согласованный путь данных: HTTP-запрос создаёт заказ в PostgreSQL, transactional outbox надёжно доставляет событие в Kafka, а конкурентный worker завершает заказ.

## Что здесь практикуется

- REST API на стандартном `net/http`;
- PostgreSQL, блокировки и SQL-транзакции через `pgx`;
- Kafka producer и consumer group через `kafka-go`;
- transactional outbox и идемпотентный consumer;
- worker pool с сохранением порядка сообщений внутри Kafka partition;
- graceful shutdown;
- версионируемые SQL-миграции;
- Docker Compose;
- unit-, integration- и нагрузочные тесты;
- JSON-логирование через `slog`;
- Prometheus, Grafana и OpenTelemetry traces в Jaeger.

## Архитектура

```text
Клиент
  │ POST /v1/orders
  ▼
order-service
  │ одна SQL-транзакция:
  │ orders + order_items + inventory reserve + outbox_events
  ▼
PostgreSQL ── outbox relay ──▶ Kafka topic orders.v1
                                  │
                                  ▼
                         fulfillment-worker
                         ├─ worker pool
                         ├─ processed_events (deduplication)
                         └─ order → completed
```

Главная проблема, которую решает outbox: обычная последовательность «сначала `COMMIT` в PostgreSQL, потом publish в Kafka» может потерять событие при падении процесса между этими действиями. Здесь бизнес-данные и outbox записываются одной транзакцией. Relay может отправить событие повторно, поэтому consumer хранит `event_id` в `processed_events` и применяет эффект только один раз. Это сочетание даёт **at-least-once delivery + idempotent processing**.

В текущей итерации процессы связаны асинхронно через Kafka. gRPC стоит добавить следующим этапом как отдельный `inventory-service`: это позволит сравнить синхронное взаимодействие request/response с асинхронными событиями, не превращая первый релиз в демонстрацию генерации protobuf-кода без понятной бизнес-причины.

## Быстрый запуск

Требования: Docker с Compose.

```bash
docker compose up --build -d
docker compose ps
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

Ответ `201 Created` содержит `id`. Повтор того же запроса с тем же `Idempotency-Key` вернёт тот же заказ с `200 OK`, не создавая новый.

Проверить состояние:

```bash
curl http://localhost:8080/v1/orders/<ORDER_ID>
```

Сразу после создания статус будет `new` или `processing`, затем — `completed`.

Интерфейсы:

- API: <http://localhost:8080>
- Prometheus: <http://localhost:9090>
- Grafana: <http://localhost:3000> (`admin` / `admin`)
- Jaeger: <http://localhost:16686>
- worker metrics: <http://localhost:8081/metrics>

Остановить:

```bash
docker compose down
```

Добавьте `-v`, только если хотите удалить локальный volume PostgreSQL.

## HTTP API

### `POST /v1/orders`

Обязательный заголовок: `Idempotency-Key`.

```json
{
  "customer_id": "customer-42",
  "items": [
    {"sku": "PHONE-001", "quantity": 1}
  ]
}
```

Коды ответа:

- `201` — новый заказ;
- `200` — повтор идемпотентного запроса;
- `400` — ошибка формата или валидации;
- `409` — недостаточно товара;
- `500` — внутренняя ошибка.

### `GET /v1/orders/{id}`

Возвращает заказ и позиции либо `404`.

### Health и metrics

- `GET /health/live` — процесс запущен;
- `GET /health/ready` — PostgreSQL доступен;
- `GET /metrics` — Prometheus exposition format.

## Почему код разбит именно так

`internal/order` содержит доменные типы, правила валидации и интерфейс repository. Он не знает о PostgreSQL, Kafka и HTTP. Поэтому правила можно быстро тестировать.

`internal/storage/postgres` реализует repository. Здесь находятся детали SQL: advisory lock для идемпотентности, атомарное резервирование остатков и запись outbox.

`internal/outbox` отвечает за повторяемую доставку событий. События арендуются через `FOR UPDATE SKIP LOCKED` и `locked_until`, поэтому позже можно запустить несколько экземпляров relay.

`internal/kafka` содержит producer и consumer. Сообщения одной partition всегда направляются в один worker lane. Это важно: Kafka commit хранит следующий offset, и параллельное завершение сообщений одной partition могло бы пропустить ещё не обработанное сообщение.

`internal/fulfillment` — идемпотентный обработчик события `order.created`. Запись в `processed_events`, освобождение резерва и смена статуса выполняются одной транзакцией.

`cmd/*` — composition root: здесь создаются зависимости, запускаются goroutine и обрабатываются `SIGINT`/`SIGTERM`.

## Транзакция создания заказа

1. Берётся transaction-scoped advisory lock по `Idempotency-Key`.
2. Проверяется, не был ли запрос выполнен раньше.
3. Каждая позиция резервируется условным `UPDATE ... WHERE available >= quantity`.
4. Записываются `orders` и `order_items`.
5. В `outbox_events` записывается `order.created`.
6. Выполняется `COMMIT`.

Если любой шаг падает, `ROLLBACK` отменяет всё, включая уже сделанные резервы.

Уровня `READ COMMITTED` достаточно: условный `UPDATE` блокирует строку inventory и повторно проверяет условие после ожидания блокировки. Это не позволяет двум транзакциям продать один и тот же остаток.

## Worker pool и гарантии

Topic создаётся с шестью partitions. Dispatcher вычисляет lane как `partition % WORKER_COUNT`, поэтому:

- разные partitions обрабатываются параллельно;
- внутри одной partition offsets идут последовательно;
- сообщение commit-ится только после успешной PostgreSQL-транзакции;
- временная ошибка повторяется с exponential backoff;
- повреждённое событие логируется и пропускается, чтобы не заблокировать partition навсегда.

В production повреждённые события обычно публикуют в DLQ. Это хорошее следующее улучшение.

## Тесты

Unit-тесты не требуют Docker:

```bash
go test ./...
go test -race ./...
go vet ./...
```

Integration-тест проверяет реальную транзакцию, резерв, переход статуса и повторную обработку одного события:

```bash
docker compose up -d postgres migrate
INTEGRATION_DATABASE_URL='postgres://fulfill:fulfill@localhost:55432/fulfill?sslmode=disable' \
  go test -tags=integration -count=1 ./tests/integration
```

Нагрузочный тест:

```bash
make load
```

По умолчанию k6 создаёт 20 заказов/сек в течение 30 секунд и проверяет `p95 < 300 ms`. Перед повторным большим прогоном пополните `CASE-001` или пересоздайте volume, потому что тест сознательно расходует inventory.

## Полезные команды

```bash
make up
make logs
make test
make test-race
make test-integration
make lint
make load
make down
```

## Что изучить в коде по порядку

1. `internal/order/service.go` — чистые бизнес-правила и dependency inversion.
2. `internal/storage/postgres/order_repository.go` — границы транзакции и конкурентный доступ.
3. `internal/outbox/relay.go` — почему возможны дубликаты, но не потеря события после commit.
4. `internal/kafka/consumer.go` — backpressure, ordering, retry и commit.
5. `internal/fulfillment/processor.go` — идемпотентность побочного эффекта.
6. `cmd/*/main.go` — lifecycle процесса и graceful shutdown.
7. `docker-compose.yml` — связи приложения и инфраструктуры.

## Идеи для следующих итераций

1. Выделить inventory в отдельный сервис и добавить protobuf/gRPC API.
2. Добавить DLQ и retry topics с ограничением количества попыток.
3. Протянуть W3C trace context через Kafka headers.
4. Добавить авторизацию и rate limiting.
5. Сгенерировать OpenAPI и контрактные тесты.
6. Добавить Kubernetes manifests, readiness/startup probes и HPA.
7. Проверить конкурентные сценарии через Testcontainers и `go test -race`.
