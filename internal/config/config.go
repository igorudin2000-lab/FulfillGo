package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	HTTPAddress        string
	MetricsAddress     string
	DatabaseURL        string
	KafkaBrokers       []string
	KafkaTopic         string
	KafkaConsumerGroup string
	WorkerCount        int
	ShutdownTimeout    time.Duration
	OutboxInterval     time.Duration
	OutboxBatchSize    int
	FulfillmentDelay   time.Duration
	OTLPEndpoint       string
}

func Load() (Config, error) {
	config := Config{
		HTTPAddress:        env("HTTP_ADDRESS", ":8080"),
		MetricsAddress:     env("METRICS_ADDRESS", ":8081"),
		DatabaseURL:        env("DATABASE_URL", "postgres://fulfill:fulfill@localhost:5432/fulfill?sslmode=disable"),
		KafkaBrokers:       splitCSV(env("KAFKA_BROKERS", "localhost:9092")),
		KafkaTopic:         env("KAFKA_TOPIC", "orders.v1"),
		KafkaConsumerGroup: env("KAFKA_CONSUMER_GROUP", "fulfillment-worker-v1"),
		OTLPEndpoint:       os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
	}

	var err error
	if config.WorkerCount, err = envInt("WORKER_COUNT", 4); err != nil {
		return Config{}, err
	}
	if config.OutboxBatchSize, err = envInt("OUTBOX_BATCH_SIZE", 100); err != nil {
		return Config{}, err
	}
	if config.ShutdownTimeout, err = envDuration("SHUTDOWN_TIMEOUT", 15*time.Second); err != nil {
		return Config{}, err
	}
	if config.OutboxInterval, err = envDuration("OUTBOX_INTERVAL", 500*time.Millisecond); err != nil {
		return Config{}, err
	}
	if config.FulfillmentDelay, err = envDuration("FULFILLMENT_DELAY", 100*time.Millisecond); err != nil {
		return Config{}, err
	}
	if config.WorkerCount < 1 {
		return Config{}, fmt.Errorf("WORKER_COUNT must be positive")
	}
	return config, nil
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) (int, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", key, err)
	}
	return parsed, nil
}

func envDuration(key string, fallback time.Duration) (time.Duration, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be a duration: %w", key, err)
	}
	return parsed, nil
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}
