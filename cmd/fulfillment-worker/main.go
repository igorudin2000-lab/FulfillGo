package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/igor/fulfill-go/internal/config"
	"github.com/igor/fulfill-go/internal/fulfillment"
	kafkaclient "github.com/igor/fulfill-go/internal/kafka"
	"github.com/igor/fulfill-go/internal/observability"
	"github.com/igor/fulfill-go/internal/storage/postgres"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	if err := run(logger); err != nil {
		logger.Error("fulfillment-worker stopped", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	signalContext, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()

	traceShutdown, err := observability.SetupTracing(signalContext, "fulfillment-worker", cfg.OTLPEndpoint)
	if err != nil {
		return err
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = traceShutdown(ctx)
	}()

	pool, err := postgres.Open(signalContext, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	metrics := observability.NewMetrics(prometheus.DefaultRegisterer)
	processor := fulfillment.NewProcessor(pool, cfg.FulfillmentDelay)
	consumer := kafkaclient.NewConsumer(
		cfg.KafkaBrokers,
		cfg.KafkaTopic,
		cfg.KafkaConsumerGroup,
		cfg.WorkerCount,
		processor,
		metrics,
		logger,
	)
	defer func() { _ = consumer.Close() }()

	metricsMux := http.NewServeMux()
	metricsMux.Handle("GET /metrics", promhttp.Handler())
	metricsMux.HandleFunc("GET /health/live", func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusOK)
		_, _ = response.Write([]byte("ok\n"))
	})
	metricsServer := &http.Server{
		Addr:              cfg.MetricsAddress,
		Handler:           metricsMux,
		ReadHeaderTimeout: 3 * time.Second,
	}
	metricsErrors := make(chan error, 1)
	go func() {
		logger.Info("worker metrics server started", "address", cfg.MetricsAddress)
		metricsErrors <- metricsServer.ListenAndServe()
	}()

	consumerErrors := make(chan error, 1)
	go func() {
		logger.Info("fulfillment consumer started", "workers", cfg.WorkerCount)
		consumerErrors <- consumer.Run(signalContext)
	}()

	select {
	case <-signalContext.Done():
		logger.Info("shutdown signal received")
	case err := <-consumerErrors:
		if err != nil {
			return err
		}
	case err := <-metricsErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}

	shutdownContext, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	return metricsServer.Shutdown(shutdownContext)
}
