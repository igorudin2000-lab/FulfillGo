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

	"github.com/igor/fulfill-go/internal/config"
	kafkaclient "github.com/igor/fulfill-go/internal/kafka"
	"github.com/igor/fulfill-go/internal/observability"
	"github.com/igor/fulfill-go/internal/order"
	"github.com/igor/fulfill-go/internal/outbox"
	"github.com/igor/fulfill-go/internal/storage/postgres"
	httptransport "github.com/igor/fulfill-go/internal/transport/http"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	if err := run(logger); err != nil {
		logger.Error("order-service stopped", "error", err)
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

	traceShutdown, err := observability.SetupTracing(signalContext, "order-service", cfg.OTLPEndpoint)
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
	repository := postgres.NewOrderRepository(pool)
	service := order.NewService(repository)
	handler := httptransport.NewHandler(service, pool, metrics, logger)

	producer := kafkaclient.NewProducer(cfg.KafkaBrokers, cfg.KafkaTopic)
	defer func() { _ = producer.Close() }()
	relay := outbox.NewRelay(
		postgres.NewOutboxStore(pool),
		producer,
		logger,
		cfg.OutboxInterval,
		cfg.OutboxBatchSize,
	)
	go relay.Run(signalContext)

	server := &http.Server{
		Addr:              cfg.HTTPAddress,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	serverErrors := make(chan error, 1)
	go func() {
		logger.Info("order-service started", "address", cfg.HTTPAddress)
		serverErrors <- server.ListenAndServe()
	}()

	select {
	case <-signalContext.Done():
		logger.Info("shutdown signal received")
	case err := <-serverErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}

	shutdownContext, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	return server.Shutdown(shutdownContext)
}
