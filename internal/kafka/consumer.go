package kafka

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/igor/fulfill-go/internal/fulfillment"
	"github.com/igor/fulfill-go/internal/observability"
)

type MessageHandler interface {
	Process(context.Context, []byte) error
}

type Consumer struct {
	reader      *kafkago.Reader
	handler     MessageHandler
	metrics     *observability.Metrics
	logger      *slog.Logger
	workerCount int
}

func NewConsumer(
	brokers []string,
	topic string,
	groupID string,
	workerCount int,
	handler MessageHandler,
	metrics *observability.Metrics,
	logger *slog.Logger,
) *Consumer {
	return &Consumer{
		reader: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers:        brokers,
			GroupID:        groupID,
			Topic:          topic,
			MinBytes:       1,
			MaxBytes:       10e6,
			MaxWait:        500 * time.Millisecond,
			CommitInterval: 0,
			StartOffset:    kafkago.FirstOffset,
		}),
		handler:     handler,
		metrics:     metrics,
		logger:      logger,
		workerCount: workerCount,
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	lanes := make([]chan kafkago.Message, c.workerCount)
	var workers sync.WaitGroup
	for index := range lanes {
		lanes[index] = make(chan kafkago.Message, 64)
		workers.Add(1)
		go func(workerID int, messages <-chan kafkago.Message) {
			defer workers.Done()
			c.runWorker(ctx, workerID, messages)
		}(index, lanes[index])
	}
	defer func() {
		for _, lane := range lanes {
			close(lane)
		}
		workers.Wait()
	}()

	for {
		message, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return nil
			}
			return err
		}

		// A partition always goes to one lane, preserving its offset order.
		lane := message.Partition % c.workerCount
		c.metrics.WorkerQueueDepth.Inc()
		select {
		case lanes[lane] <- message:
		case <-ctx.Done():
			c.metrics.WorkerQueueDepth.Dec()
			return nil
		}
	}
}

func (c *Consumer) runWorker(ctx context.Context, workerID int, messages <-chan kafkago.Message) {
	for message := range messages {
		c.metrics.WorkerQueueDepth.Dec()
		if !c.processWithRetry(ctx, workerID, message) {
			return
		}
	}
}

func (c *Consumer) processWithRetry(ctx context.Context, workerID int, message kafkago.Message) bool {
	backoff := 250 * time.Millisecond
	for {
		err := c.handler.Process(ctx, message.Value)
		if err == nil {
			if err := c.reader.CommitMessages(ctx, message); err != nil {
				c.logger.Error("commit kafka message failed",
					"worker_id", workerID,
					"partition", message.Partition,
					"offset", message.Offset,
					"error", err,
				)
				continue
			}
			c.metrics.KafkaProcessed.WithLabelValues("success").Inc()
			return true
		}

		if errors.Is(err, fulfillment.ErrInvalidEvent) {
			c.logger.Error("discarding invalid kafka message",
				"worker_id", workerID,
				"partition", message.Partition,
				"offset", message.Offset,
				"error", err,
			)
			if commitErr := c.reader.CommitMessages(ctx, message); commitErr != nil {
				continue
			}
			c.metrics.KafkaProcessed.WithLabelValues("discarded").Inc()
			return true
		}

		c.metrics.KafkaProcessed.WithLabelValues("retry").Inc()
		c.logger.Error("kafka message processing failed; retrying",
			"worker_id", workerID,
			"partition", message.Partition,
			"offset", message.Offset,
			"backoff", backoff,
			"error", err,
		)
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false
		case <-timer.C:
		}
		if backoff < 10*time.Second {
			backoff *= 2
		}
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
