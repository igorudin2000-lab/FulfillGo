package outbox

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"
)

type storeStub struct {
	events    []Event
	published []string
	failed    []string
}

func (store *storeStub) Claim(context.Context, int, time.Duration) ([]Event, error) {
	return store.events, nil
}

func (store *storeStub) MarkPublished(_ context.Context, id string) error {
	store.published = append(store.published, id)
	return nil
}

func (store *storeStub) MarkFailed(_ context.Context, id string, _ error) error {
	store.failed = append(store.failed, id)
	return nil
}

type publisherStub struct {
	failID string
}

func (publisher publisherStub) Publish(_ context.Context, event Event) error {
	if event.ID == publisher.failID {
		return errors.New("kafka unavailable")
	}
	return nil
}

func (publisherStub) Close() error { return nil }

func TestRelayPublishBatchMarksResults(t *testing.T) {
	t.Parallel()

	store := &storeStub{events: []Event{{ID: "ok"}, {ID: "failed"}}}
	relay := NewRelay(
		store,
		publisherStub{failID: "failed"},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		time.Second,
		10,
	)

	if err := relay.publishBatch(context.Background()); err != nil {
		t.Fatalf("publishBatch() error = %v", err)
	}
	if len(store.published) != 1 || store.published[0] != "ok" {
		t.Fatalf("published = %v, want [ok]", store.published)
	}
	if len(store.failed) != 1 || store.failed[0] != "failed" {
		t.Fatalf("failed = %v, want [failed]", store.failed)
	}
}
