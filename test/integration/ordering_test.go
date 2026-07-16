//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
)

// mustCreateOrderedSubscription creates a subscription with message ordering enabled.
func mustCreateOrderedSubscription(t *testing.T, client *pubsub.Client, id string, topic *pubsub.Topic) *pubsub.Subscription {
	t.Helper()
	ctx := context.Background()
	sub, err := client.CreateSubscription(ctx, id, pubsub.SubscriptionConfig{
		Topic:                 topic,
		AckDeadline:           10 * time.Second,
		EnableMessageOrdering: true,
	})
	if err != nil {
		t.Fatalf("CreateSubscription(%q): %v", id, err)
	}
	t.Cleanup(func() { _ = sub.Delete(context.Background()) })
	return sub
}

// TestOrdering_MultipleSubscribers verifies that when several subscriber streams
// are connected to one subscription, messages sharing an ordering key are still
// delivered to the client in publish order.
func TestOrdering_MultipleSubscribers(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()
	topic := mustCreateTopic(t, client, uniqueName("topic"))
	topic.EnableMessageOrdering = true
	sub := mustCreateOrderedSubscription(t, client, uniqueName("sub"), topic)

	const (
		keys        = 4
		perKey      = 25
		totalWanted = keys * perKey
	)

	// Publish `perKey` messages per ordering key. Within a key the client sends
	// them sequentially, so the emulator must preserve that order.
	var wg sync.WaitGroup
	for k := 0; k < keys; k++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", k)
			for i := 0; i < perKey; i++ {
				res := topic.Publish(ctx, &pubsub.Message{
					Data:        []byte(strconv.Itoa(i)),
					OrderingKey: key,
				})
				if _, err := res.Get(ctx); err != nil {
					t.Errorf("Publish key=%s i=%d: %v", key, i, err)
					return
				}
			}
		}(k)
	}
	wg.Wait()

	// Receive with multiple concurrent streams — this is the "multiple
	// subscribers connected" scenario from the bug report.
	sub.ReceiveSettings.NumGoroutines = 5

	var mu sync.Mutex
	received := make(map[string][]int)
	var count int

	recvCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	err := sub.Receive(recvCtx, func(ctx context.Context, msg *pubsub.Message) {
		n, _ := strconv.Atoi(string(msg.Data))
		msg.Ack()
		mu.Lock()
		received[msg.OrderingKey] = append(received[msg.OrderingKey], n)
		count++
		if count >= totalWanted {
			cancel()
		}
		mu.Unlock()
	})
	if err != nil && err != context.Canceled {
		t.Fatalf("Receive: %v", err)
	}

	if count < totalWanted {
		t.Fatalf("received %d messages, want %d", count, totalWanted)
	}

	// Every ordering key must have arrived as a strictly increasing 0..perKey-1.
	for k := 0; k < keys; k++ {
		key := fmt.Sprintf("key-%d", k)
		got := received[key]
		if len(got) != perKey {
			t.Errorf("key %s: got %d messages, want %d", key, len(got), perKey)
			continue
		}
		for i, v := range got {
			if v != i {
				t.Errorf("key %s out of order at position %d: got %d, want %d (full sequence: %v)", key, i, v, i, got)
				break
			}
		}
	}
}
