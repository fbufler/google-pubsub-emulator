//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	pubsubpb "cloud.google.com/go/pubsub/apiv1/pubsubpb"
)

// TestOrdering_KeyStreamAffinity verifies that when several StreamingPull streams
// are open on one ordered subscription, all messages sharing an ordering key are
// delivered on a single stream. Real Pub/Sub pins an ordering key to one stream
// so that a subscriber reading the streams independently still observes order.
//
// This uses raw StreamingPull streams (rather than the smart client, which hides
// the problem by re-serialising per key on the client side).
func TestOrdering_KeyStreamAffinity(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()
	topic := mustCreateTopic(t, client, uniqueName("topic"))
	topic.EnableMessageOrdering = true
	sub := mustCreateOrderedSubscription(t, client, uniqueName("sub"), topic)
	subName := fqSub(sub)

	const (
		keys   = 4
		perKey = 5
		total  = keys * perKey
	)

	// Publish perKey ordered messages for each key.
	for k := 0; k < keys; k++ {
		key := fmt.Sprintf("key-%d", k)
		for i := 0; i < perKey; i++ {
			res := topic.Publish(ctx, &pubsub.Message{Data: []byte(strconv.Itoa(i)), OrderingKey: key})
			if _, err := res.Get(ctx); err != nil {
				t.Fatalf("Publish key=%s i=%d: %v", key, i, err)
			}
		}
	}

	raw := newRawSubscriberClient(t)

	runCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var (
		mu          sync.Mutex
		keyToStream = map[string]int{} // ordering key -> stream index that delivered it
		seen        = map[string]bool{}
		delivered   int
		violations  []string
	)

	// runStream opens one StreamingPull stream, acking everything it receives and
	// recording which ordering key arrived on which stream.
	runStream := func(streamIdx int) error {
		stream, err := raw.StreamingPull(runCtx)
		if err != nil {
			return err
		}
		if err := stream.Send(&pubsubpb.StreamingPullRequest{
			Subscription:             subName,
			StreamAckDeadlineSeconds: 10,
		}); err != nil {
			return err
		}
		for {
			resp, err := stream.Recv()
			if err == io.EOF || runCtx.Err() != nil {
				return nil
			}
			if err != nil {
				return nil
			}
			var ackIDs []string
			mu.Lock()
			for _, rm := range resp.ReceivedMessages {
				ackIDs = append(ackIDs, rm.AckId)
				key := rm.Message.OrderingKey
				if prev, ok := keyToStream[key]; ok && prev != streamIdx {
					violations = append(violations, fmt.Sprintf("key %s delivered on streams %d and %d", key, prev, streamIdx))
				} else {
					keyToStream[key] = streamIdx
				}
				if id := rm.Message.MessageId; !seen[id] {
					seen[id] = true
					delivered++
				}
			}
			if delivered >= total {
				cancel()
			}
			mu.Unlock()
			if len(ackIDs) > 0 {
				_ = stream.Send(&pubsubpb.StreamingPullRequest{AckIds: ackIDs})
			}
		}
	}

	var wg sync.WaitGroup
	for s := 0; s < 2; s++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_ = runStream(idx)
		}(s)
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if delivered < total {
		t.Fatalf("delivered %d/%d messages", delivered, total)
	}
	if len(violations) > 0 {
		t.Fatalf("ordering key split across streams (order not preserved for independent subscribers):\n  %v", violations)
	}
}
