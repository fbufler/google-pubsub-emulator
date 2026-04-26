package usecases

import (
	"context"
	"sync"
	"time"

	"github.com/fbufler/google-pubsub/internal/core/entities"
	"github.com/fbufler/google-pubsub/internal/core/storage/repositories"
	"github.com/fbufler/google-pubsub/internal/core/types"
)

// subscriptionDispatcher pushes messages from a subscription's queue directly to
// registered streaming-pull consumers without any polling interval.
// One dispatcher runs per subscription for the lifetime of that subscription.
type subscriptionDispatcher struct {
	subName       types.FQDN
	subscriptions *repositories.SubscriptionRepository
	pendingMsgs   *repositories.PendingMessageRepository
	deliveryDelay time.Duration

	mu        sync.Mutex
	nextIdx   int
	consumers []chan<- []*entities.PendingMessage

	trigger chan struct{} // fired by register() so existing messages are drained immediately
}

func newSubscriptionDispatcher(
	subName types.FQDN,
	subscriptions *repositories.SubscriptionRepository,
	pendingMsgs *repositories.PendingMessageRepository,
	deliveryDelay time.Duration,
) *subscriptionDispatcher {
	return &subscriptionDispatcher{
		subName:       subName,
		subscriptions: subscriptions,
		pendingMsgs:   pendingMsgs,
		deliveryDelay: deliveryDelay,
		trigger:       make(chan struct{}, 1),
	}
}

// register adds a consumer and returns a deregister function.
// It immediately triggers a delivery so any already-queued messages are pushed
// without waiting for the next Enqueue or requeue tick.
func (d *subscriptionDispatcher) register(ch chan<- []*entities.PendingMessage) func() {
	d.mu.Lock()
	d.consumers = append(d.consumers, ch)
	d.mu.Unlock()

	select {
	case d.trigger <- struct{}{}:
	default:
	}

	return func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		for i, c := range d.consumers {
			if c == ch {
				d.consumers = append(d.consumers[:i], d.consumers[i+1:]...)
				return
			}
		}
	}
}

// run is the dispatcher's main loop. It reacts to three events:
//   - a message is enqueued (queue notify channel)
//   - register() was called with queued messages already waiting (trigger channel)
//   - the periodic requeue ticker fires (to redeliver expired leases)
func (d *subscriptionDispatcher) run(ctx context.Context) {
	notify, ok := d.pendingMsgs.Watch(d.subName)
	if !ok {
		return
	}

	requeue := time.NewTicker(500 * time.Millisecond)
	defer requeue.Stop()

	for {
		select {
		case <-ctx.Done():
			d.mu.Lock()
			for _, ch := range d.consumers {
				close(ch)
			}
			d.consumers = nil
			d.mu.Unlock()
			return
		case <-notify:
			if d.deliveryDelay > 0 {
				time.Sleep(d.deliveryDelay)
			}
			d.deliver(ctx)
		case <-d.trigger:
			if d.deliveryDelay > 0 {
				time.Sleep(d.deliveryDelay)
			}
			d.deliver(ctx)
		case <-requeue.C:
			d.deliver(ctx)
		}
	}
}

// deliver pulls all available messages and pushes them to one registered consumer
// using round-robin. If all consumer channels are full the messages remain
// in-flight and will be requeued when their ack deadline expires.
func (d *subscriptionDispatcher) deliver(ctx context.Context) {
	d.mu.Lock()
	n := len(d.consumers)
	d.mu.Unlock()
	if n == 0 {
		return
	}

	sub, err := d.subscriptions.GetSubscription(ctx, d.subName)
	if err != nil {
		return
	}

	msgs, err := d.pendingMsgs.Pull(ctx, d.subName, 1000, sub.AckDeadline(), sub.EnableMessageOrdering())
	if err != nil || len(msgs) == 0 {
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.consumers) == 0 {
		// Messages are now in-flight; they'll be requeued at ack deadline.
		return
	}
	// Try each consumer once (non-blocking), round-robin starting from nextIdx.
	for i := 0; i < len(d.consumers); i++ {
		idx := d.nextIdx % len(d.consumers)
		d.nextIdx++
		select {
		case d.consumers[idx] <- msgs:
			return
		default:
		}
	}
	// All consumer channels full; messages remain in-flight.
}
