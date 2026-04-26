package usecases

import (
	"context"
	"time"

	"github.com/fbufler/google-pubsub/internal/core/entities"
	"github.com/fbufler/google-pubsub/internal/core/storage/repositories"
	"github.com/fbufler/google-pubsub/internal/core/types"
)

type PublisherUsecase struct {
	topics          *repositories.TopicRepository
	messages        *repositories.MessageRepository
	subscriptions   *repositories.SubscriptionRepository
	pendingMessages *repositories.PendingMessageRepository
}

func NewPublisher(
	topics *repositories.TopicRepository,
	messages *repositories.MessageRepository,
	subscriptions *repositories.SubscriptionRepository,
	pendingMessages *repositories.PendingMessageRepository,
) *PublisherUsecase {
	return &PublisherUsecase{
		topics:          topics,
		messages:        messages,
		subscriptions:   subscriptions,
		pendingMessages: pendingMessages,
	}
}

// Publish assigns IDs and publish timestamps to msgs, stores them under topicName,
// and fans them out to all subscriptions on that topic.
// Returns the assigned message IDs in the same order as msgs.
func (pub *PublisherUsecase) Publish(ctx context.Context, topicName types.FQDN, msgs []*entities.Message) ([]string, error) {
	if _, err := pub.topics.GetTopic(ctx, topicName); err != nil {
		return nil, fromPersistence(err)
	}

	now := time.Now()
	ids := make([]string, 0, len(msgs))
	for _, m := range msgs {
		id := newMsgID()
		if err := m.SetID(id); err != nil {
			return nil, types.WrapUsecaseError(types.UsecaseInternal, "set message id", err)
		}
		if err := m.SetPublishTime(now); err != nil {
			return nil, types.WrapUsecaseError(types.UsecaseInternal, "set message publish time", err)
		}
		ids = append(ids, id)

		key := types.FQDN(topicName.String() + "/messages/" + id)
		if err := pub.messages.StoreMessage(ctx, key, m); err != nil {
			return nil, fromPersistence(err)
		}
	}

	subs, err := pub.subscriptions.ListSubscriptionsByTopic(ctx, topicName)
	if err != nil {
		return nil, fromPersistence(err)
	}

	// Fan-out runs in a separate goroutine so that the Publish response is sent
	// to the client before any subscription is notified of new messages.
	// This matches real PubSub semantics: Publish acknowledges once the message
	// is stored; delivery to subscribers is always asynchronous.
	go func() {
		for _, sub := range subs {
			go func(sub *entities.Subscription) {
				pendingMsgs := make([]*entities.PendingMessage, 0, len(msgs))
				for _, m := range msgs {
					if !matchesFilter(sub.Filter(), m.Attributes()) {
						continue
					}
					pm, err := newPendingMessage(m, sub.Name())
					if err != nil {
						return
					}
					pendingMsgs = append(pendingMsgs, pm)
				}
				pub.pendingMessages.Enqueue(ctx, sub.Name(), pendingMsgs) //nolint:errcheck
			}(sub)
		}
	}()

	return ids, nil
}
