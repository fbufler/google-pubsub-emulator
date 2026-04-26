package usecases

import (
	"context"
	"sync"
	"time"

	"github.com/fbufler/google-pubsub/internal/core/entities"
	"github.com/fbufler/google-pubsub/internal/core/storage/repositories"
	"github.com/fbufler/google-pubsub/internal/core/types"
)

type dispatcherEntry struct {
	dispatcher *subscriptionDispatcher
	cancel     context.CancelFunc
}

type SubscriberUsecase struct {
	ctx             context.Context
	topics          *repositories.TopicRepository
	subscriptions   *repositories.SubscriptionRepository
	pendingMessages *repositories.PendingMessageRepository
	messages        *repositories.MessageRepository
	deliveryDelay   time.Duration
	dispatchers     sync.Map // types.FQDN → *dispatcherEntry
}

func NewSubscriber(
	ctx context.Context,
	topics *repositories.TopicRepository,
	subscriptions *repositories.SubscriptionRepository,
	pendingMessages *repositories.PendingMessageRepository,
	messages *repositories.MessageRepository,
	deliveryDelay time.Duration,
) *SubscriberUsecase {
	return &SubscriberUsecase{
		ctx:             ctx,
		topics:          topics,
		subscriptions:   subscriptions,
		pendingMessages: pendingMessages,
		messages:        messages,
		deliveryDelay:   deliveryDelay,
	}
}

func (s *SubscriberUsecase) CreateSubscription(ctx context.Context, sub *entities.Subscription) error {
	if err := sub.SetCreatedAt(time.Now()); err != nil {
		return types.WrapUsecaseError(types.UsecaseInternal, "set created at", err)
	}
	if _, err := s.topics.GetTopic(ctx, sub.TopicName()); err != nil {
		return fromPersistence(err)
	}
	if err := s.subscriptions.CreateSubscription(ctx, sub); err != nil {
		return fromPersistence(err)
	}
	s.pendingMessages.InitSubscription(ctx, sub.Name())
	s.startDispatcher(sub.Name())
	return nil
}

func (s *SubscriberUsecase) startDispatcher(subName types.FQDN) {
	dispCtx, cancel := context.WithCancel(s.ctx)
	d := newSubscriptionDispatcher(subName, s.subscriptions, s.pendingMessages, s.deliveryDelay)
	s.dispatchers.Store(subName, &dispatcherEntry{dispatcher: d, cancel: cancel})
	go d.run(dispCtx)
}

func (s *SubscriberUsecase) GetSubscription(ctx context.Context, name types.FQDN) (*entities.Subscription, error) {
	sub, err := s.subscriptions.GetSubscription(ctx, name)
	if err != nil {
		return nil, fromPersistence(err)
	}
	return sub, nil
}

func (s *SubscriberUsecase) UpdateSubscription(ctx context.Context, sub *entities.Subscription) error {
	if err := s.subscriptions.UpdateSubscription(ctx, sub); err != nil {
		return fromPersistence(err)
	}
	return nil
}

func (s *SubscriberUsecase) DeleteSubscription(ctx context.Context, name types.FQDN) error {
	if v, ok := s.dispatchers.LoadAndDelete(name); ok {
		v.(*dispatcherEntry).cancel()
	}
	if err := s.subscriptions.DeleteSubscription(ctx, name); err != nil {
		return fromPersistence(err)
	}
	s.pendingMessages.DropSubscription(ctx, name)
	return nil
}

func (s *SubscriberUsecase) ListSubscriptions(ctx context.Context, project string) ([]*entities.Subscription, error) {
	subs, err := s.subscriptions.ListSubscriptions(ctx, project)
	if err != nil {
		return nil, fromPersistence(err)
	}
	return subs, nil
}

func (s *SubscriberUsecase) ModifyPushConfig(ctx context.Context, subName types.FQDN, config *types.PushConfig) error {
	sub, err := s.subscriptions.GetSubscription(ctx, subName)
	if err != nil {
		return fromPersistence(err)
	}
	if err := sub.SetPushConfig(config); err != nil {
		return types.WrapUsecaseError(types.UsecaseInvalidArgument, "invalid push config", err)
	}
	if err := s.subscriptions.UpdateSubscription(ctx, sub); err != nil {
		return fromPersistence(err)
	}
	return nil
}

// StreamMessages registers the caller as a streaming consumer for subName.
// Messages are pushed to the returned channel as soon as they are enqueued.
// The caller must invoke the returned cancel func when done.
func (s *SubscriberUsecase) StreamMessages(ctx context.Context, subName types.FQDN) (<-chan []*entities.PendingMessage, func(), error) {
	v, ok := s.dispatchers.Load(subName)
	if !ok {
		return nil, nil, types.NewUsecaseError(types.UsecaseNotFound, "subscription not found")
	}
	ch := make(chan []*entities.PendingMessage, 16)
	cancel := v.(*dispatcherEntry).dispatcher.register(ch)
	return ch, cancel, nil
}

// Pull returns up to maxMessages pending messages for subName.
func (s *SubscriberUsecase) Pull(ctx context.Context, subName types.FQDN, maxMessages int32) ([]*entities.PendingMessage, error) {
	sub, err := s.subscriptions.GetSubscription(ctx, subName)
	if err != nil {
		return nil, fromPersistence(err)
	}
	result, err := s.pendingMessages.Pull(ctx, subName, int(maxMessages), sub.AckDeadline(), sub.EnableMessageOrdering())
	if err != nil {
		return nil, fromPersistence(err)
	}
	return result, nil
}

func (s *SubscriberUsecase) Acknowledge(ctx context.Context, subName types.FQDN, ackIDs []string) error {
	if err := s.pendingMessages.AcknowledgeByAckID(ctx, subName, ackIDs); err != nil {
		return fromPersistence(err)
	}
	return nil
}

func (s *SubscriberUsecase) ModifyAckDeadline(ctx context.Context, subName types.FQDN, ackIDs []string, deadline time.Duration) error {
	if err := s.pendingMessages.UpdateDeadline(ctx, subName, ackIDs, deadline); err != nil {
		return fromPersistence(err)
	}
	return nil
}

// HandleDeadLetters filters msgs that have exceeded the subscription's max delivery
// attempts, forwards them to the dead-letter topic, and acknowledges them.
// Returns only the messages that should still be delivered.
func (s *SubscriberUsecase) HandleDeadLetters(ctx context.Context, subName types.FQDN, msgs []*entities.PendingMessage) ([]*entities.PendingMessage, error) {
	sub, err := s.GetSubscription(ctx, subName)
	if err != nil || sub.DeadLetterPolicy() == nil {
		return msgs, nil
	}
	max := sub.DeadLetterPolicy().MaxDeliveryAttempts
	dlqTopic := types.FQDN(sub.DeadLetterPolicy().DeadLetterTopic)

	var keep []*entities.PendingMessage
	var dlqMsgs []*entities.Message
	var dlqAckIDs []string

	for _, pm := range msgs {
		if max > 0 && pm.DeliveryAttempt() >= max {
			dlqMsgs = append(dlqMsgs, pm.Message())
			dlqAckIDs = append(dlqAckIDs, pm.AckID())
		} else {
			keep = append(keep, pm)
		}
	}

	if len(dlqMsgs) > 0 {
		now := time.Now()
		for _, m := range dlqMsgs {
			id := newMsgID()
			if err := m.SetID(id); err != nil {
				return nil, types.WrapUsecaseError(types.UsecaseInternal, "set dead letter message id", err)
			}
			if err := m.SetPublishTime(now); err != nil {
				return nil, types.WrapUsecaseError(types.UsecaseInternal, "set dead letter message publish time", err)
			}
			key := types.FQDN(dlqTopic.String() + "/messages/" + id)
			if err := s.messages.StoreMessage(ctx, key, m); err != nil {
				return nil, fromPersistence(err)
			}
		}

		dlqSubs, err := s.subscriptions.ListSubscriptionsByTopic(ctx, dlqTopic)
		if err != nil {
			return nil, fromPersistence(err)
		}
		for _, dlqSub := range dlqSubs {
			pendingMsgs := make([]*entities.PendingMessage, 0, len(dlqMsgs))
			for _, m := range dlqMsgs {
				pm, err := newPendingMessage(m, dlqSub.Name())
				if err != nil {
					return nil, err
				}
				pendingMsgs = append(pendingMsgs, pm)
			}
			if err := s.pendingMessages.Enqueue(ctx, dlqSub.Name(), pendingMsgs); err != nil {
				return nil, fromPersistence(err)
			}
		}
		if err := s.Acknowledge(ctx, subName, dlqAckIDs); err != nil {
			return nil, err
		}
	}

	return keep, nil
}
