package handler

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/fbufler/google-pubsub/internal/api/mapper"
	"github.com/fbufler/google-pubsub/internal/api/payload"
	"github.com/fbufler/google-pubsub/internal/core/entities"
	"github.com/fbufler/google-pubsub/internal/core/types"
	"github.com/fbufler/google-pubsub/internal/core/usecases"
)

func NewCreateSubscription(_ context.Context, uc *usecases.SubscriberUsecase) func(context.Context, *connect.Request[pubsubpb.Subscription]) (*connect.Response[pubsubpb.Subscription], error) {
	return func(ctx context.Context, req *connect.Request[pubsubpb.Subscription]) (*connect.Response[pubsubpb.Subscription], error) {
		request := payload.NewCreateSubscriptionRequest(ctx, req)

		sub, err := mapper.ProtoToSubscription(request.Payload())
		if err != nil {
			return nil, toConnectError(err)
		}

		if err := uc.CreateSubscription(ctx, sub); err != nil {
			logErr(err, "create subscription failed", "subscription", request.Payload().Name)
			return nil, toConnectError(err)
		}
		slog.Debug("subscription created", "subscription", request.Payload().Name, "topic", request.Payload().Topic)

		return payload.NewCreateSubscriptionResponse(ctx, mapper.SubscriptionToProto(sub)).Encode(), nil
	}
}

func NewGetSubscription(_ context.Context, uc *usecases.SubscriberUsecase) func(context.Context, *connect.Request[pubsubpb.GetSubscriptionRequest]) (*connect.Response[pubsubpb.Subscription], error) {
	return func(ctx context.Context, req *connect.Request[pubsubpb.GetSubscriptionRequest]) (*connect.Response[pubsubpb.Subscription], error) {
		request := payload.NewGetSubscriptionRequest(ctx, req)

		sub, err := uc.GetSubscription(ctx, types.FQDN(request.Payload().Subscription))
		if err != nil {
			logErr(err, "get subscription failed", "subscription", request.Payload().Subscription)
			return nil, toConnectError(err)
		}
		slog.Debug("subscription retrieved", "subscription", request.Payload().Subscription)

		return payload.NewGetSubscriptionResponse(ctx, mapper.SubscriptionToProto(sub)).Encode(), nil
	}
}

func NewUpdateSubscription(_ context.Context, uc *usecases.SubscriberUsecase) func(context.Context, *connect.Request[pubsubpb.UpdateSubscriptionRequest]) (*connect.Response[pubsubpb.Subscription], error) {
	return func(ctx context.Context, req *connect.Request[pubsubpb.UpdateSubscriptionRequest]) (*connect.Response[pubsubpb.Subscription], error) {
		request := payload.NewUpdateSubscriptionRequest(ctx, req)
		msg := request.Payload()

		if msg.Subscription == nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("subscription is required"))
		}

		existing, err := uc.GetSubscription(ctx, types.FQDN(msg.Subscription.Name))
		if err != nil {
			logErr(err, "update subscription: get failed", "subscription", msg.Subscription.Name)
			return nil, toConnectError(err)
		}

		for _, path := range msg.UpdateMask.GetPaths() {
			switch path {
			case "ack_deadline_seconds":
				if err := existing.SetAckDeadline(time.Duration(msg.Subscription.AckDeadlineSeconds) * time.Second); err != nil {
					return nil, toConnectError(err)
				}
			case "labels":
				if err := existing.SetLabels(types.Labels(msg.Subscription.Labels)); err != nil {
					return nil, toConnectError(err)
				}
			case "message_retention_duration":
				if msg.Subscription.MessageRetentionDuration != nil {
					if err := existing.SetMessageRetention(msg.Subscription.MessageRetentionDuration.AsDuration()); err != nil {
						return nil, toConnectError(err)
					}
				}
			case "retain_acked_messages":
				if err := existing.SetRetainAckedMessages(msg.Subscription.RetainAckedMessages); err != nil {
					return nil, toConnectError(err)
				}
			case "push_config":
				if msg.Subscription.PushConfig != nil {
					if err := existing.SetPushConfig(&types.PushConfig{
						Endpoint:   msg.Subscription.PushConfig.PushEndpoint,
						Attributes: msg.Subscription.PushConfig.Attributes,
					}); err != nil {
						return nil, toConnectError(err)
					}
				}
			case "dead_letter_policy":
				if msg.Subscription.DeadLetterPolicy != nil {
					if err := existing.SetDeadLetterPolicy(&types.DeadLetterPolicy{
						DeadLetterTopic:     msg.Subscription.DeadLetterPolicy.DeadLetterTopic,
						MaxDeliveryAttempts: msg.Subscription.DeadLetterPolicy.MaxDeliveryAttempts,
					}); err != nil {
						return nil, toConnectError(err)
					}
				}
			case "retry_policy":
				if msg.Subscription.RetryPolicy != nil {
					rp := &types.RetryPolicy{}
					if msg.Subscription.RetryPolicy.MinimumBackoff != nil {
						rp.MinimumBackoff = msg.Subscription.RetryPolicy.MinimumBackoff.AsDuration()
					}
					if msg.Subscription.RetryPolicy.MaximumBackoff != nil {
						rp.MaximumBackoff = msg.Subscription.RetryPolicy.MaximumBackoff.AsDuration()
					}
					if err := existing.SetRetryPolicy(rp); err != nil {
						return nil, toConnectError(err)
					}
				}
			}
		}

		if err := uc.UpdateSubscription(ctx, existing); err != nil {
			logErr(err, "update subscription failed", "subscription", msg.Subscription.Name)
			return nil, toConnectError(err)
		}
		slog.Debug("subscription updated", "subscription", msg.Subscription.Name)

		return payload.NewUpdateSubscriptionResponse(ctx, mapper.SubscriptionToProto(existing)).Encode(), nil
	}
}

func NewDeleteSubscription(_ context.Context, uc *usecases.SubscriberUsecase) func(context.Context, *connect.Request[pubsubpb.DeleteSubscriptionRequest]) (*connect.Response[emptypb.Empty], error) {
	return func(ctx context.Context, req *connect.Request[pubsubpb.DeleteSubscriptionRequest]) (*connect.Response[emptypb.Empty], error) {
		request := payload.NewDeleteSubscriptionRequest(ctx, req)

		if err := uc.DeleteSubscription(ctx, types.FQDN(request.Payload().Subscription)); err != nil {
			logErr(err, "delete subscription failed", "subscription", request.Payload().Subscription)
			return nil, toConnectError(err)
		}
		slog.Debug("subscription deleted", "subscription", request.Payload().Subscription)

		return payload.NewEmptyResponse(ctx).Encode(), nil
	}
}

func NewListSubscriptions(_ context.Context, uc *usecases.SubscriberUsecase) func(context.Context, *connect.Request[pubsubpb.ListSubscriptionsRequest]) (*connect.Response[pubsubpb.ListSubscriptionsResponse], error) {
	return func(ctx context.Context, req *connect.Request[pubsubpb.ListSubscriptionsRequest]) (*connect.Response[pubsubpb.ListSubscriptionsResponse], error) {
		request := payload.NewListSubscriptionsRequest(ctx, req)

		subs, err := uc.ListSubscriptions(ctx, projectID(request.Payload().Project))
		if err != nil {
			logErr(err, "list subscriptions failed", "project", request.Payload().Project)
			return nil, toConnectError(err)
		}

		resp := &pubsubpb.ListSubscriptionsResponse{}
		for _, sub := range subs {
			resp.Subscriptions = append(resp.Subscriptions, mapper.SubscriptionToProto(sub))
		}
		slog.Debug("subscriptions listed", "project", request.Payload().Project, "count", len(resp.Subscriptions))

		return payload.NewListSubscriptionsResponse(ctx, resp).Encode(), nil
	}
}

func NewPull(_ context.Context, uc *usecases.SubscriberUsecase) func(context.Context, *connect.Request[pubsubpb.PullRequest]) (*connect.Response[pubsubpb.PullResponse], error) {
	return func(ctx context.Context, req *connect.Request[pubsubpb.PullRequest]) (*connect.Response[pubsubpb.PullResponse], error) {
		request := payload.NewPullRequest(ctx, req)
		msg := request.Payload()

		max := msg.MaxMessages
		if max <= 0 {
			max = 100
		}

		subName := types.FQDN(msg.Subscription)
		msgs, err := uc.Pull(ctx, subName, max)
		if err != nil {
			logErr(err, "pull failed", "subscription", msg.Subscription)
			return nil, toConnectError(err)
		}

		msgs, err = uc.HandleDeadLetters(ctx, subName, msgs)
		if err != nil {
			logErr(err, "dead letter handling failed", "subscription", msg.Subscription)
			return nil, toConnectError(err)
		}

		resp := &pubsubpb.PullResponse{}
		for _, pm := range msgs {
			resp.ReceivedMessages = append(resp.ReceivedMessages, mapper.PendingMessageToProto(pm))
		}
		slog.Debug("messages pulled", "subscription", msg.Subscription, "count", len(resp.ReceivedMessages))
		for _, pm := range msgs {
			slog.Debug("message pulled", "subscription", msg.Subscription, "id", pm.Message().ID(), "data", string(pm.Message().Data()), "attributes", pm.Message().Attributes(), "orderingKey", pm.Message().OrderingKey(), "ackID", pm.AckID())
		}

		return payload.NewPullResponse(ctx, resp).Encode(), nil
	}
}

func NewAcknowledge(_ context.Context, uc *usecases.SubscriberUsecase) func(context.Context, *connect.Request[pubsubpb.AcknowledgeRequest]) (*connect.Response[emptypb.Empty], error) {
	return func(ctx context.Context, req *connect.Request[pubsubpb.AcknowledgeRequest]) (*connect.Response[emptypb.Empty], error) {
		request := payload.NewAcknowledgeRequest(ctx, req)

		if err := uc.Acknowledge(ctx, types.FQDN(request.Payload().Subscription), request.Payload().AckIds); err != nil {
			logErr(err, "acknowledge failed", "subscription", request.Payload().Subscription)
			return nil, toConnectError(err)
		}
		slog.Debug("messages acknowledged", "subscription", request.Payload().Subscription, "count", len(request.Payload().AckIds))

		return payload.NewEmptyResponse(ctx).Encode(), nil
	}
}

func NewModifyAckDeadline(_ context.Context, uc *usecases.SubscriberUsecase) func(context.Context, *connect.Request[pubsubpb.ModifyAckDeadlineRequest]) (*connect.Response[emptypb.Empty], error) {
	return func(ctx context.Context, req *connect.Request[pubsubpb.ModifyAckDeadlineRequest]) (*connect.Response[emptypb.Empty], error) {
		request := payload.NewModifyAckDeadlineRequest(ctx, req)
		msg := request.Payload()

		deadline := time.Duration(msg.AckDeadlineSeconds) * time.Second
		if err := uc.ModifyAckDeadline(ctx, types.FQDN(msg.Subscription), msg.AckIds, deadline); err != nil {
			logErr(err, "modify ack deadline failed", "subscription", msg.Subscription)
			return nil, toConnectError(err)
		}
		slog.Debug("ack deadline modified", "subscription", msg.Subscription, "count", len(msg.AckIds), "deadline", deadline)

		return payload.NewEmptyResponse(ctx).Encode(), nil
	}
}

func NewModifyPushConfig(_ context.Context, uc *usecases.SubscriberUsecase) func(context.Context, *connect.Request[pubsubpb.ModifyPushConfigRequest]) (*connect.Response[emptypb.Empty], error) {
	return func(ctx context.Context, req *connect.Request[pubsubpb.ModifyPushConfigRequest]) (*connect.Response[emptypb.Empty], error) {
		request := payload.NewModifyPushConfigRequest(ctx, req)
		msg := request.Payload()

		var cfg *types.PushConfig
		if msg.PushConfig != nil {
			cfg = &types.PushConfig{
				Endpoint:   msg.PushConfig.PushEndpoint,
				Attributes: msg.PushConfig.Attributes,
			}
		}

		if err := uc.ModifyPushConfig(ctx, types.FQDN(msg.Subscription), cfg); err != nil {
			logErr(err, "modify push config failed", "subscription", msg.Subscription)
			return nil, toConnectError(err)
		}
		slog.Debug("push config modified", "subscription", msg.Subscription)

		return payload.NewEmptyResponse(ctx).Encode(), nil
	}
}

func NewStreamingPull(_ context.Context, uc *usecases.SubscriberUsecase) func(context.Context, *connect.BidiStream[pubsubpb.StreamingPullRequest, pubsubpb.StreamingPullResponse]) error {
	return func(ctx context.Context, stream *connect.BidiStream[pubsubpb.StreamingPullRequest, pubsubpb.StreamingPullResponse]) error {
		initReq, err := stream.Receive()
		if err != nil {
			return err
		}
		subName := types.FQDN(initReq.Subscription)
		if subName == "" {
			return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("subscription is required"))
		}
		if _, err := uc.GetSubscription(ctx, subName); err != nil {
			logErr(err, "streaming pull: subscription lookup failed", "subscription", subName)
			return toConnectError(err)
		}
		slog.Debug("streaming pull connected", "subscription", subName)

		if err := processStreamReq(ctx, uc, subName, initReq); err != nil {
			return err
		}

		msgCh, unregister, err := uc.StreamMessages(ctx, subName)
		if err != nil {
			return toConnectError(err)
		}
		defer unregister()

		incomingCh := make(chan *pubsubpb.StreamingPullRequest, 64)
		errCh := make(chan error, 1)

		go func() {
			for {
				req, err := stream.Receive()
				if err != nil {
					errCh <- err
					return
				}
				select {
				case incomingCh <- req:
				case <-ctx.Done():
					return
				}
			}
		}()

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()

			case err := <-errCh:
				return err

			case req := <-incomingCh:
				if err := processStreamReq(ctx, uc, subName, req); err != nil {
					return err
				}

			case msgs, ok := <-msgCh:
				if !ok {
					return connect.NewError(connect.CodeUnavailable, fmt.Errorf("subscription %s dispatcher stopped", subName))
				}
				msgs, err = uc.HandleDeadLetters(ctx, subName, msgs)
				if err != nil {
					return toConnectError(err)
				}
				if len(msgs) == 0 {
					continue
				}
				if err := sendMessages(stream, msgs); err != nil {
					return err
				}
			}
		}
	}
}

func sendMessages(stream *connect.BidiStream[pubsubpb.StreamingPullRequest, pubsubpb.StreamingPullResponse], msgs []*entities.PendingMessage) error {
	resp := &pubsubpb.StreamingPullResponse{}
	for _, pm := range msgs {
		resp.ReceivedMessages = append(resp.ReceivedMessages, mapper.PendingMessageToProto(pm))
		slog.Debug("message streamed", "id", pm.Message().ID(), "data", string(pm.Message().Data()), "attributes", pm.Message().Attributes(), "orderingKey", pm.Message().OrderingKey(), "ackID", pm.AckID())
	}
	return stream.Send(resp)
}

func processStreamReq(ctx context.Context, uc *usecases.SubscriberUsecase, subName types.FQDN, req *pubsubpb.StreamingPullRequest) error {
	if len(req.AckIds) > 0 {
		if err := uc.Acknowledge(ctx, subName, req.AckIds); err != nil {
			return toConnectError(err)
		}
	}
	if len(req.ModifyDeadlineAckIds) > 0 && len(req.ModifyDeadlineSeconds) > 0 {
		for i, ackID := range req.ModifyDeadlineAckIds {
			var sec int32
			if i < len(req.ModifyDeadlineSeconds) {
				sec = req.ModifyDeadlineSeconds[i]
			}
			if err := uc.ModifyAckDeadline(ctx, subName, []string{ackID}, time.Duration(sec)*time.Second); err != nil {
				return toConnectError(err)
			}
		}
	}
	return nil
}
