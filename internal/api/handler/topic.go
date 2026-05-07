package handler

import (
	"context"
	"fmt"
	"log/slog"

	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/fbufler/google-pubsub/internal/api/mapper"
	"github.com/fbufler/google-pubsub/internal/api/payload"
	"github.com/fbufler/google-pubsub/internal/core/entities"
	"github.com/fbufler/google-pubsub/internal/core/types"
	"github.com/fbufler/google-pubsub/internal/core/usecases"
)

func NewCreateTopic(_ context.Context, uc *usecases.TopicUsecase) func(context.Context, *connect.Request[pubsubpb.Topic]) (*connect.Response[pubsubpb.Topic], error) {
	return func(ctx context.Context, req *connect.Request[pubsubpb.Topic]) (*connect.Response[pubsubpb.Topic], error) {
		request := payload.NewCreateTopicRequest(ctx, req)

		topic, err := mapper.ProtoToTopic(request.Payload())
		if err != nil {
			return nil, toConnectError(err)
		}

		topicName := request.Payload().Name
		topic, err = uc.CreateTopic(ctx, topic)
		if err != nil {
			logErr(err, "create topic failed", "topic", topicName)
			return nil, toConnectError(err)
		}

		protoTopic, err := mapper.TopicToProto(topic)
		if err != nil {
			return nil, toConnectError(err)
		}

		return payload.NewCreateTopicResponse(ctx, protoTopic).Encode(), nil
	}
}

func NewGetTopic(_ context.Context, uc *usecases.TopicUsecase) func(context.Context, *connect.Request[pubsubpb.GetTopicRequest]) (*connect.Response[pubsubpb.Topic], error) {
	return func(ctx context.Context, req *connect.Request[pubsubpb.GetTopicRequest]) (*connect.Response[pubsubpb.Topic], error) {
		request := payload.NewGetTopicRequest(ctx, req)

		topic, err := uc.GetTopic(ctx, types.FQDN(request.Payload().Topic))
		if err != nil {
			logErr(err, "get topic failed", "topic", request.Payload().Topic)
			return nil, toConnectError(err)
		}
		slog.Debug("topic retrieved", "topic", request.Payload().Topic)

		protoTopic, err := mapper.TopicToProto(topic)
		if err != nil {
			return nil, toConnectError(err)
		}

		return payload.NewGetTopicResponse(ctx, protoTopic).Encode(), nil
	}
}

func NewUpdateTopic(_ context.Context, uc *usecases.TopicUsecase) func(context.Context, *connect.Request[pubsubpb.UpdateTopicRequest]) (*connect.Response[pubsubpb.Topic], error) {
	return func(ctx context.Context, req *connect.Request[pubsubpb.UpdateTopicRequest]) (*connect.Response[pubsubpb.Topic], error) {
		request := payload.NewUpdateTopicRequest(ctx, req)
		msg := request.Payload()

		if msg.Topic == nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("topic is required"))
		}

		existing, err := uc.GetTopic(ctx, types.FQDN(msg.Topic.Name))
		if err != nil {
			logErr(err, "update topic: get failed", "topic", msg.Topic.Name)
			return nil, toConnectError(err)
		}

		for _, path := range msg.UpdateMask.GetPaths() {
			switch path {
			case "labels":
				if err := existing.SetLabels(types.Labels(msg.Topic.Labels)); err != nil {
					return nil, toConnectError(err)
				}
			case "message_retention_duration":
				if msg.Topic.MessageRetentionDuration != nil {
					if err := existing.SetMessageRetention(msg.Topic.MessageRetentionDuration.AsDuration()); err != nil {
						return nil, toConnectError(err)
					}
				}
			}
		}

		if err := uc.UpdateTopic(ctx, existing); err != nil {
			logErr(err, "update topic failed", "topic", msg.Topic.Name)
			return nil, toConnectError(err)
		}
		slog.Debug("topic updated", "topic", msg.Topic.Name)

		protoTopic, err := mapper.TopicToProto(existing)
		if err != nil {
			return nil, toConnectError(err)
		}

		return payload.NewUpdateTopicResponse(ctx, protoTopic).Encode(), nil
	}
}

func NewDeleteTopic(_ context.Context, uc *usecases.TopicUsecase) func(context.Context, *connect.Request[pubsubpb.DeleteTopicRequest]) (*connect.Response[emptypb.Empty], error) {
	return func(ctx context.Context, req *connect.Request[pubsubpb.DeleteTopicRequest]) (*connect.Response[emptypb.Empty], error) {
		request := payload.NewDeleteTopicRequest(ctx, req)

		if err := uc.DeleteTopic(ctx, types.FQDN(request.Payload().Topic)); err != nil {
			logErr(err, "delete topic failed", "topic", request.Payload().Topic)
			return nil, toConnectError(err)
		}
		slog.Debug("topic deleted", "topic", request.Payload().Topic)

		return payload.NewEmptyResponse(ctx).Encode(), nil
	}
}

func NewListTopics(_ context.Context, uc *usecases.TopicUsecase) func(context.Context, *connect.Request[pubsubpb.ListTopicsRequest]) (*connect.Response[pubsubpb.ListTopicsResponse], error) {
	return func(ctx context.Context, req *connect.Request[pubsubpb.ListTopicsRequest]) (*connect.Response[pubsubpb.ListTopicsResponse], error) {
		request := payload.NewListTopicsRequest(ctx, req)

		topics, err := uc.ListTopics(ctx, projectID(request.Payload().Project))
		if err != nil {
			logErr(err, "list topics failed", "project", request.Payload().Project)
			return nil, toConnectError(err)
		}

		resp := &pubsubpb.ListTopicsResponse{}
		for _, t := range topics {
			pt, err := mapper.TopicToProto(t)
			if err != nil {
				return nil, toConnectError(err)
			}
			resp.Topics = append(resp.Topics, pt)
		}
		slog.Debug("topics listed", "project", request.Payload().Project, "count", len(resp.Topics))

		return payload.NewListTopicsResponse(ctx, resp).Encode(), nil
	}
}

func NewListTopicSubscriptions(_ context.Context, uc *usecases.TopicUsecase) func(context.Context, *connect.Request[pubsubpb.ListTopicSubscriptionsRequest]) (*connect.Response[pubsubpb.ListTopicSubscriptionsResponse], error) {
	return func(ctx context.Context, req *connect.Request[pubsubpb.ListTopicSubscriptionsRequest]) (*connect.Response[pubsubpb.ListTopicSubscriptionsResponse], error) {
		request := payload.NewListTopicSubscriptionsRequest(ctx, req)

		names, err := uc.ListTopicSubscriptions(ctx, types.FQDN(request.Payload().Topic))
		if err != nil {
			logErr(err, "list topic subscriptions failed", "topic", request.Payload().Topic)
			return nil, toConnectError(err)
		}

		strNames := make([]string, len(names))
		for i, n := range names {
			strNames[i] = n.String()
		}
		slog.Debug("topic subscriptions listed", "topic", request.Payload().Topic, "count", len(strNames))

		return payload.NewListTopicSubscriptionsResponse(ctx, &pubsubpb.ListTopicSubscriptionsResponse{Subscriptions: strNames}).Encode(), nil
	}
}

func NewListTopicSnapshots(_ context.Context, uc *usecases.TopicUsecase) func(context.Context, *connect.Request[pubsubpb.ListTopicSnapshotsRequest]) (*connect.Response[pubsubpb.ListTopicSnapshotsResponse], error) {
	return func(ctx context.Context, req *connect.Request[pubsubpb.ListTopicSnapshotsRequest]) (*connect.Response[pubsubpb.ListTopicSnapshotsResponse], error) {
		request := payload.NewListTopicSnapshotsRequest(ctx, req)

		snaps, err := uc.ListTopicSnapshots(ctx, types.FQDN(request.Payload().Topic))
		if err != nil {
			logErr(err, "list topic snapshots failed", "topic", request.Payload().Topic)
			return nil, toConnectError(err)
		}

		names := make([]string, len(snaps))
		for i, s := range snaps {
			names[i] = s.Name().String()
		}
		slog.Debug("topic snapshots listed", "topic", request.Payload().Topic, "count", len(names))

		return payload.NewListTopicSnapshotsResponse(ctx, &pubsubpb.ListTopicSnapshotsResponse{Snapshots: names}).Encode(), nil
	}
}

func NewPublish(_ context.Context, uc *usecases.PublisherUsecase) func(context.Context, *connect.Request[pubsubpb.PublishRequest]) (*connect.Response[pubsubpb.PublishResponse], error) {
	return func(ctx context.Context, req *connect.Request[pubsubpb.PublishRequest]) (*connect.Response[pubsubpb.PublishResponse], error) {
		request := payload.NewPublishRequest(ctx, req)
		msg := request.Payload()

		if msg.Topic == "" {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("topic is required"))
		}

		msgs := make([]*entities.Message, 0, len(msg.Messages))
		for _, m := range msg.Messages {
			entity, err := mapper.ProtoToMessage(m)
			if err != nil {
				return nil, toConnectError(err)
			}
			msgs = append(msgs, entity)
		}

		ids, err := uc.Publish(ctx, types.FQDN(msg.Topic), msgs)
		if err != nil {
			logErr(err, "publish failed", "topic", msg.Topic)
			return nil, toConnectError(err)
		}
		slog.Debug("messages published", "topic", msg.Topic, "count", len(ids))
		for i, m := range msgs {
			slog.Debug("message published", "topic", msg.Topic, "id", ids[i], "data", string(m.Data()), "attributes", m.Attributes(), "orderingKey", m.OrderingKey())
		}

		return payload.NewPublishResponse(ctx, &pubsubpb.PublishResponse{MessageIds: ids}).Encode(), nil
	}
}

func NewDetachSubscription(_ context.Context, uc *usecases.TopicUsecase) func(context.Context, *connect.Request[pubsubpb.DetachSubscriptionRequest]) (*connect.Response[pubsubpb.DetachSubscriptionResponse], error) {
	return func(ctx context.Context, req *connect.Request[pubsubpb.DetachSubscriptionRequest]) (*connect.Response[pubsubpb.DetachSubscriptionResponse], error) {
		request := payload.NewDetachSubscriptionRequest(ctx, req)

		if err := uc.DetachSubscription(ctx, types.FQDN(request.Payload().Subscription)); err != nil {
			logErr(err, "detach subscription failed", "subscription", request.Payload().Subscription)
			return nil, toConnectError(err)
		}
		slog.Debug("subscription detached", "subscription", request.Payload().Subscription)

		return payload.NewDetachSubscriptionResponse(ctx, &pubsubpb.DetachSubscriptionResponse{}).Encode(), nil
	}
}
