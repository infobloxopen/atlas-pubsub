package grpc

// This file contains many convenience methods for interacting with the pubusub
// interfaces from a server that supports the grpc protobuf interface defined
// within this package.

import (
	"context"
	fmt "fmt"
	"time"

	google_protobuf "github.com/golang/protobuf/ptypes/wrappers"
	pubsub "github.com/infobloxopen/atlas-pubsub"
	"google.golang.org/grpc"
)

// NewSubscriber returns an implementation of
// pubsub.Subscriber from the given grpc connection. This is a
// convenience in case you want to operate with go channels instead of
// interacting with the client directly
func NewSubscriber(topic, subscriptionID string, conn *grpc.ClientConn) pubsub.Subscriber {
	return &grpcClientWrapper{topic, subscriptionID, NewPubSubClient(conn), make(chan struct{}, 2)}
}

// NewPublisher returns an implementation of pubsub.Publisher from the
// given grpc connection. This is a convenience in case you want to operate with go
// channels instead of interacting with the client directly
func NewPublisher(topic string, conn *grpc.ClientConn) pubsub.Publisher {
	return &grpcClientWrapper{topic: topic, client: NewPubSubClient(conn)}
}

type grpcClientWrapper struct {
	topic          string
	subscriptionID string
	client         PubSubClient
	cancel         chan struct{}
}

func (w *grpcClientWrapper) Publish(ctx context.Context, message []byte, metadata map[string]string) error {
	_, err := w.client.Publish(ctx, &PublishRequest{Topic: w.topic, Message: message, Metadata: metadata})
	return err
}

func (w *grpcClientWrapper) DeleteTopic(ctx context.Context) error {
	select {
	case w.cancel <- struct{}{}:
	case <-time.After(1 * time.Second):
	}

	_, err := w.client.DeleteTopic(ctx, &DeleteTopicRequest{Topic: w.topic})
	return err
}

func (w *grpcClientWrapper) Start(ctx context.Context, opts ...pubsub.Option) (<-chan pubsub.Message, <-chan error) {
	// Default Options
	subscriberOptions := &pubsub.Options{
		VisibilityTimeout: 30 * time.Second,
		RetentionPeriod:   345600 * time.Second,
	}
	for _, opt := range opts {
		opt(subscriberOptions)
	}
	msgC := make(chan pubsub.Message)
	errC := make(chan error)

	go func() {
		defer close(msgC)
		stream, err := w.client.Subscribe(ctx, &SubscribeRequest{
			Topic:             w.topic,
			SubscriptionId:    w.subscriptionID,
			Filter:            subscriberOptions.Filter,
			RetentionPeriod:   &google_protobuf.UInt64Value{Value: uint64(subscriberOptions.RetentionPeriod.Seconds())},
			VisibilityTimeout: &google_protobuf.UInt64Value{Value: uint64(subscriberOptions.VisibilityTimeout.Seconds())},
		})
		if err != nil {
			errC <- err
			return
		}
		for {
			select {
			case <-w.cancel:
				errC <- fmt.Errorf("PUBSUB client: stream closed for subscription %s, topic %s", w.subscriptionID, w.topic)
				return
			case <-stream.Context().Done():
				errC <- fmt.Errorf("PUBSUB client: stream closed for subscription %s, topic %s", w.subscriptionID, w.topic)
				return
			case <-ctx.Done():
				errC <- fmt.Errorf("PUBSUB client: context closed for subscription %s, topic %s", w.subscriptionID, w.topic)
				return
			default:
				if msg, err := stream.Recv(); err != nil {
					errC <- err
				} else {
					wMsg := grpcClientWrapperMessage{
						ctx:        ctx,
						messageID:  msg.GetMessageId(),
						message:    msg.GetMessage(),
						metadata:   msg.GetMetadata(),
						subscriber: w,
					}
					msgC <- &wMsg
				}
			}
		}
	}()

	return msgC, errC
}

func (w *grpcClientWrapper) DeleteSubscription(ctx context.Context) error {
	select {
	case w.cancel <- struct{}{}:
	case <-time.After(1 * time.Second):
	}

	_, err := w.client.DeleteSubscription(ctx, &DeleteSubscriptionRequest{Topic: w.topic, SubscriptionId: w.subscriptionID})
	return err
}

func (w *grpcClientWrapper) AckMessage(ctx context.Context, messageID string) error {
	_, err := w.client.Ack(ctx, &AckRequest{Topic: w.topic, SubscriptionId: w.subscriptionID, MessageId: messageID})
	return err
}

func (w *grpcClientWrapper) ExtendAckDeadline(ctx context.Context, messageID string, newDuration time.Duration) error {
	panic("not implemented")
}

type grpcClientWrapperMessage struct {
	ctx        context.Context
	messageID  string
	message    []byte
	metadata   map[string]string
	subscriber pubsub.Subscriber
}

func (m *grpcClientWrapperMessage) MessageID() string {
	return m.messageID
}
func (m *grpcClientWrapperMessage) Message() []byte {
	return m.message
}
func (m *grpcClientWrapperMessage) Metadata() map[string]string {
	return m.metadata
}
func (m *grpcClientWrapperMessage) ExtendAckDeadline(newDuration time.Duration) error {
	return m.subscriber.ExtendAckDeadline(m.ctx, m.messageID, newDuration)
}
func (m *grpcClientWrapperMessage) Ack() error {
	return m.subscriber.AckMessage(m.ctx, m.messageID)
}
