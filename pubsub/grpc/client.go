package grpc

// This file contains many convenience methods for interacting with the pubusub
// interfaces from a server that supports the grpc protobuf interface defined
// within this package.

import (
	"context"
	"time"

	"github.com/infobloxopen/atlas-pubsub/pubsub"
	"google.golang.org/grpc"
)

// NewSubscriberFromConnection returns an implementation of
// pubsub.AtLeastOnceSubscriber from the given grpc connection. This is a
// convenience in case you want to operate with go channels instead of
// interacting with the client directly
func NewSubscriberFromConnection(topic, subscriptionID string, conn *grpc.ClientConn) pubsub.AtLeastOnceSubscriber {
	return NewSubscriberFromClient(topic, subscriptionID, NewPubSubClient(conn))
}

// NewSubscriberFromClient returns an implementation of
// pubsub.AtLeastOnceSubscriber from the given PubSubClient. This is a
// convenience in case you want to operate with go channels instead of
// interacting with the client directly
func NewSubscriberFromClient(topic, subscriptionID string, client PubSubClient) pubsub.AtLeastOnceSubscriber {
	return &grpcClientWrapper{topic, subscriptionID, client}
}

// NewPublisherFromConnection returns an implementation of pubsub.Publisher from the
// given grpc connection. This is a convenience in case you want to operate with go
// channels instead of interacting with the client directly
func NewPublisherFromConnection(topic string, conn *grpc.ClientConn) pubsub.Publisher {
	return NewPublisherFromClient(topic, NewPubSubClient(conn))
}

// NewPublisherFromClient returns an implementation of pubsub.Publisher from the
// given PubSubClient. This is a convenience in case you want to operate with go
// channels instead of interacting with the client directly
func NewPublisherFromClient(topic string, client PubSubClient) pubsub.Publisher {
	return &grpcClientWrapper{topic: topic, client: client}
}

type grpcClientWrapper struct {
	topic          string
	subscriptionID string
	client         PubSubClient
}

func (w *grpcClientWrapper) Publish(ctx context.Context, message []byte) error {
	_, err := w.client.Publish(ctx, &PublishRequest{Topic: w.topic, Message: message})
	return err
}

func (w *grpcClientWrapper) Start(ctx context.Context) (<-chan pubsub.AtLeastOnceMessage, <-chan error) {
	msgC := make(chan pubsub.AtLeastOnceMessage)
	errC := make(chan error)

	go func() {
		defer close(msgC)

		stream, err := w.client.Subscribe(ctx, &SubscribeRequest{Topic: w.topic, SubscriptionId: w.subscriptionID})
		if err != nil {
			errC <- err
			return
		}
		for {
			select {
			case <-stream.Context().Done():
				return
			case <-ctx.Done():
				return
			default:
				if msg, err := stream.Recv(); err != nil {
					errC <- err
				} else {
					wMsg := grpcClientWrapperMessage{
						ctx:           ctx,
						messageID:     msg.GetMessageId(),
						message:       msg.GetMessage(),
						clientWrapper: w,
					}
					msgC <- &wMsg
				}
			}
		}
	}()

	return msgC, errC
}

func (w *grpcClientWrapper) AckMessage(ctx context.Context, messageID string) error {
	_, err := w.client.Ack(ctx, &AckRequest{Topic: w.topic, SubscriptionId: w.subscriptionID, MessageId: messageID})
	return err
}

func (w *grpcClientWrapper) ExtendAckDeadline(ctx context.Context, messageID string, newDuration time.Duration) error {
	panic("not implemented")
}

type grpcClientWrapperMessage struct {
	ctx           context.Context
	messageID     string
	message       []byte
	clientWrapper *grpcClientWrapper
}

func (m *grpcClientWrapperMessage) MessageID() string {
	return m.messageID
}
func (m *grpcClientWrapperMessage) Message() []byte {
	return m.message
}
func (m *grpcClientWrapperMessage) ExtendAckDeadline(newDuration time.Duration) error {
	return m.clientWrapper.ExtendAckDeadline(m.ctx, m.messageID, newDuration)
}
func (m *grpcClientWrapperMessage) Ack() error {
	return m.clientWrapper.AckMessage(m.ctx, m.messageID)
}
