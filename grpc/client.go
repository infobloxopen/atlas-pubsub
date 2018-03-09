package grpc

// This file contains many convenience methods for interacting with the pubusub
// interfaces from a server that supports the grpc protobuf interface defined
// within this package.

import (
	"context"
	"time"

	pubsub "github.com/infobloxopen/atlas-pubsub"
	"google.golang.org/grpc"
)

// NewSubscriber returns an implementation of
// pubsub.Subscriber from the given grpc connection. This is a
// convenience in case you want to operate with go channels instead of
// interacting with the client directly
func NewSubscriber(topic, subscriptionID string, conn *grpc.ClientConn) pubsub.Subscriber {
	return &grpcClientWrapper{topic, subscriptionID, NewPubSubClient(conn)}
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
}

func (w *grpcClientWrapper) Publish(ctx context.Context, message []byte) error {
	_, err := w.client.Publish(ctx, &PublishRequest{Topic: w.topic, Message: message})
	return err
}

func (w *grpcClientWrapper) Start(ctx context.Context) (<-chan pubsub.Message, <-chan error) {
	msgC := make(chan pubsub.Message)
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
						ctx:        ctx,
						messageID:  msg.GetMessageId(),
						message:    msg.GetMessage(),
						subscriber: w,
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
	ctx        context.Context
	messageID  string
	message    []byte
	subscriber pubsub.Subscriber
}

func (m *grpcClientWrapperMessage) MessageID() string {
	return m.messageID
}
func (m *grpcClientWrapperMessage) Message() []byte {
	return m.message
}
func (m *grpcClientWrapperMessage) ExtendAckDeadline(newDuration time.Duration) error {
	return m.subscriber.ExtendAckDeadline(m.ctx, m.messageID, newDuration)
}
func (m *grpcClientWrapperMessage) Ack() error {
	return m.subscriber.AckMessage(m.ctx, m.messageID)
}
