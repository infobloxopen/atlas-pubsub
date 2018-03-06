package grpc

import (
	"context"

	"github.com/infobloxopen/atlas-pubsub/pubsub"
)

// PublisherFactory is a simple function wrapper for creating a pubsub.Publisher
// instance
type PublisherFactory func(context.Context, string) (pubsub.Publisher, error)

// SubscriberFactory is a simple function wrapper for creating a
// pubsub.AtLeastOnceSubscriber instance
type SubscriberFactory func(context.Context, string, string) (pubsub.AtLeastOnceSubscriber, error)

// NewPubSubServer returns an implementation of PubSubServer by consuming a
// pubsub.Publisher and pubsub.AtLeastOnceSubscriber implementation
func NewPubSubServer(publisherFactory PublisherFactory, subscriberFactory SubscriberFactory) PubSubServer {
	return &grpcWrapper{publisherFactory, subscriberFactory}
}

type grpcWrapper struct {
	publisherFactory  PublisherFactory
	subscriberFactory SubscriberFactory
}

func (s *grpcWrapper) Publish(ctx context.Context, req *PublishRequest) (*PublishResponse, error) {
	p, perr := s.publisherFactory(ctx, req.GetTopic())
	if perr != nil {
		return nil, perr
	}

	if err := p.Publish(ctx, req.GetMessage()); err != nil {
		return nil, err
	}
	return &PublishResponse{}, nil
}

func (s *grpcWrapper) Subscribe(req *SubscribeRequest, srv PubSub_SubscribeServer) error {
	subscriber, serr := s.subscriberFactory(context.Background(), req.GetTopic(), req.GetSubscriptionId())
	if serr != nil {
		return serr
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, e := subscriber.Start(ctx)
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, isOpen := <-c:
			if !isOpen {
				return nil
			}
			sErr := srv.Send(&SubscribeResponse{MessageId: msg.MessageID(), Message: msg.Message()})
			if sErr != nil {
				return sErr
			}
		case err := <-e:
			return err
		}
	}
}

func (s *grpcWrapper) Ack(ctx context.Context, req *AckRequest) (*AckResponse, error) {
	subscriber, serr := s.subscriberFactory(ctx, req.GetTopic(), req.GetSubscriptionId())
	if serr != nil {
		return nil, serr
	}

	return &AckResponse{}, subscriber.AckMessage(ctx, req.GetMessageId())
}
