package grpc

import (
	"log"

	"golang.org/x/net/context"

	pubsub "github.com/infobloxopen/atlas-pubsub"
)

// PublisherFactory is a simple function wrapper for creating a pubsub.Publisher
// instance
type PublisherFactory func(context.Context, string) (pubsub.Publisher, error)

// SubscriberFactory is a simple function wrapper for creating a
// pubsub.Subscriber instance
type SubscriberFactory func(context.Context, string, string) (pubsub.Subscriber, error)

// ErrorHandler allows handling any errors that come during subscription, like
// logging it or terminating early
type ErrorHandler func(error)

// NewPubSubServer returns an implementation of PubSubServer by consuming a
// pubsub.Publisher and pubsub.OnceSubscriber implementation
func NewPubSubServer(publisherFactory PublisherFactory, subscriberFactory SubscriberFactory, errorHandler ErrorHandler) PubSubServer {
	if errorHandler == nil {
		errorHandler = func(err error) { log.Print(err) }
	}

	return &grpcWrapper{publisherFactory, subscriberFactory, errorHandler}
}

type grpcWrapper struct {
	publisherFactory  PublisherFactory
	subscriberFactory SubscriberFactory
	errorHandler      ErrorHandler
}

func (s *grpcWrapper) Publish(ctx context.Context, req *PublishRequest) (*PublishResponse, error) {
	p, perr := s.publisherFactory(ctx, req.GetTopic())
	if perr != nil {
		return nil, perr
	}

	if err := p.Publish(ctx, req.GetMessage(), req.GetMetadata()); err != nil {
		return nil, err
	}
	return &PublishResponse{}, nil
}

func (s *grpcWrapper) Subscribe(req *SubscribeRequest, srv PubSub_SubscribeServer) error {
	log.Printf("starting subscription for %v", req)
	subscriber, serr := s.subscriberFactory(context.Background(), req.GetTopic(), req.GetSubscriptionId())
	if serr != nil {
		return serr
	}

	ctx, cancel := context.WithCancel(srv.Context())
	defer cancel()
	c, e := subscriber.Start(ctx, req.GetFilter())
	for {
		select {
		case <-srv.Context().Done():
			return nil
		case msg, isOpen := <-c:
			if !isOpen {
				return nil
			}
			if err := srv.Send(&SubscribeResponse{
				MessageId: msg.MessageID(),
				Message:   msg.Message(),
			}); err != nil {
				s.errorHandler(err)
			}
		case err := <-e:
			s.errorHandler(err)
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
