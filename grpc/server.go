package grpc

import (
	"log"
	"time"

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
func NewPubSubServer(publisherFactory PublisherFactory, subscriberFactory SubscriberFactory) PubSubServer {
	return &grpcWrapper{publisherFactory, subscriberFactory}
}

type grpcWrapper struct {
	publisherFactory  PublisherFactory
	subscriberFactory SubscriberFactory
}

func (s *grpcWrapper) Publish(ctx context.Context, req *PublishRequest) (*PublishResponse, error) {
	p, err := s.publisherFactory(ctx, req.GetTopic())
	if err != nil {
		log.Printf("GRPC: error initializing publisher for topic %q: %v", req.GetTopic(), err)
		return nil, err
	}

	if err := p.Publish(ctx, req.GetMessage(), req.GetMetadata()); err != nil {
		log.Printf("GRPC: error publishing to topic %q: %v", req.GetTopic(), err)
		return nil, err
	}

	log.Printf("GRPC: published to topic %q", req.GetTopic())
	return &PublishResponse{}, nil
}

func (s *grpcWrapper) Subscribe(req *SubscribeRequest, srv PubSub_SubscribeServer) error {
	subscriber, err := s.subscriberFactory(context.Background(), req.GetTopic(), req.GetSubscriptionId())
	if err != nil {
		log.Printf("GRPC: error initializing subscriber for topic %q, subID %q: %v", req.GetTopic(), req.GetSubscriptionId(), err)
		return err
	}

	ctx, cancel := context.WithCancel(srv.Context())
	defer cancel()

	// Change int64 to time.Duration
	retentionPeriod := time.Duration(req.GetRetentionPeriod()) * time.Second
	visibilityTimeout := time.Duration(req.GetVisibilityTimeout()) * time.Second
	c, e := subscriber.Start(ctx, pubsub.VisibilityTimeout(visibilityTimeout), pubsub.RetentionPeriod(retentionPeriod), pubsub.Filter(req.GetFilter()))
	log.Printf("GRPC: starting subscription for topic %q, subID %q, filter %v", req.GetTopic(), req.GetSubscriptionId(), req.GetFilter())
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
				log.Printf("GRPC: error serving message for topic %q, subID %q: %v", req.GetTopic(), req.GetSubscriptionId(), err)
				return err
			}
		case err := <-e:
			log.Printf("GRPC: general error received for topic %q, subID %q: %v", req.GetTopic(), req.GetSubscriptionId(), err)
			return err
		}
	}
}

func (s *grpcWrapper) Ack(ctx context.Context, req *AckRequest) (*AckResponse, error) {
	subscriber, err := s.subscriberFactory(ctx, req.GetTopic(), req.GetSubscriptionId())
	if err != nil {
		log.Printf("GRPC: error acking message for topic %q, subID %q, messageID %q: %v", req.GetTopic(), req.GetSubscriptionId(), req.GetMessageId(), err)
		return nil, err
	}
	log.Printf("GRPC: acking message for topic %q, subID %q, messageID %q", req.GetTopic(), req.GetSubscriptionId(), req.GetMessageId())
	err = subscriber.AckMessage(ctx, req.GetMessageId())
	if err != nil {
		log.Printf("GRPC: error acking message for topic %q, subID %q, messageID %q: %v", req.GetTopic(), req.GetSubscriptionId(), req.GetMessageId(), err)
	}
	return &AckResponse{}, err
}
