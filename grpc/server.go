package grpc

import (
	"log"
	"time"

	"golang.org/x/net/context"

	"github.com/infobloxopen/atlas-app-toolkit/requestid"
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

	// Check if requestId already exists in metadata
	_, exists := req.Metadata[requestid.DefaultRequestIDKey]
	if !exists {
		// Inject request-id from context if it is available
		reqId, exists := requestid.FromContext(ctx)
		if !exists {
			log.Printf("Cannot pass requestId from ctx, as value does not exist for key: %s", requestid.DefaultRequestIDKey)
		} else {
			req.Metadata[requestid.DefaultRequestIDKey] = reqId
		}
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
	subscriberOptions := []pubsub.Option{}
	if req.GetRetentionPeriod() != nil {
		retentionPeriod := time.Duration(req.GetRetentionPeriod().GetValue()) * time.Second
		subscriberOptions = append(subscriberOptions, pubsub.RetentionPeriod(retentionPeriod))
	}
	if req.GetVisibilityTimeout() != nil {
		visibilityTimeout := time.Duration(req.GetVisibilityTimeout().GetValue()) * time.Second
		subscriberOptions = append(subscriberOptions, pubsub.VisibilityTimeout(visibilityTimeout))
	}
	if len(req.GetFilter()) != 0 {
		subscriberOptions = append(subscriberOptions, pubsub.Filter(req.GetFilter()))
	}

	c, e := subscriber.Start(ctx, subscriberOptions...)

	log.Printf("GRPC: starting subscription %v", req)
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
				Metadata:  msg.Metadata(),
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
