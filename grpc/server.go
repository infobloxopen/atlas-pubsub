package grpc

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/net/context"

	pubsub "github.com/infobloxopen/atlas-pubsub"
	"github.com/sirupsen/logrus"
)

type PubSubServerOption func(*grpcWrapper)

func WithLogger(logger *logrus.Logger) PubSubServerOption {
	return func(w *grpcWrapper) {
		w.logger = logger
	}
}

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
func NewPubSubServer(publisherFactory PublisherFactory, subscriberFactory SubscriberFactory, opts ...PubSubServerOption) PubSubServer {
	w := &grpcWrapper{
		publisherFactory:  publisherFactory,
		subscriberFactory: subscriberFactory,
		logger:            logrus.StandardLogger(),
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

type grpcWrapper struct {
	publisherFactory  PublisherFactory
	subscriberFactory SubscriberFactory
	logger            *logrus.Logger
}

func (s *grpcWrapper) Publish(ctx context.Context, req *PublishRequest) (*PublishResponse, error) {
	p, err := s.publisherFactory(ctx, req.GetTopic())
	if err != nil {
		s.logger.Errorf("GRPC: error initializing publisher for topic %q: %v", req.GetTopic(), err)
		return nil, err
	}

	if err := p.Publish(ctx, req.GetMessage(), req.GetMetadata()); err != nil {
		s.logger.Errorf("GRPC: error publishing to topic %q: %v", req.GetTopic(), err)
		return nil, err
	}

	s.logger.Infof("GRPC: published to topic %q", req.GetTopic())
	return &PublishResponse{}, nil
}

func (s *grpcWrapper) Subscribe(req *SubscribeRequest, srv PubSub_SubscribeServer) error {
	subscriber, err := s.subscriberFactory(context.Background(), req.GetTopic(), req.GetSubscriptionId())
	if err != nil {
		s.logger.Errorf("GRPC: error initializing subscriber for topic %q, subID %q: %v", req.GetTopic(), req.GetSubscriptionId(), err)
		return err
	}

	sigs := registerSignalHandler()
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

	s.logger.Infof("GRPC: starting subscription %v", req)
	for {
		select {
		case sig := <-sigs:
			s.logger.Infof("GRPC: handle system signal %q, for topic %q, subID %q", sig, req.GetTopic(), req.GetSubscriptionId())
			return nil
		case <-srv.Context().Done():
			s.logger.Infof("GRPC: server context done, for topc %q, subID %q", req.GetTopic(), req.GetSubscriptionId())
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
				s.logger.Errorf("GRPC: error serving message for topic %q, subID %q: %v", req.GetTopic(), req.GetSubscriptionId(), err)
				return err
			}
		case err := <-e:
			s.logger.Errorf("GRPC: general error received for topic %q, subID %q: %v", req.GetTopic(), req.GetSubscriptionId(), err)
			return err
		}
	}
}

func (s *grpcWrapper) Ack(ctx context.Context, req *AckRequest) (*AckResponse, error) {
	subscriber, err := s.subscriberFactory(ctx, req.GetTopic(), req.GetSubscriptionId())
	if err != nil {
		s.logger.Errorf("GRPC: error acking message for topic %q, subID %q, messageID %q: %v", req.GetTopic(), req.GetSubscriptionId(), req.GetMessageId(), err)
		return nil, err
	}
	s.logger.Infof("GRPC: acking message for topic %q, subID %q, messageID %q", req.GetTopic(), req.GetSubscriptionId(), req.GetMessageId())
	err = subscriber.AckMessage(ctx, req.GetMessageId())
	if err != nil {
		s.logger.Errorf("GRPC: error acking message for topic %q, subID %q, messageID %q: %v", req.GetTopic(), req.GetSubscriptionId(), req.GetMessageId(), err)
	}
	return &AckResponse{}, err
}

func (s *grpcWrapper) DeleteTopic(ctx context.Context, req *DeleteTopicRequest) (*DeleteTopicResponse, error) {
	subscriber, err := s.publisherFactory(ctx, req.GetTopic())
	if err != nil {
		s.logger.Errorf("GRPC: error delete topic %q, error %s", req.GetTopic(), err)
		return &DeleteTopicResponse{}, err
	}

	if err := subscriber.DeleteTopic(ctx); err != nil {
		s.logger.Errorf("GRPC: error delete topic %q, error %s", req.GetTopic(), err)
		return &DeleteTopicResponse{}, err

	}

	return &DeleteTopicResponse{}, nil
}

func (s *grpcWrapper) DeleteSubscription(ctx context.Context, req *DeleteSubscriptionRequest) (*DeleteSubscriptionResponse, error) {
	subscriber, err := s.subscriberFactory(ctx, req.GetTopic(), req.GetSubscriptionId())
	if err != nil {
		s.logger.Errorf("GRPC: error delete subscription, topic %q, subscriptionId %q, error %s", req.GetTopic(), req.GetSubscriptionId(), err)
		return &DeleteSubscriptionResponse{}, err
	}

	if err := subscriber.DeleteSubscription(ctx); err != nil {
		s.logger.Errorf("GRPC: error delete subscription, topic %q, subscriptionId %q, error %s", req.GetTopic(), req.GetSubscriptionId(), err)
		return &DeleteSubscriptionResponse{}, err
	}

	return &DeleteSubscriptionResponse{}, nil
}

func registerSignalHandler() <-chan os.Signal {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)

	return sigs
}
