package aws

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	pubsub "github.com/infobloxopen/atlas-pubsub"
	"github.com/sirupsen/logrus"
)

// ErrAckDeadlineOutOfRange is returned whenever an invalid duration is passed to ExtendAckDeadline
var ErrAckDeadlineOutOfRange = errors.New("The visibility timeout value is out of range. Values can be 0 to 43200 seconds")

type SubscriberOption func(*awsSubscriber)

func SubscribeWithLogger(logger *logrus.Logger) SubscriberOption {
	return func(sub *awsSubscriber) {
		sub.logger = logger
	}
}

// NewSubscriber creates an AWS message broker that will subscribe to
// the given topic with at-least-once message delivery semantics for the given
// subscriptionID
// TODO: info on permissions needed within the config to make this work
func NewSubscriber(sess *session.Session, topic, subscriptionID string, opts ...SubscriberOption) (pubsub.Subscriber, error) {
	return newSubscriber(sns.New(sess), sqs.New(sess), topic, subscriptionID, opts...)
}

func newSubscriber(snsClient snsiface.SNSAPI, sqsClient sqsiface.SQSAPI, topic, subscriptionID string, opts ...SubscriberOption) (*awsSubscriber, error) {
	subscriber := awsSubscriber{sns: snsClient, sqs: sqsClient, topic: topic, subscriptionID: subscriptionID, logger: logrus.StandardLogger()}
	for _, opt := range opts {
		opt(&subscriber)
	}
	err := subscriber.ensureSubscription(topic, subscriptionID)
	if err != nil {
		return nil, err
	}
	return &subscriber, nil
}

type awsSubscriber struct {
	sns snsiface.SNSAPI
	sqs sqsiface.SQSAPI

	topic          string
	subscriptionID string

	queueURL        *string
	queueArn        *string
	topicArn        *string
	subscriptionArn *string

	logger *logrus.Logger
}

func (s *awsSubscriber) Start(ctx context.Context, opts ...pubsub.Option) (<-chan pubsub.Message, <-chan error) {
	// Default Options for AWS subscriber
	subscriberOptions := &pubsub.Options{
		VisibilityTimeout: 30 * time.Second,
		RetentionPeriod:   345600 * time.Second,
	}
	for _, opt := range opts {
		opt(subscriberOptions)
	}
	msgChannel := make(chan pubsub.Message)
	errChannel := make(chan error, 1)
	go func() {
		defer close(msgChannel)
		defer close(errChannel)

		if fErr := s.ensureFilterPolicy(subscriberOptions.Filter); fErr != nil {
			errChannel <- fErr
			return
		}

		// Set Retention Period and visibility timeout if needed
		if err := ensureQueueAttributes(s.queueURL, subscriberOptions.RetentionPeriod, subscriberOptions.VisibilityTimeout, s.sqs); err != nil {
			errChannel <- err
			return
		}

		for {
			select {
			case <-ctx.Done():
				return
			default:
				messages, err := s.pull(ctx)
				if err != nil {
					errChannel <- err
					return
				}

				for _, msg := range messages {
					msgChannel <- msg
				}
			}
		}
	}()

	return msgChannel, errChannel
}

func (s *awsSubscriber) AckMessage(ctx context.Context, messageID string) error {
	s.logger.Infof("AWS: deleting message with id %q for queueURL %q", messageID, *s.queueURL)
	_, err := s.sqs.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      s.queueURL,
		ReceiptHandle: aws.String(messageID),
	})
	return err
}

// Extend the message visibility.
// newDuration: The new value for the message's visibility timeout.
// Value needs to be 0 to 43200 seconds.
func (s *awsSubscriber) ExtendAckDeadline(ctx context.Context, messageID string, newDuration time.Duration) error {
	// Change time.duration to int64
	d := int64(newDuration.Seconds())
	if d < 0 || d > 43200 {
		return ErrAckDeadlineOutOfRange
	}
	s.logger.Infof("AWS: extending message visibility for id %q, queueURL %q to %d seconds", messageID, *s.queueURL, d)
	_, err := s.sqs.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
		QueueUrl:          s.queueURL,
		ReceiptHandle:     aws.String(messageID),
		VisibilityTimeout: aws.Int64(d),
	})
	return err
}

// deleteQueue removes the subscription queue.
func (s *awsSubscriber) deleteQueue() error {
	s.logger.Infof("AWS: deleting SQS queue %q", *s.queueURL)
	_, err := s.sqs.DeleteQueue(&sqs.DeleteQueueInput{
		QueueUrl: s.queueURL,
	})
	return err
}

func (s *awsSubscriber) DeleteSubscription(ctx context.Context) error {
	s.logger.Infof("AWS: delete subscription subscriptionID %q, from topic %q", s.subscriptionID, s.topic)
	if err := s.unsubscribe(); err != nil {
		return err
	}

	if err := s.deleteQueue(); err != nil {
		return err
	}

	return nil
}

// unsubsctribe unsubscribes the subscription
// If the subscription doesn't exists it will return nil.
func (s *awsSubscriber) unsubscribe() error {
	s.logger.Infof("AWS: unsubscribe subscriptionID %q, from topic %q", s.subscriptionID, s.topic)
	_, err := s.sns.Unsubscribe(&sns.UnsubscribeInput{
		SubscriptionArn: s.subscriptionArn,
	})
	return err
}

type awsMessage struct {
	ctx        context.Context
	messageID  string
	message    []byte
	metadata   map[string]string
	subscriber *awsSubscriber
}

func (m *awsMessage) MessageID() string {
	return m.messageID
}
func (m *awsMessage) Message() []byte {
	return m.message
}
func (m *awsMessage) Metadata() map[string]string {
	return m.metadata
}
func (m *awsMessage) ExtendAckDeadline(newDuration time.Duration) error {
	return m.subscriber.ExtendAckDeadline(m.ctx, m.MessageID(), newDuration)
}
func (m *awsMessage) Ack() error {
	return m.subscriber.AckMessage(m.ctx, m.MessageID())
}

// ensureSubscription takes a topic and subscriptionID and creates all required
// plumbing
//
// TODO: consider edge cases where outside tampering of the queue could happen:
// * what if someone subscribes the same queue to a different SNS topic?
// * other stuff?
func (s *awsSubscriber) ensureSubscription(topic, subscriptionID string) error {
	logStatus := func(step string) {
		s.logger.Infof("AWS: %s for topic %q, subID %q", step, topic, subscriptionID)
	}
	queueName, err := buildAWSQueueName(topic, subscriptionID)
	if err != nil {
		return err
	}
	// create the queue if needed
	logStatus("ensuring queue exists")
	s.queueURL, err = ensureQueue(queueName, s.sqs, s.logger)
	if err != nil {
		return err
	}
	// create the topic if needed
	logStatus("ensuring SNS topic exists")
	s.topicArn, err = ensureTopic(topic, s.sns)
	if err != nil {
		return err
	}
	// set the queue policy to allow SNS
	logStatus("ensuring queue policy is set")
	err = ensureQueuePolicy(s.queueURL, s.topicArn, s.sqs)
	if err != nil {
		return err
	}
	// subscribe the queue to the topic
	logStatus("ensuring SQS queue is subscribed to SNS topic")
	s.queueArn, s.subscriptionArn, err = ensureQueueSubscription(s.queueURL, s.topicArn, s.sns, s.sqs)
	if err != nil {
		return err
	}

	return nil
}

// filterPoliciesEqual will verify that both maps are equal as far as filter policies
// are concerned. This differs from using reflect.DeepEqual in that a nil map will
// compare equally to an empty map
func filterPoliciesEqual(l, r map[string]string) bool {
	if len(l) != len(r) {
		return false
	}
	for k, vl := range l {
		if vr, ok := r[k]; !ok || vl != vr {
			return false
		}
	}
	return true
}

// ensureFilterPolicy will verify that the passed-in policy matches the existing
// policy and, if not, the existing policy gets updated
func (s *awsSubscriber) ensureFilterPolicy(filter map[string]string) error {
	attrs, err := s.sns.GetSubscriptionAttributes(&sns.GetSubscriptionAttributesInput{SubscriptionArn: s.subscriptionArn})
	if err != nil {
		return err
	}

	currentFilterPolicy, err := decodeFilterPolicy(attrs.Attributes["FilterPolicy"])
	if err != nil || !filterPoliciesEqual(currentFilterPolicy, filter) {
		s.logger.Infof("AWS: filter policy changed for topic %q, subID %q. old: %v, new: %v", s.topic, s.subscriptionID, currentFilterPolicy, filter)
		/*
		   If the new filter is empty, we need to delete the subscription and recreate it.
		   This is needed because the AWS API won't allow you to set an empty FilterPolicy
		*/
		if len(filter) == 0 {
			s.logger.Infof("AWS: clearing filter policy for topic %q, subID %q", s.topic, s.subscriptionID)
			err := s.unsubscribe()
			if err != nil {
				return err
			}
			resp, err := s.sns.Subscribe(&sns.SubscribeInput{
				Protocol: aws.String("sqs"),
				TopicArn: s.topicArn,
				Endpoint: s.queueArn,
			})
			if err != nil {
				return err
			}
			s.subscriptionArn = resp.SubscriptionArn
			return nil
		}

		newFilterPolicy, err := encodeFilterPolicy(filter)
		if err != nil {
			return err
		}
		s.logger.Infof("AWS: modifying filter policy for topic %q, subID %q", s.topic, s.subscriptionID)
		if _, ssaErr := s.sns.SetSubscriptionAttributes(&sns.SetSubscriptionAttributesInput{
			SubscriptionArn: s.subscriptionArn,
			AttributeName:   aws.String("FilterPolicy"),
			AttributeValue:  newFilterPolicy,
		}); ssaErr != nil {
			return ssaErr
		}
	}

	return nil
}

// pull returns the message and error channel for the subscriber
func (s *awsSubscriber) pull(ctx context.Context) ([]*awsMessage, error) {
	resp, err := s.sqs.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:              s.queueURL,
		WaitTimeSeconds:       aws.Int64(20),
		MaxNumberOfMessages:   aws.Int64(1),
		MessageAttributeNames: []*string{aws.String("All")},
	})
	if err != nil {
		s.logger.Infof("AWS: error while fetching messages %v", err)
		return nil, err
	}

	messages := make([]*awsMessage, 0, len(resp.Messages))
	for _, msg := range resp.Messages {
		message, err := decodeFromSQSMessage(msg.Body)
		if err != nil {
			s.logger.Infof("AWS: error parsing SQS message body: %v", err)
			return nil, err
		}

		attributes, err := decodeMessageAttributes(msg.Body)
		if err != nil {
			s.logger.Infof("AWS: error parsing SQS message attributes: %v", err)
			return nil, err
		}
		messages = append(messages, &awsMessage{
			ctx:        ctx,
			subscriber: s,
			messageID:  *msg.ReceiptHandle,
			message:    message,
			metadata:   attributes,
		})
	}

	return messages, nil
}
