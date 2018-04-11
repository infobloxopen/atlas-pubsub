package aws

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	pubsub "github.com/infobloxopen/atlas-pubsub"
)

// ErrAckDeadlineOutOfRange is returned whenever an invalid duration is passed to ExtendAckDeadline
var ErrAckDeadlineOutOfRange = errors.New("The visibility timeout value is out of range. Values can be 0 to 43200 seconds")

// NewSubscriber creates an AWS message broker that will subscribe to
// the given topic with at-least-once message delivery semantics for the given
// subscriptionID
// TODO: info on permissions needed within the config to make this work
func NewSubscriber(sess *session.Session, topic, subscriptionID string) (pubsub.Subscriber, error) {
	return newSubscriber(sns.New(sess), sqs.New(sess), topic, subscriptionID)
}

func newSubscriber(snsClient snsiface.SNSAPI, sqsClient sqsiface.SQSAPI, topic, subscriptionID string) (*awsSubscriber, error) {
	subscriber := awsSubscriber{sns: snsClient, sqs: sqsClient, topic: topic, subscriptionID: subscriptionID}
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

	wg sync.WaitGroup
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
	channel := make(chan pubsub.Message)
	errChannel := make(chan error)
	go func() {
		defer close(channel)
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
				s.wg.Wait()
				return
			default:
				s.pull(ctx, channel, errChannel)
			}
		}
	}()

	return channel, errChannel
}

func (s *awsSubscriber) AckMessage(ctx context.Context, messageID string) error {
	log.Printf("AWS: deleting message with id %q for queueURL %q", messageID, *s.queueURL)
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
	log.Printf("AWS: extending message visibility for id %q, queueURL %q to %d seconds", messageID, *s.queueURL, d)
	_, err := s.sqs.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
		QueueUrl:          s.queueURL,
		ReceiptHandle:     aws.String(messageID),
		VisibilityTimeout: aws.Int64(d),
	})
	return err
}

func (s *awsSubscriber) DeleteSubscription() error {
	log.Printf("AWS: deleting SQS queue %q", *s.queueURL)
	_, err := s.sqs.DeleteQueue(&sqs.DeleteQueueInput{
		QueueUrl: s.queueURL,
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
		log.Printf("AWS: %s for topic %q, subID %q", step, topic, subscriptionID)
	}
	queueName, err := buildAWSQueueName(topic, subscriptionID)
	if err != nil {
		return err
	}
	// create the queue if needed
	logStatus("ensuring queue exists")
	s.queueURL, err = ensureQueue(queueName, s.sqs)
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
		log.Printf("AWS: filter policy changed for topic %q, subID %q. old: %v, new: %v", s.topic, s.subscriptionID, currentFilterPolicy, filter)
		/*
		   If the new filter is empty, we need to delete the subscription and recreate it.
		   This is needed because the AWS API won't allow you to set an empty FilterPolicy
		*/
		if len(filter) == 0 {
			log.Printf("AWS: clearing filter policy for topic %q, subID %q", s.topic, s.subscriptionID)
			if _, err := s.sns.Unsubscribe(&sns.UnsubscribeInput{
				SubscriptionArn: s.subscriptionArn,
			}); err != nil {
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
		log.Printf("AWS: modifying filter policy for topic %q, subID %q", s.topic, s.subscriptionID)
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

func (s *awsSubscriber) pull(ctx context.Context, channel chan pubsub.Message, errChannel chan error) {
	s.wg.Add(1)
	defer s.wg.Done()
	resp, err := s.sqs.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            s.queueURL,
		WaitTimeSeconds:     aws.Int64(20),
		MaxNumberOfMessages: aws.Int64(1),
	})
	if err != nil {
		errChannel <- err
		return
	}
	for _, msg := range resp.Messages {
		message, err := decodeFromSQSMessage(msg.Body)
		if err != nil {
			errChannel <- err
			continue
		}
		channel <- &awsMessage{
			ctx:        ctx,
			subscriber: s,
			messageID:  *msg.ReceiptHandle,
			message:    message,
			metadata:   decodeMessageAttributes(msg.MessageAttributes),
		}
	}
}
