package aws

import (
	"context"
	"reflect"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	pubsub "github.com/infobloxopen/atlas-pubsub"
)

// NewSubscriber creates an AWS message broker that will subscribe to
// the given topic with at-least-once message delivery semantics for the given
// subscriptionID
// TODO: info on permissions needed within the config to make this work
func NewSubscriber(topic, subscriptionID string) (pubsub.Subscriber, error) {
	sess, err := ensureSession()
	if err != nil {
		return nil, err
	}

	return newSubscriber(sns.New(sess), sqs.New(sess), topic, subscriptionID)
}

func newSubscriber(snsClient snsiface.SNSAPI, sqsClient sqsiface.SQSAPI, topic, subscriptionID string) (pubsub.Subscriber, error) {
	subscriber := awsSubscriber{sns: snsClient, sqs: sqsClient}
	err := subscriber.ensureSubscription(topic, subscriptionID)
	if err != nil {
		return nil, err
	}
	return &subscriber, nil
}

type awsSubscriber struct {
	sns snsiface.SNSAPI
	sqs sqsiface.SQSAPI

	queueURL        *string
	queueArn        *string
	topicArn        *string
	subscriptionArn *string

	wg sync.WaitGroup
}

func (s *awsSubscriber) Start(ctx context.Context, filter map[string]string) (<-chan pubsub.Message, <-chan error) {
	channel := make(chan pubsub.Message)
	errChannel := make(chan error)
	go func() {
		defer close(channel)
		if fErr := s.ensureFilterPolicy(filter); fErr != nil {
			errChannel <- fErr
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
	_, error := s.sqs.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      s.queueURL,
		ReceiptHandle: aws.String(messageID),
	})
	return error
}

func (s *awsSubscriber) ExtendAckDeadline(ctx context.Context, messageID string, newDuration time.Duration) error {
	panic("not implemented")
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
func (m *awsMessage) ExtendAckDeadline(time.Duration) error {
	panic("not implemented")
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
	queueName, err := buildAWSQueueName(topic, subscriptionID)
	if err != nil {
		return err
	}
	// create the queue if needed
	s.queueURL, err = ensureQueue(queueName, s.sqs)
	if err != nil {
		return err
	}
	// create the topic if needed
	s.topicArn, err = ensureTopic(topic, s.sns)
	if err != nil {
		return err
	}
	// set the queue policy to allow SNS
	err = ensureQueuePolicy(s.queueURL, s.topicArn, s.sqs)
	if err != nil {
		return err
	}
	// subscribe the queue to the topic
	s.queueArn, s.subscriptionArn, err = ensureQueueSubscription(s.queueURL, s.topicArn, s.sns, s.sqs)
	if err != nil {
		return err
	}

	return nil
}

func (s *awsSubscriber) ensureFilterPolicy(filter map[string]string) error {
	attrs, err := s.sns.GetSubscriptionAttributes(&sns.GetSubscriptionAttributesInput{SubscriptionArn: s.subscriptionArn})
	if err != nil {
		return err
	}

	currentFilterPolicy, err := decodeFilterPolicy(attrs.Attributes["FilterPolicy"])
	if err != nil || !reflect.DeepEqual(currentFilterPolicy, filter) {
		/*
		   If the new filter is empty, we need to delete the subscription and recreate it.
		   This is needed because the AWS API won't allow you to set an empty FilterPolicy
		*/
		if len(filter) == 0 {
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
		metadata := decodeMessageAttributes(msg.MessageAttributes)
		if err != nil {
			errChannel <- err
			continue
		}
		channel <- &awsMessage{
			ctx:        ctx,
			subscriber: s,
			messageID:  *msg.ReceiptHandle,
			message:    message,
			metadata:   metadata,
		}
	}
}
