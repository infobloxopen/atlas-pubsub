package aws

import (
	"context"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/infobloxopen/atlas-pubsub/pubsub"
)

// NewSubscriber creates an AWS message broker that will subscribe to
// the given topic with at-least-once message delivery semantics for the given
// subscriptionID
// TODO: info on permissions needed within the config to make this work
func NewSubscriber(config *aws.Config, topic, subscriptionID string) (pubsub.Subscriber, error) {
	sess, err := ensureSession(config)
	if err != nil {
		return nil, err
	}

	return newSubscriber(sns.New(sess), sqs.New(sess), topic, subscriptionID)
}

func newSubscriber(snsClient snsiface.SNSAPI, sqsClient sqsiface.SQSAPI, topic, subscriptionID string) (pubsub.Subscriber, error) {
	queueURL, err := ensureSubscription(topic, subscriptionID, snsClient, sqsClient)
	if err != nil {
		return nil, err
	}

	return &awsSubscriber{
		sns:      snsClient,
		sqs:      sqsClient,
		queueURL: queueURL,
	}, nil
}

type awsSubscriber struct {
	sns snsiface.SNSAPI
	sqs sqsiface.SQSAPI

	queueURL *string
	wg       sync.WaitGroup
}

func (s *awsSubscriber) Start(ctx context.Context) (<-chan pubsub.Message, <-chan error) {
	channel := make(chan pubsub.Message)
	errChannel := make(chan error)
	go func() {
		defer close(channel)

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
	} else {
		for _, msg := range resp.Messages {
			message, err := decodeFromSQSMessage(msg.Body)
			if err != nil {
				errChannel <- err
			} else {
				channel <- &awsMessage{
					ctx:        ctx,
					subscriber: s,
					messageID:  *msg.ReceiptHandle,
					message:    message,
				}
			}
		}
	}
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
	subscriber *awsSubscriber
}

func (m *awsMessage) MessageID() string {
	return m.messageID
}
func (m *awsMessage) Message() []byte {
	return m.message
}
func (m *awsMessage) ExtendAckDeadline(time.Duration) error {
	panic("not implemented")
}
func (m *awsMessage) Ack() error {
	return m.subscriber.AckMessage(m.ctx, m.MessageID())
}
