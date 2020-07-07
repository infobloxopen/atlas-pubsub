package aws

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
	pubsub "github.com/infobloxopen/atlas-pubsub"
	"github.com/sirupsen/logrus"
)

type PublisherOption func(*publisher)

func PublishWithLogger(logger *logrus.Logger) PublisherOption {
	return func(pub *publisher) {
		pub.logger = logger
	}
}

// NewPublisher creates a new AWS message broker that will publish
// messages to the given topic.
// TODO: info on permissions needed within the config to make this work
//
// Topic names must be made up of only uppercase and lowercase
// ASCII letters, numbers, underscores, and hyphens, and must be between 1 and
// 247 characters long.
func NewPublisher(sess *session.Session, topic string, opts ...PublisherOption) (pubsub.Publisher, error) {
	return newPublisher(sns.New(sess), topic, opts...)
}

func newPublisher(snsClient snsiface.SNSAPI, topic string, opts ...PublisherOption) (*publisher, error) {
	p := publisher{
		sns:    snsClient,
		logger: logrus.StandardLogger(),
	}
	for _, opt := range opts {
		opt(&p)
	}

	p.logger.Infof("AWS: ensuring SNS topic exists for %q", topic)
	topicArn, err := ensureTopic(topic, snsClient)
	if err != nil {
		return nil, err
	}
	p.topicArn = *topicArn

	return &p, nil
}

type publisher struct {
	sns      snsiface.SNSAPI
	topicArn string
	logger   *logrus.Logger
}

func (p publisher) Publish(ctx context.Context, msg []byte, metadata map[string]string) error {
	p.logger.Infof("AWS: publish to topic %q", p.topicArn)
	message := encodeToSNSMessage(msg)
	messageAttributes := encodeMessageAttributes(metadata)

	_, publishErr := p.sns.PublishWithContext(ctx, &sns.PublishInput{
		TopicArn:          aws.String(p.topicArn),
		Message:           message,
		MessageAttributes: messageAttributes,
	})

	return publishErr
}

func (p publisher) DeleteTopic(ctx context.Context) error {
	p.logger.Infof("AWS: delete topic %q", p.topicArn)
	_, err := p.sns.DeleteTopic(&sns.DeleteTopicInput{
		TopicArn: aws.String(p.topicArn),
	})
	return err
}
