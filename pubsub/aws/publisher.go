package aws

import (
	"context"

	"github.com/infobloxopen/atlas-pubsub/pubsub"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
)

// NewPublisher creates a new AWS message broker that will publish
// messages to the given topic.
// TODO: info on permissions needed within the config to make this work
//
// Topic names must be made up of only uppercase and lowercase
// ASCII letters, numbers, underscores, and hyphens, and must be between 1 and
// 247 characters long.
func NewPublisher(config *aws.Config, topic string) (pubsub.Publisher, error) {
	sess, err := ensureSession(config)
	if err != nil {
		return nil, err
	}
	return newPublisher(sns.New(sess), topic)
}

func newPublisher(snsClient snsiface.SNSAPI, topic string) (pubsub.Publisher, error) {
	topicArn, err := ensureTopic(topic, snsClient)
	if err != nil {
		return nil, err
	}

	p := publisher{
		sns:      snsClient,
		topicArn: *topicArn,
	}

	return p, nil
}

type publisher struct {
	sns      snsiface.SNSAPI
	topicArn string
}

func (p publisher) Publish(ctx context.Context, msg []byte) error {
	message, err := encodeToSNSMessage(msg)
	if err != nil {
		return err
	}

	_, publishErr := p.sns.PublishWithContext(ctx, &sns.PublishInput{
		TopicArn: aws.String(p.topicArn),
		Message:  message,
	})

	return publishErr
}
