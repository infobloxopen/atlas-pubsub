package aws

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
	pubsub "github.com/infobloxopen/atlas-pubsub"
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

func (p publisher) Publish(ctx context.Context, msg []byte, metadata map[string]string) error {
	message := encodeToSNSMessage(msg)
	messageAttributes := encodeMessageAttributes(metadata)

	_, publishErr := p.sns.PublishWithContext(ctx, &sns.PublishInput{
		TopicArn:          aws.String(p.topicArn),
		Message:           message,
		MessageAttributes: messageAttributes,
	})

	return publishErr
}
