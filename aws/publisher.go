package aws

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
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
func NewPublisher(sess *session.Session, topic string) (pubsub.Publisher, error) {
	return newPublisher(sns.New(sess), topic)
}

func newPublisher(snsClient snsiface.SNSAPI, topic string) (*publisher, error) {
	log.Printf("AWS: ensuring SNS topic exists for %q", topic)
	topicArn, err := ensureTopic(topic, snsClient)
	if err != nil {
		return nil, err
	}

	p := publisher{
		sns:      snsClient,
		topicArn: *topicArn,
	}

	return &p, nil
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

func (p publisher) DeleteTopic() error {
	_, err := p.sns.DeleteTopic(&sns.DeleteTopicInput{
		TopicArn: aws.String(p.topicArn),
	})
	return err
}
