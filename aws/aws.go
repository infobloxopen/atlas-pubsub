package aws

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/google/uuid"
)

// some arbitrary prefix I came up with to help distinguish between aws broker
// queues, topics and queues, topics created for other means
const topicNamePrefix = "pubsub__"

// AWS SQS queue names cannot exceed 80 characters. The naming convention for
// queues is to have the prefix+topic+subscriptionID, so we have to share these
// 80 characters between the 3 items.
const subscriptionIDMaxLength = 40

// AWS SNS topic names can be up to 256 characters, but because of our naming
// convention we're limited to the total 80-character max of the SQS name length.
const topicNameMaxLength = 40 - len(topicNamePrefix)

// VerifyPermissions checks if the aws config exists and checks if it has permissions to
// create sns topics, send messages, create SQS topics, delete topics, and delete sqs queues
func VerifyPermissions(sess *session.Session) error {
	// Check if environment contains aws config
	topic := "verifyPermissions"
	subscriptionID := uuid.New().String()

	log.Println("verify permissions: creating subscriber")
	subscriber, err := newSubscriber(sns.New(sess), sqs.New(sess), topic, subscriptionID)
	if err != nil {
		return err
	}

	log.Println("verify permissions: creating publisher")
	publisher, err := newPublisher(sns.New(sess), topic)
	if err != nil {
		return err
	}
	return verifyPermissions(subscriber, publisher)
}

// verifyPermissions checks if the aws config has correct permissions
func verifyPermissions(subscriber *awsSubscriber, publisher *publisher) error {
	defer func() {
		// Delete publisher topic and subscriber queue
		log.Println("verify permissions: deleting subscription")
		subscriber.DeleteSubscription()
	}()

	ctx, stop := context.WithTimeout(context.Background(), 1*time.Second)
	defer stop()

	// Filter for the correct subscription
	md := map[string]string{"subscription": *subscriber.queueArn}

	log.Println("verify permissions: starting subscription")
	c, e := subscriber.Start(ctx, md)
	testMessage := []byte("Permissions Verification Test Message. Subscription queue: " + *subscriber.queueArn)

	log.Println("verify permissions: publishing test message")
	err := publisher.Publish(ctx, testMessage, md)
	if err != nil {
		return err
	}

	select {
	case msg, isOpen := <-c:
		if !isOpen {
			return errors.New("error, channel closed prematurely")
		}
		if bytes.Equal(msg.Message(), testMessage) {
			log.Println("verify permissions: success")
			return nil
		}
		return errors.New("error, received the wrong message from publisher")
	case err := <-e:
		return err
	}
}

// Utility functions for performing AWS commands (create SNS topic, SQS queue, etc)

// buildAWSTopicName takes in a topic name and returns a formatted version fitting
// this broker's naming convention. Errors will be returned if the topic name is
// too long.
//
// The going convention is that this broker prepends "pubsub__" to any topic
// name, so that administration of topics and queues created by this aws broker
// is easier from within the console or through any other management tools
func buildAWSTopicName(topic string) (*string, error) {
	if topicLength := len(topic); topicLength == 0 {
		return nil, errors.New("topic name is required")
	} else if topicLength > topicNameMaxLength {
		return nil, fmt.Errorf("topic name must be no more than %v characters", topicNameMaxLength)
	}

	return aws.String(fmt.Sprintf("%s%s", topicNamePrefix, topic)), nil
}

// buildAWSQueueName takes in a topic and subscription and returns a formatted
// version based on this broker's naming convention. Errors will be returned if
// the topic/subscription combo is too long
//
// The going convention is that the queue name concatenates the aws-formatted
// topic name with the subscriptionID, separated by a dash.
//
// Example: for topic `foo` and subcription `bar`, the queue name would be "pubsub__foo-bar"
//
// This is useful to prevent queue name clashes between message queues while
// allowing an easy identifying mechanism for subscribing to a persistent queue.
func buildAWSQueueName(topic, subscriptionID string) (*string, error) {
	scrubbedTopic, topicErr := buildAWSTopicName(topic)
	if topicErr != nil {
		return nil, topicErr
	}
	if subIDLen := len(subscriptionID); subIDLen == 0 {
		return nil, errors.New("subscriptionID is required")
	} else if subIDLen > subscriptionIDMaxLength {
		return nil, fmt.Errorf("subscriptionID must be no more than %v characters", subscriptionIDMaxLength)
	}
	return aws.String(fmt.Sprintf("%s-%s", *scrubbedTopic, subscriptionID)), nil
}

// ensureTopic takes a topic name and returns the topicArn, creating the SNS
// topic if necessary
func ensureTopic(topic string, snsClient snsiface.SNSAPI) (*string, error) {
	scrubbedTopic, nameErr := buildAWSTopicName(topic)
	if nameErr != nil {
		return nil, nameErr
	}

	topicResp, topicErr := snsClient.CreateTopic(&sns.CreateTopicInput{Name: scrubbedTopic})
	if topicErr != nil {
		return nil, topicErr
	}

	return topicResp.TopicArn, nil
}

// ensureQueue returns the queueURL for the given queueName, creating the queue
// if it doesn't exist
func ensureQueue(queueName *string, sqsClient sqsiface.SQSAPI) (*string, error) {
	queueURLResp, queueURLErr := sqsClient.GetQueueUrl(&sqs.GetQueueUrlInput{QueueName: queueName})
	if queueURLErr == nil {
		return queueURLResp.QueueUrl, nil
	}
	if awsErr, ok := queueURLErr.(awserr.Error); ok && awsErr.Code() == sqs.ErrCodeQueueDoesNotExist {
		createResp, createErr := sqsClient.CreateQueue(&sqs.CreateQueueInput{
			QueueName: queueName,
		})
		if createErr != nil {
			return nil, createErr
		}
		return createResp.QueueUrl, nil
	}
	return nil, queueURLErr
}

// ensureQueuePolicy overwrites any existing policy for the given queue with one
// that allows write permissions from the given SNS topicArn
// An alternative to overwriting all policies for the given queue would be to just
// make sure that this specific policy exists, but that's not a use case I'm
// interested in right now
func ensureQueuePolicy(queueURL, topicArn *string, sqsClient sqsiface.SQSAPI) error {
	// some weird policy thing I stole from Andrei's POC
	sqsPolicy := `
	{
	    "Version": "2012-10-17",
	    "Statement": [
	        {
	            "Effect": "Allow",
	            "Principal": {
	                "AWS": "*"
	            },
	            "Action": "SQS:SendMessage",
	            "Resource": "*",
	            "Condition": {
	                "ArnEquals": {
	                    "aws:SourceArn": "%s"
	                }
	            }
	        }
	    ]
	}
	`
	_, policyErr := sqsClient.SetQueueAttributes(&sqs.SetQueueAttributesInput{
		QueueUrl: queueURL,
		Attributes: map[string]*string{
			"Policy": aws.String(fmt.Sprintf(sqsPolicy, *topicArn)),
		},
	})
	return policyErr
}

// ensureQueueSubscription subscribes the given SQS queue to the given SNS topic
func ensureQueueSubscription(queueURL, topicArn *string, snsClient snsiface.SNSAPI, sqsClient sqsiface.SQSAPI) (*string, *string, error) {
	arnResp, attrErr := sqsClient.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		QueueUrl:       queueURL,
		AttributeNames: []*string{aws.String("QueueArn")},
	})
	if attrErr != nil {
		return nil, nil, attrErr
	}
	queueARN := arnResp.Attributes["QueueArn"]

	subResp, subErr := snsClient.Subscribe(&sns.SubscribeInput{
		Protocol: aws.String("sqs"),
		TopicArn: topicArn,
		Endpoint: queueARN,
	})
	if subErr != nil {
		return nil, nil, subErr
	}
	return queueARN, subResp.SubscriptionArn, nil
}

// encodeFilterPolicy converts the given metadata map into input needed for the
// sqs.Attributes filter policy
func encodeFilterPolicy(filter map[string]string) (*string, error) {
	if filter == nil || len(filter) == 0 {
		return nil, nil
	}

	// SNS wants the value to be an object or array, so we have to convert the given input
	arrayified := make(map[string][]string)
	for k, v := range filter {
		arrayified[k] = []string{v}
	}
	bytes, err := json.Marshal(arrayified)
	return aws.String(string(bytes)), err
}

// decodeFilterPolicy converts the given string from a sns.GetSubscriptionAttributesOutput.Attributes
// value into a map to meet the subscriber interface
func decodeFilterPolicy(filterPolicy *string) (map[string]string, error) {
	decoded := make(map[string]string)
	if filterPolicy == nil {
		return decoded, nil
	}

	var unmarshalled map[string][]string
	if err := json.Unmarshal([]byte(*filterPolicy), &unmarshalled); err != nil {
		return nil, err
	}
	for k, v := range unmarshalled {
		if len(v) > 1 {
			return nil, fmt.Errorf("invalid filter policy for pub/sub: expected single filter parameter but got %v", v)
		}
		decoded[k] = v[0]
	}
	return decoded, nil
}

// encodeToSNSMessage converts the given message into a string to be used
// by SNS
func encodeToSNSMessage(msg []byte) *string {
	return aws.String(base64.StdEncoding.EncodeToString(msg))
}

// encodeMessageAttributes converts the given metadata map into input needed for
// sns.PublishInput.MessageAttributes
func encodeMessageAttributes(metadata map[string]string) map[string]*sns.MessageAttributeValue {
	attributes := make(map[string]*sns.MessageAttributeValue)
	if metadata != nil {
		for key, value := range metadata {
			attributes[key] = &sns.MessageAttributeValue{StringValue: aws.String(value), DataType: aws.String("String")}
		}
	}

	return attributes
}

// decodeFromSQSMessage takes the sqs.Message.Body and unmarshals it into a []byte
func decodeFromSQSMessage(sqsMessage *string) ([]byte, error) {
	v := new(struct {
		Payload string `json:"Message"`
	})
	umErr := json.Unmarshal([]byte(*sqsMessage), v)
	if umErr != nil {
		return nil, umErr
	}

	return base64.StdEncoding.DecodeString(v.Payload)
}

func decodeMessageAttributes(attributes map[string]*sqs.MessageAttributeValue) map[string]string {
	decoded := map[string]string{}
	for key, value := range attributes {
		if *value.DataType == "String" {
			decoded[key] = *value.StringValue
		}
	}

	return decoded
}
