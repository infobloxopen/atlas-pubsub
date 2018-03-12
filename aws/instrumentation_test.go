package aws

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/google/uuid"
)

type mockSNS struct {
	snsiface.SNSAPI
	// whenever sns.CreateTopic is called, the input is stored here
	spiedCreateTopicInput *sns.CreateTopicInput
	// whenever sns.CreateTopic is called, this will be returned for the error field
	stubbedCreateTopicError error
	// whenever sns.PublishWithContext is called, the input is stored here
	spiedPublishInput   *sns.PublishInput
	stubbedPublishError error

	spiedSubscribeInput   *sns.SubscribeInput
	stubbedSubscribeError error

	spiedSetSubscriptionAttributesInput   *sns.SetSubscriptionAttributesInput
	stubbedSetSubscriptionAttributesError error
}

// CreateTopic records the arguments passed in and returns the specified mocked error
func (mock *mockSNS) CreateTopic(input *sns.CreateTopicInput) (*sns.CreateTopicOutput, error) {
	mock.spiedCreateTopicInput = input
	return &sns.CreateTopicOutput{TopicArn: aws.String("some fake topic arn")}, mock.stubbedCreateTopicError
}

func (mock *mockSNS) Subscribe(input *sns.SubscribeInput) (*sns.SubscribeOutput, error) {
	mock.spiedSubscribeInput = input
	return &sns.SubscribeOutput{}, mock.stubbedSubscribeError
}

// PublishWithContext records the input argument passed in and returns a stub response
func (mock *mockSNS) PublishWithContext(ctx aws.Context, input *sns.PublishInput, opts ...request.Option) (*sns.PublishOutput, error) {
	mock.spiedPublishInput = input
	return &sns.PublishOutput{MessageId: aws.String("someUniqueMessageId")}, mock.stubbedPublishError
}

func (mock *mockSNS) SetSubscriptionAttributes(input *sns.SetSubscriptionAttributesInput) (*sns.SetSubscriptionAttributesOutput, error) {
	mock.spiedSetSubscriptionAttributesInput = input
	return &sns.SetSubscriptionAttributesOutput{}, mock.stubbedSetSubscriptionAttributesError
}

type mockSQS struct {
	sqsiface.SQSAPI

	spiedGetQueueURLInput    *sqs.GetQueueUrlInput
	stubbedGetQueueURLOutput *sqs.GetQueueUrlOutput
	stubbedGetQueueURLError  error

	spiedCreateQueueInput    *sqs.CreateQueueInput
	stubbedCreateQueueOutput *sqs.CreateQueueOutput
	stubbedCreateQueueError  error

	spiedGetQueueAttributesInput    *sqs.GetQueueAttributesInput
	stubbedGetQueueAttributesOutput *sqs.GetQueueAttributesOutput
	stubbedGetQueueAttributesError  error

	spiedSetQueueAttributesInput   *sqs.SetQueueAttributesInput
	stubbedSetQueueAttributesError error

	stubbedReceiveMessageMessages []*sqs.Message

	spiedDeleteMessageInput   *sqs.DeleteMessageInput
	stubbedDeleteMessageError error
}

func (mock *mockSQS) GetQueueUrl(input *sqs.GetQueueUrlInput) (*sqs.GetQueueUrlOutput, error) {
	mock.spiedGetQueueURLInput = input
	output := mock.stubbedGetQueueURLOutput
	if output == nil && mock.stubbedGetQueueURLError == nil {
		output = &sqs.GetQueueUrlOutput{}
	}

	return output, mock.stubbedGetQueueURLError
}

func (mock *mockSQS) CreateQueue(input *sqs.CreateQueueInput) (*sqs.CreateQueueOutput, error) {
	mock.spiedCreateQueueInput = input
	output := mock.stubbedCreateQueueOutput
	if output == nil {
		output = &sqs.CreateQueueOutput{}
	}

	return output, mock.stubbedCreateQueueError
}

func (mock *mockSQS) GetQueueAttributes(input *sqs.GetQueueAttributesInput) (*sqs.GetQueueAttributesOutput, error) {
	mock.spiedGetQueueAttributesInput = input
	output := mock.stubbedGetQueueAttributesOutput
	if output == nil && mock.stubbedGetQueueAttributesError == nil {
		output = &sqs.GetQueueAttributesOutput{}
	}
	return output, mock.stubbedGetQueueAttributesError
}

func (mock *mockSQS) SetQueueAttributes(input *sqs.SetQueueAttributesInput) (*sqs.SetQueueAttributesOutput, error) {
	mock.spiedSetQueueAttributesInput = input
	return &sqs.SetQueueAttributesOutput{}, mock.stubbedSetQueueAttributesError
}

func (mock *mockSQS) ReceiveMessageWithContext(ctx aws.Context, input *sqs.ReceiveMessageInput, options ...request.Option) (*sqs.ReceiveMessageOutput, error) {
	var msgs []*sqs.Message
	numMsgs := int(*input.MaxNumberOfMessages)
	if mock.stubbedReceiveMessageMessages == nil || len(mock.stubbedReceiveMessageMessages) == 0 {
		time.Sleep(100 * time.Second) // sleep for long enough to time out the test
		panic("no more stubbed messages to return")
	} else if len(mock.stubbedReceiveMessageMessages) < numMsgs {
		msgs = mock.stubbedReceiveMessageMessages
		mock.stubbedReceiveMessageMessages = nil
	} else {
		msgs = mock.stubbedReceiveMessageMessages[:numMsgs]
		mock.stubbedReceiveMessageMessages = mock.stubbedReceiveMessageMessages[numMsgs:]
	}
	return &sqs.ReceiveMessageOutput{Messages: msgs}, nil
}

func (mock *mockSQS) DeleteMessage(input *sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error) {
	mock.spiedDeleteMessageInput = input
	return nil, mock.stubbedDeleteMessageError
}

// mustWrapIntoSQSMessage panics if wrapIntoSQSMessage returns an error. This is
// a convenience format for cases where you want to inline the wrapped message
func mustWrapIntoSQSMessage(testMsg []byte, receiptHandle *string, md map[string]string) *sqs.Message {
	msg, err := wrapIntoSQSMessage(testMsg, receiptHandle, md)
	if err != nil {
		panic(err)
	}

	return msg
}

// wrapIntoSQSMessage helps wrap a given message into the format used when
// receiving a message from an SQS queue
func wrapIntoSQSMessage(testMsg []byte, receiptHandle *string, md map[string]string) (*sqs.Message, error) {
	encoded := encodeToSNSMessage(testMsg)
	payload := struct {
		Payload string `json:"Message"`
	}{
		*encoded,
	}
	marshalled, merr := json.Marshal(payload)
	if merr != nil {
		return nil, fmt.Errorf("did not expect json.Marshal to return err, but returned: %v", merr)
	}

	handle := receiptHandle
	if handle == nil {
		handle = aws.String(uuid.New().String())
	}

	attrs := make(map[string]*sqs.MessageAttributeValue)
	for key, value := range md {
		attrs[key] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(value)}
	}

	return &sqs.Message{
		Body:              aws.String(string(marshalled)),
		ReceiptHandle:     handle,
		MessageAttributes: attrs,
	}, nil
}

func TestWrapIntoSQSMessage(t *testing.T) {
	expected := []byte("foo")
	expectedHandle := "bar"
	expectedMd := map[string]string{"baz": "qux"}
	msg := mustWrapIntoSQSMessage(expected, aws.String(expectedHandle), expectedMd)

	{ // verify message body
		actual, _ := decodeFromSQSMessage(msg.Body)
		if !bytes.Equal(expected, actual) {
			t.Errorf("expected %q, got %q", expected, actual)
		}
	}
	{ // verify handle
		actual := *msg.ReceiptHandle
		if expectedHandle != actual {
			t.Errorf("expected %q, got %q", expectedHandle, actual)
		}
	}
	{ // verify metadata
		for key, expectedValue := range expectedMd {
			if actualEntry, ok := msg.MessageAttributes[key]; !ok {
				t.Errorf("expected %q in map, but wasn't", key)
			} else if actualValue := *actualEntry.StringValue; expectedValue != actualValue {
				t.Errorf("expected %q, got %q", expectedValue, actualValue)
			}
		}
	}
}
