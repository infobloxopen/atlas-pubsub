package aws

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
)

type mockSNS struct {
	snsiface.SNSAPI
	// whenever sns.CreateTopic is called, the input is stored here
	spiedCreateTopicInput *sns.CreateTopicInput
	// whenever sns.CreateTopic is called, this will be returned for the error field
	stubbedCreateTopicError error
	// whenever sns.PublishWithContext is called, the input is stored here
	spiedPublishInput *sns.PublishInput

	spiedSubscribeInput   *sns.SubscribeInput
	stubbedSubscribeError error
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
	return &sns.PublishOutput{MessageId: aws.String("someUniqueMessageId")}, nil
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

	stubbedSetQueueAttributesError error

	// if true, calling ReceiveMessage will return an error if there are no more
	// messages in stubbedReceiveMessageMessages
	useStrictMessageOrdering      bool
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

func (mock *mockSQS) SetQueueAttributes(*sqs.SetQueueAttributesInput) (*sqs.SetQueueAttributesOutput, error) {
	return &sqs.SetQueueAttributesOutput{}, mock.stubbedSetQueueAttributesError
}

func (mock *mockSQS) ReceiveMessageWithContext(ctx aws.Context, input *sqs.ReceiveMessageInput, options ...request.Option) (*sqs.ReceiveMessageOutput, error) {
	var msgs []*sqs.Message
	numMsgs := int(*input.MaxNumberOfMessages)
	if mock.stubbedReceiveMessageMessages == nil || len(mock.stubbedReceiveMessageMessages) == 0 {
		if mock.useStrictMessageOrdering {
			return nil, errors.New("no more messages to send")
		}
		msgs = []*sqs.Message{mustWrapIntoSQSMessage(&TestProto{Value: "foo"}, nil)}
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

// TestProto is a proto.Message implementation for testing
type TestProto struct {
	Value string `protobuf:"bytes,1,opt,name=value" json:"value,omitempty"`
}

func (m *TestProto) Reset()         { *m = TestProto{} }
func (m *TestProto) String() string { return proto.CompactTextString(m) }
func (*TestProto) ProtoMessage()    {}

// mustWrapIntoSQSMessage panics if wrapIntoSQSMessage returns an error. This is
// a convenience format for cases where you want to inline the wrapped message
func mustWrapIntoSQSMessage(testMsg proto.Message, receiptHandle *string) *sqs.Message {
	msg, err := wrapIntoSQSMessage(testMsg, receiptHandle)
	if err != nil {
		panic(err)
	}

	return msg
}

// wrapIntoSQSMessage helps wrap a given proto.Message into the format used when
// receiving a message from an SQS queue
func wrapIntoSQSMessage(testMsg proto.Message, receiptHandle *string) (*sqs.Message, error) {
	encoded, err := encodeToSNSMessage(testMsg)
	if err != nil {
		return nil, fmt.Errorf("did not expect encodeToSNSMessage to return err, but returned: %v", err)
	}
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

	return &sqs.Message{
		Body:          aws.String(string(marshalled)),
		ReceiptHandle: handle,
	}, nil
}

func TestWrapIntoSQSMessage(t *testing.T) {
	expected := TestProto{Value: "foo"}
	msg := mustWrapIntoSQSMessage(&expected, nil)
	bytes, _ := decodeFromSQSMessage(msg.Body)
	actual := TestProto{}
	proto.Unmarshal(bytes, &actual)

	if expected.Value != actual.Value {
		t.Errorf("expected \"%s\", got \"%s\"", expected.Value, actual.Value)
	}
}
