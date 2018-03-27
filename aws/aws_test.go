package aws

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func TestVerifyPermissions(t *testing.T) {
	// Create publihser
	snsMock := mockSNS{}
	p, err := newPublisher(&snsMock, "topic")
	if err != nil {
		t.Errorf("expected no error from newPublisher, but got: %v", err)
		return
	}

	// Create a subscriber
	expectedMessage := []byte("Wrong message")
	expectedHandle := "fooHandle"
	expectedMetadata := map[string]string{"foo": "bar"}
	sqsMock := mockSQS{
		stubbedReceiveMessageMessages: []*sqs.Message{
			mustWrapIntoSQSMessage(t, expectedMessage, aws.String(expectedHandle), expectedMetadata),
			&sqs.Message{Body: aws.String("some message")},
		},
	}
	s, err := newSubscriber(&snsMock, &sqsMock, "topic", "subscriptionID")
	if err != nil {
		t.Errorf("expected no error from newSubscriber, but got: %v", err)
		return
	}
	s.queueArn = aws.String("test-queue-arn")

	// Test to see if publisher sends an error after running Publish code
	snsMock.stubbedPublishError = errors.New("publish message error")
	actual := verifyPermissions(s, p)
	expected := snsMock.stubbedPublishError
	if actual != expected {
		t.Errorf("expected error %v, but got %v", expected, actual)
	}

	// Remove error and see if publish test passes
	snsMock.stubbedPublishError = nil
	metadata := map[string]string{"foo": "bar"}
	message := encodeToSNSMessage([]byte("test"))
	messageAttributes := encodeMessageAttributes(metadata)
	snsMock.spiedPublishInput = &sns.PublishInput{
		TopicArn:          aws.String(p.topicArn),
		Message:           message,
		MessageAttributes: messageAttributes,
	}

	// Test when sending a wrong message
	expectedFail := ("error, received the wrong message from publisher")
	actual = verifyPermissions(s, p)
	if actual.Error() != expectedFail {
		t.Errorf("expected error %v, but got %v", expectedFail, actual)
	}

	// Test when sending a good message
	expectedMessage = []byte("Permissions Verification Test Message. Subscription queue: " + *s.queueArn)
	sqsMock.stubbedReceiveMessageMessages = []*sqs.Message{
		mustWrapIntoSQSMessage(t, []byte(expectedMessage), aws.String(expectedHandle), expectedMetadata),
		&sqs.Message{Body: aws.String("some message")},
	}
	actual = verifyPermissions(s, p)
	if actual != nil {
		t.Errorf("expected no error but got %v", actual)
	}
}

func TestTopicAndQueueNamingMaxLengths(t *testing.T) {
	totalLength := len(topicNamePrefix) + topicNameMaxLength + subscriptionIDMaxLength
	if totalLength > 80 {
		t.Errorf("the total length for a prefix+topic+subscription cannot exceed 80 characters, but is %d", totalLength)
	}
}

func TestBuildAWSQueueName(t *testing.T) {
	{ // verify a bad topic name returns an error
		_, err := buildAWSQueueName(strings.Repeat("a", topicNameMaxLength+1), "subscriptionID")
		if err == nil {
			t.Error("expected buildAWSQueueName to return an error for a bad topic, but didn't")
		}
	}
	{ // verify an empty subscriptionID returns an error
		_, err := buildAWSQueueName("topic", "")
		if err == nil {
			t.Error("expected buildAWSQueueName to return an error for an empty subscriptionID, but didn't")
		}
	}
	{ // verify a too long subscriptionID returns an error
		_, err := buildAWSQueueName("topic", strings.Repeat("a", subscriptionIDMaxLength+1))
		if err == nil {
			t.Error("expected buildAWSQueueName to return an error for a too long subscriptionID, but didn't")
		}
	}
	{ // verify a successful queue name
		expectedQueueName := "pubsub__topic-subscriptionID"
		actualQueueName, err := buildAWSQueueName("topic", "subscriptionID")
		if *actualQueueName != expectedQueueName {
			t.Errorf("expected queuename to be \"%s\", but was \"%s\"", expectedQueueName, *actualQueueName)
		}
		if err != nil {
			t.Errorf("expected buildAWSQueueName to not return an error, but returned: %v", err)
		}
	}
}

func TestEnsureQueue(t *testing.T) {
	sqsSpy := mockSQS{}
	queueName := aws.String("test queue name")
	{ // verify nil error returns queue url
		expectedQueueURL := aws.String("expected queue url")
		sqsSpy.stubbedGetQueueURLOutput = &sqs.GetQueueUrlOutput{QueueUrl: expectedQueueURL}
		actualQueueURL, err := ensureQueue(queueName, &sqsSpy)
		if err != nil {
			t.Errorf("expected GetQueueUrl not to return error, but got \"%v\"", err)
		} else if *actualQueueURL != *expectedQueueURL {
			t.Errorf("expected GetQueueUrl to return \"%s\", but got \"%s\"", *expectedQueueURL, *actualQueueURL)
		}
	}
	{ // verify returning a 404 error creates the queue and returns the created queueURL
		expectedCreatedQueueURL := aws.String("expected queue URL")
		sqsSpy.stubbedGetQueueURLError = awserr.New(sqs.ErrCodeQueueDoesNotExist, "foo", errors.New("foo"))
		sqsSpy.stubbedCreateQueueOutput = &sqs.CreateQueueOutput{QueueUrl: expectedCreatedQueueURL}
		actualCreatedQueueURL, actualErr := ensureQueue(queueName, &sqsSpy)
		if sqsSpy.spiedCreateQueueInput == nil {
			t.Error("expected sqs.CreateQueue to be called, but wasn't")
		} else {
			actualQueueName := sqsSpy.spiedCreateQueueInput.QueueName
			if actualQueueName != queueName {
				t.Errorf("expected sqs.CreateQueue to be called for queue name \"%s\", but was called with \"%s\"", *queueName, *actualQueueName)
			}
			if expectedCreatedQueueURL != actualCreatedQueueURL {
				t.Errorf("expected ensureQueue to return queueURL \"%s\", but was \"%s\"", *expectedCreatedQueueURL, *actualCreatedQueueURL)
			}
			if actualErr != nil {
				t.Errorf("expected ensureQueue to not return an error, but returned: %v", actualErr)
			}
		}
	}
	{ // verify ensureQueue forwards any sqs.CreateQueue errors
		expectedError := errors.New("expected sqs.CreateQueue error")
		sqsSpy.stubbedCreateQueueError = expectedError
		_, actualError := ensureQueue(queueName, &sqsSpy)
		if expectedError != actualError {
			t.Errorf("expected sqs.CreateQueue to return error \"%s\", but returned \"%s\"", expectedError.Error(), actualError.Error())
		}
	}
	{ // verify ensureQueue returns any non-404 errors from sqs.GetQueueURL
		expectedError := errors.New("expected sqs.GetQueueURL error")
		sqsSpy.stubbedGetQueueURLError = expectedError
		_, actualError := ensureQueue(queueName, &sqsSpy)
		if expectedError != actualError {
			t.Errorf("expected sqs.GetQueueURL to return error \"%s\", but return \"%s\"", expectedError.Error(), actualError.Error())
		}
	}
}

func TestEnsureQueuePolicy(t *testing.T) {
	sqsSpy := mockSQS{}
	queueURL := aws.String("queueURL")
	topicArn := aws.String("topicArn")
	{ // verify ensureQueuePolice forwards any sqs.SetQueueAttributes errors
		expectedErr := errors.New("expected SetQueueAttributes error")
		sqsSpy.stubbedSetQueueAttributesError = expectedErr
		actualErr := ensureQueuePolicy(queueURL, topicArn, &sqsSpy)
		if actualErr != expectedErr {
			t.Errorf("expected sqs.SetQueueAttributes to return error :\n%v\nbut was:\n%v", expectedErr, actualErr)
		}
	}
}

func TestEncodeFilterPolicy(t *testing.T) {
	expected := fmt.Sprintf("{%q:[%q]}", "foo", "bar")
	actual, err := encodeFilterPolicy(map[string]string{"foo": "bar"})
	if err != nil {
		t.Errorf("wasn't expecting error:\n%v", err)
	}
	if expected != *actual {
		t.Errorf("expected: %q\nactual:%q", expected, actual)
	}
}

func TestDecodeFilterPolicy(t *testing.T) {
	testCases := []struct {
		input    *string
		expected interface{}
	}{
		{nil, make(map[string]string)},
		{aws.String("{\"foo\":[\"bar\"]}"), map[string]string{"foo": "bar"}},
		{aws.String("{\"foo\":[\"bar\", \"baz\"]}"), errors.New("")},
		{aws.String("some mangled thing"), errors.New("")},
	}
	for _, testCase := range testCases {
		expected := testCase.expected
		if actual, err := decodeFilterPolicy(testCase.input); err != nil {
			if _, ok := expected.(error); !ok {
				t.Errorf("expected %v, but got %v", expected, err)
			}
			if len(actual) != 0 {
				t.Errorf("expected filter policy to be empty, but was %v", actual)
			}
		} else {
			if !reflect.DeepEqual(expected, actual) {
				t.Errorf("expected %v, but got %v", expected, actual)
			}
		}
	}
}

func TestDecodeFromSQSMessage(t *testing.T) {
	expectedValue := []byte("foo")

	sqsMsg, werr := wrapIntoSQSMessage(expectedValue, nil, nil)
	if werr != nil {
		t.Errorf("did not expect wrapIntoSQSMessage to return err, but returned: %v", werr)
	}

	{
		actualValue, derr := decodeFromSQSMessage(sqsMsg.Body)
		if derr != nil {
			t.Errorf("did not expect decodeFromSQSMessage to return err, but returned: %v", derr)
		}
		if !bytes.Equal(expectedValue, actualValue) {
			t.Errorf("expected value to be \"%s\", but was \"%s\"", expectedValue, actualValue)
		}
	}
}

func TestEncodeMessageAttributes(t *testing.T) {
	cases := []struct {
		input    map[string]string
		expected map[string]*sns.MessageAttributeValue
	}{
		{
			map[string]string{"foo": "bar"},
			map[string]*sns.MessageAttributeValue{"foo": &sns.MessageAttributeValue{StringValue: aws.String("bar"), DataType: aws.String("String")}},
		},
		{
			nil,
			make(map[string]*sns.MessageAttributeValue),
		},
	}
	for _, c := range cases {
		actual := encodeMessageAttributes(c.input)
		if !reflect.DeepEqual(c.expected, actual) {
			t.Errorf("expected messageAttributes to be %v, but was %v", c.expected, actual)
		}
	}
}

func TestDecodeMessageAttributes(t *testing.T) {
	input := map[string]*sqs.MessageAttributeValue{
		"foo": &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("bar")},
		"baz": &sqs.MessageAttributeValue{DataType: aws.String("NotString"), StringValue: aws.String("qux")},
	}
	expected := map[string]string{"foo": "bar"}
	actual := decodeMessageAttributes(input)
	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected decoded metadata to be %v, but was %v", expected, actual)
	}
}
