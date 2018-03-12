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

// func TestEnsureQueueSubscription(t *testing.T) {
// 	snsSpy := mockSNS{}
// 	sqsSpy := mockSQS{}
// 	queueURL := aws.String("queueURL")
// 	topicArn := aws.String("topicArn")
// 	{ // verify error is returned when GetQueueAttributes returns error
// 		expectedErr := errors.New("test error for sqs.GetQueueAttributes")
// 		sqsSpy.stubbedGetQueueAttributesError = expectedErr
// 		actualErr := ensureQueueSubscription(queueURL, topicArn, &snsSpy, &sqsSpy)
// 		if actualErr == nil {
// 			t.Errorf("expected GetQueueAttributes to return error \"%s\", but didn't", expectedErr.Error())
// 		} else if actualErr.Error() != expectedErr.Error() {
// 			t.Errorf("expected GetQueueAttributes to return error \"%s\", but returned \"%s\"", expectedErr.Error(), actualErr.Error())
// 		}
// 		sqsSpy.stubbedGetQueueAttributesError = nil
// 	}
// 	expectedEndpoint := aws.String("expectedQueueArn")
// 	sqsSpy.stubbedGetQueueAttributesOutput = &sqs.GetQueueAttributesOutput{
// 		Attributes: map[string]*string{"QueueArn": expectedEndpoint},
// 	}
// 	{ // verify error is returned when sns.Subscribe returns error
// 		expectedErr := errors.New("test error for sns.Subscribe")
// 		snsSpy.stubbedSubscribeError = expectedErr
// 		actualErr := ensureQueueSubscription(queueURL, topicArn, &snsSpy, &sqsSpy)
// 		if actualErr == nil {
// 			t.Errorf("expected Subscribe to return error \"%s\", but didn't", expectedErr.Error())
// 		} else if actualErr.Error() != expectedErr.Error() {
// 			t.Errorf("expected Subscribe to return error \"%s\", but returned \"%s\"", expectedErr.Error(), actualErr.Error())
// 		}
// 		snsSpy.stubbedSubscribeError = nil
// 	}
// 	{ // verify sns.Subscribe subscribes the given topicArn to the queueArn returned from sqs.GetQueueAttributes
// 		ensureQueueSubscription(queueURL, topicArn, &snsSpy, &sqsSpy)
// 		actualEndpoint := snsSpy.spiedSubscribeInput.Endpoint
// 		if actualEndpoint != expectedEndpoint {
// 			t.Errorf("expected Subscribe to have endpoint \"%s\", but was \"%s\"", *expectedEndpoint, *actualEndpoint)
// 		}
// 		actualTopicArn := snsSpy.spiedSubscribeInput.TopicArn
// 		if actualTopicArn != topicArn {
// 			t.Errorf("expected Subscribe topicArn to be \"%s\", but was \"%s\"", *topicArn, *actualTopicArn)
// 		}
// 	}
// 	happyPathErr := ensureQueueSubscription(queueURL, topicArn, &snsSpy, &sqsSpy)
// 	if happyPathErr != nil {
// 		t.Errorf("expected happy path to not return error, but returned \"%s\"", happyPathErr.Error())
// 	}
// }

// func TestEnsureFilterPolicy(t *testing.T) {
// 	sqsSpy := mockSQS{}
// 	queueURL := aws.String("queueURL")
// 	filter := map[string]string{"foo": "bar"}
// 	ensureFilterPolicy(queueURL, filter, &sqsSpy)
// 	if sqsSpy.spiedSetQueueAttributesInput == nil {
// 		t.Error("expected SetQueueAttributes to be called, but wasn't")
// 	} else {
// 		actualQueueURL := sqsSpy.spiedSetQueueAttributesInput.QueueUrl
// 		if queueURL != actualQueueURL {
// 			t.Errorf("expected SetQueueAttributes to be called on queueURL %q, but was %q", queueURL, actualQueueURL)
// 		}
//
// 		actualFilter := *sqsSpy.spiedSetQueueAttributesInput.Attributes["FilterPolicy"]
// 		expectedFilter, _ := encodeFilterPolicy(filter)
//
// 		if expectedFilter != actualFilter {
// 			t.Errorf("expected filter to be %q, but was %q", expectedFilter, actualFilter)
// 		}
// 	}
// }

func TestEncodeFilterPolicy(t *testing.T) {
	{
		expected := fmt.Sprintf("{%q:[%q]}", "foo", "bar")
		actual, err := encodeFilterPolicy(map[string]string{"foo": "bar"})
		if err != nil {
			t.Errorf("wasn't expecting error:\n%v", err)
		}
		if expected != *actual {
			t.Errorf("expected: %q\nactual:%q", expected, actual)
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
