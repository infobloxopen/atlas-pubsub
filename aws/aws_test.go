package aws

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// setupVerifyPermissions sets up a publisher and subscriber and returns the mocks for further configuration.
// When you're ready to exercise the verifyPermissions function, call the returned func
func setupVerifyPermissions(t *testing.T) (*mockSNS, *mockSQS, func() error) {
	queueArn := "testQueueArn"
	snsMock := mockSNS{}
	sqsMock := mockSQS{
		stubbedReceiveMessageMessages: []*sqs.Message{
			mustWrapIntoSQSMessage(t, []byte(fmt.Sprintf("Permissions Verification Test Message. Subscription queue: %s", queueArn)), aws.String("testHandle"), nil),
		},
	}
	p, err := newPublisher(&snsMock, "topic")
	if err != nil {
		t.Fatalf("expected no error from newPublisher, but got: %v", err)
	}
	s, err := newSubscriber(&snsMock, &sqsMock, "topic", "subscriptionID")
	if err != nil {
		t.Fatalf("expected no error from newSubscriber, but got: %v", err)
	}
	s.queueArn = aws.String(queueArn)

	return &snsMock, &sqsMock, func() error { return verifyPermissions(s, p) }
}

func TestVerifyPermissions(t *testing.T) {
	{ // verify default setup passes
		_, _, test := setupVerifyPermissions(t)
		if err := test(); err != nil {
			t.Errorf("expected default VerifyPermissions to pass, but got error %v", err)
		}
	}
	{ // verify any kind of publish error returns an error
		snsMock, _, test := setupVerifyPermissions(t)
		snsMock.stubbedPublishError = errors.New("test publish error")
		actual := test()
		if expected := snsMock.stubbedPublishError; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	}
	{ // verify receiving the wrong message causes an error
		_, sqsMock, test := setupVerifyPermissions(t)
		sqsMock.stubbedReceiveMessageMessages = []*sqs.Message{
			mustWrapIntoSQSMessage(t, []byte("the wrong message"), nil, nil),
		}
		if actual := test(); actual == nil {
			t.Errorf("expected an error, but got nil")
		}
	}
	{ // verify subscriber receiving error causes an error
		_, sqsMock, test := setupVerifyPermissions(t)
		sqsMock.stubbedReceiveMessageMessages = []*sqs.Message{
			&sqs.Message{Body: aws.String("some malformed message")},
		}
		if actual := test(); actual == nil {
			t.Errorf("expected an error, but got nil")
		}
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
		expectedQueueName := "ps_topic-subscriptionID"
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

func TestEnsureQueueAttributes(t *testing.T) {
	queueURL := aws.String("queueURL")
	testCases := []struct {
		RetentionDuration         time.Duration
		VisibilityDuration        time.Duration
		CallsAWS                  bool
		ExpectedErr               error
		ExpectedVisibilityTimeout int64
		ExpectedRetentionTimeout  int64
	}{
		{RetentionDuration: 1 * time.Minute, VisibilityDuration: 0 * time.Second, CallsAWS: true, ExpectedRetentionTimeout: 60, ExpectedVisibilityTimeout: 0},
		{RetentionDuration: 1 * time.Minute, VisibilityDuration: 43200 * time.Second, CallsAWS: true, ExpectedRetentionTimeout: 60, ExpectedVisibilityTimeout: 43200},
		{RetentionDuration: 1 * time.Minute, VisibilityDuration: -1 * time.Second, ExpectedErr: ErrVisibilityTimeoutOutOfRange},
		{RetentionDuration: 1 * time.Minute, VisibilityDuration: 1000 * time.Minute, ExpectedErr: ErrVisibilityTimeoutOutOfRange},
		{RetentionDuration: 1 * time.Minute, ExpectedRetentionTimeout: 60, ExpectedErr: errors.New("expected SetQueueAttributes error"), CallsAWS: true},
		{RetentionDuration: 5 * time.Minute, VisibilityDuration: 0 * time.Second, CallsAWS: true, ExpectedRetentionTimeout: 300},
		{RetentionDuration: 60 * time.Second, VisibilityDuration: 0 * time.Second, CallsAWS: true, ExpectedRetentionTimeout: 60},
		{RetentionDuration: 1209600 * time.Second, VisibilityDuration: 0 * time.Second, CallsAWS: true, ExpectedRetentionTimeout: 1209600},
		{RetentionDuration: -1 * time.Second, VisibilityDuration: 0 * time.Second, ExpectedErr: ErrMessageRetentionPeriodOutOfRange},
		{RetentionDuration: 1000 * time.Hour, VisibilityDuration: 0 * time.Second, ExpectedErr: ErrMessageRetentionPeriodOutOfRange},
	}
	for _, tc := range testCases {
		sqsMock := mockSQS{
			stubbedSetQueueAttributesError: tc.ExpectedErr,
		}
		actualErr := ensureQueueAttributes(queueURL, tc.RetentionDuration, tc.VisibilityDuration, &sqsMock)
		expectedErr := tc.ExpectedErr
		if expectedErr != actualErr {
			t.Errorf("expected error %v, but got %v", expectedErr, actualErr)
		}
		if sqsMock.spiedSetQueueAttributesInput == nil {
			if tc.CallsAWS {
				t.Errorf("expected call to AWS, but none was made")
			}
			continue
		}
		if !tc.CallsAWS {
			t.Errorf("expected not to call AWS, but it was called")
		}
		expectedRetentionTimeout := *aws.String(strconv.Itoa(int(tc.ExpectedRetentionTimeout)))
		actualRetentionTimeout := *sqsMock.spiedSetQueueAttributesInput.Attributes[sqs.QueueAttributeNameMessageRetentionPeriod]
		if expectedRetentionTimeout != actualRetentionTimeout {
			t.Errorf("expected retention duration to be %s, but was %s", expectedRetentionTimeout, actualRetentionTimeout)
		}
		expectedVisibilityTimeout := *aws.String(strconv.Itoa(int(tc.ExpectedVisibilityTimeout)))
		actualVisibilityTimeout := *sqsMock.spiedSetQueueAttributesInput.Attributes[sqs.QueueAttributeNameVisibilityTimeout]
		if expectedVisibilityTimeout != actualVisibilityTimeout {
			t.Errorf("expected visibility timeout to be %s, but was %s", expectedVisibilityTimeout, actualVisibilityTimeout)
		}
		actualQueueURL := *sqsMock.spiedSetQueueAttributesInput.QueueUrl
		if *queueURL != actualQueueURL {
			t.Errorf("expected queue URL to be %q, but was %q", *queueURL, actualQueueURL)
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
		t.Errorf("expected: %q\nactual:%q", expected, *actual)
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
	tests := []struct {
		input     *string
		expected  map[string]string
		expectErr bool
	}{
		{input: nil, expected: make(map[string]string)},
		{input: aws.String(``), expected: make(map[string]string)},
		{input: aws.String(`{}`), expected: make(map[string]string)},
		{input: aws.String(`{"SomeUnrelatedThing": "SomeUnrelatedValue"}`), expected: make(map[string]string)},
		{input: aws.String(`{"MessageAttributes":{}}`), expected: make(map[string]string)},
		{input: aws.String(`{"MessageAttributes":{"foo":{}}}`), expected: make(map[string]string)},
		{input: aws.String(`{"MessageAttributes":{"foo":{"Type":"NotString", "Value":"bar"}}}`), expected: make(map[string]string)},
		{input: aws.String(`{"MessageAttributes":{"foo":{"Type":"String", "Value":""}}}`), expected: map[string]string{"foo": ""}},
		{input: aws.String(`{"MessageAttributes":{"foo":{"Type":"String"}}}`), expected: map[string]string{"foo": ""}},
		{input: aws.String(`{"MessageAttributes":{"foo":{"Type":"String", "Value":"bar"}}}`), expected: map[string]string{"foo": "bar"}},
		{input: aws.String(`{"MessageAttributes":{"foo":{"Type":"", "Value":"bar"}}}`), expected: make(map[string]string)},
		{input: aws.String(`{"MessageAttributes":{"foo":{"Value":"bar"}}}`), expected: make(map[string]string)},
		{input: aws.String("some malformed thing"), expectErr: true},
		{input: aws.String(`{"MessageAttributes":some malformed thing}`), expectErr: true},
		{input: aws.String(`{"MessageAttributes":"not a map"}`), expectErr: true},
	}
	for _, testCase := range tests {
		expected := testCase.expected
		actual, err := decodeMessageAttributes(testCase.input)
		if (err != nil) != testCase.expectErr {
			t.Errorf("expected to get error: %v, actual error: %v", testCase.expectErr, err)
		}
		if !reflect.DeepEqual(expected, actual) {
			t.Errorf("expected decoded message attributes to be %v, but was %v", expected, actual)
		}
	}
}
