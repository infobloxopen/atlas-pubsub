package aws

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	pubsub "github.com/infobloxopen/atlas-pubsub"
)

// TestStart will run through a single successful message and then one mangled message
func TestStart(t *testing.T) {
	expectedMessage := []byte("foo")
	expectedHandle := "fooHandle"
	expectedMetadata := map[string]string{"foo": "bar"}

	sqsMock := mockSQS{
		stubbedReceiveMessageMessages: []*sqs.Message{
			mustWrapIntoSQSMessage(t, expectedMessage, aws.String(expectedHandle), expectedMetadata),
			&sqs.Message{Body: aws.String("some mangled message")},
		},
	}

	s, serr := newSubscriber(&mockSNS{}, &sqsMock, "topic", "subscriptionID")
	if serr != nil {
		t.Errorf("expected no error from newSubscriber, but got: %v", serr)
		return
	}
	ctx, stop := context.WithCancel(context.Background())
	defer stop()

	msgChannel, errChannel := s.Start(ctx)

	{ // test a successful message
		msg := <-msgChannel

		{ // verify message type
			actualMessage := msg.Message()
			if !bytes.Equal(expectedMessage, actualMessage) {
				t.Errorf("expected %v, but got %v", expectedMessage, actualMessage)
			}
		}
		{ // verify messageID
			actualHandle := msg.MessageID()
			if expectedHandle != actualHandle {
				t.Errorf("expected %q, but got %q", expectedHandle, actualHandle)
			}
		}
		{ // verify metadata
			actualMetadata := msg.Metadata()
			if !reflect.DeepEqual(expectedMetadata, actualMetadata) {
				t.Errorf("expected %v, but got %v", expectedMetadata, actualMetadata)
			}
		}
		{ // verify Ack error is forwarded
			sqsMock.stubbedDeleteMessageError = errors.New("expected sqs.DeleteMessage error")
			ackError := msg.Ack()
			{ // verify the messageID is forwarded
				expectedHandle := msg.MessageID()
				actualHandle := *sqsMock.spiedDeleteMessageInput.ReceiptHandle
				if expectedHandle != actualHandle {
					t.Errorf("expected %q, but got %q", expectedHandle, actualHandle)
				}
			}
			if ackError == nil {
				t.Error("expected Ack to return an error, but didn't")
			}
		}
		{ // verify ExtendAckDeadline error is catching errors
			// Pass a good value
			actual := msg.ExtendAckDeadline(time.Second)
			if actual != nil {
				t.Errorf("expected no error, but got %v", actual)
			}
			// Pass a bad value
			sqsMock.stubbedChangeMessageVisibilityError = errors.New("test extend ack error")
			actual = msg.ExtendAckDeadline(time.Second)
			expected := sqsMock.stubbedChangeMessageVisibilityError
			if actual != expected {
				t.Errorf("expected error %v, but got %v", expected, actual)
			}
		}
	}
	{ // test an error message
		err := <-errChannel
		if err == nil {
			t.Error("expected an error, but got nothing")
		}
	}
	{ // test cancelling via the context
		stop()
		// hacky way to wait for the control loop to receive the cancellation and
		// close the channel
		time.Sleep(10 * time.Millisecond)

		_, isOpen := <-msgChannel
		if isOpen {
			t.Error("expected channel to be closed, but was open")
		}
	}
}

// verify that the channel is closed immediately if there are errors trying to
// set a filter policy
func TestStart_FilterPolicyError_ClosesMessageChannel(t *testing.T) {
	snsMock := mockSNS{}

	s, serr := newSubscriber(&snsMock, &mockSQS{}, "topic", "subscriptionID")
	if serr != nil {
		t.Errorf("expected no error from newSubscriber, but got: %v", serr)
		return
	}

	snsMock.stubbedSetSubscriptionAttributesError = errors.New("test ensureFilterPolicy error")

	msgChannel, errChannel := s.Start(context.Background(), pubsub.Filter(map[string]string{"foo": "bar"}))
	time.Sleep(10 * time.Millisecond)

	{
		expected := snsMock.stubbedSetSubscriptionAttributesError
		actual := <-errChannel
		if expected != actual {
			t.Errorf("expected error %v, but got %v", expected, actual)
		}
		{
			_, isOpen := <-msgChannel
			if isOpen {
				t.Errorf("expected msgChannel to be closed, but was open")
			}
		}
	}
}

func TestFilterPoliciesEqual(t *testing.T) {
	testCases := []struct {
		l, r     map[string]string
		expected bool
	}{
		{nil, nil, true},
		{make(map[string]string), nil, true},
		{make(map[string]string), make(map[string]string), true},
		{map[string]string{"foo": "bar"}, map[string]string{"foo": "bar"}, true},
		{map[string]string{"foo": "bar"}, map[string]string{"foo": "baz"}, false},
		{map[string]string{"foo": "bar"}, map[string]string{}, false},
		{map[string]string{"foo": "bar"}, map[string]string{"foo": "bar", "baz": "qux"}, false},
	}

	for _, tc := range testCases {
		if actual := filterPoliciesEqual(tc.l, tc.r); tc.expected != actual {
			t.Errorf("expected '%v' for %v, %v, but was '%v'", tc.expected, tc.l, tc.r, actual)
		}
	}
}

// verify that no calls to update the subscription happens if the passed-in filter matches the existing filter
func TestEnsureFilterPolicy_NewFilterMatches_NoModificationDone(t *testing.T) {
	testCases := []struct {
		existing map[string]string
		new      map[string]string
	}{
		{map[string]string{"foo": "bar"}, map[string]string{"foo": "bar"}},
		{make(map[string]string), make(map[string]string)},
		{make(map[string]string), nil},
		{nil, make(map[string]string)},
		{nil, nil},
	}
	for _, tc := range testCases {
		snsMock := &mockSNS{
			stubbedGetSubscriptionAttributesOutput: &sns.GetSubscriptionAttributesOutput{Attributes: map[string]*string{"FilterPolicy": mustEncodeFilterPolicy(t, tc.existing)}},
		}
		subscriber := awsSubscriber{
			sns: snsMock,
		}
		subscriber.ensureFilterPolicy(tc.new)
		if snsMock.spiedSubscribeInput != nil {
			t.Error("sns.Subscribe was called, but shouldn't have been")
		}
		if snsMock.spiedSetSubscriptionAttributesInput != nil {
			t.Error("sns.SetSubscriptionAttributes was called, but shouldn't have been")
		}
	}
}

// verify that if you're trying to clear a filter policy, it unsubscribes then
// resubscribes
func TestEnsureFilterPolicy_ClearingFilter_ResubscribesToPolicy(t *testing.T) {
	snsMock := &mockSNS{
		stubbedGetSubscriptionAttributesOutput: &sns.GetSubscriptionAttributesOutput{
			Attributes: map[string]*string{
				"FilterPolicy": mustEncodeFilterPolicy(t, map[string]string{"foo": "bar"}),
			},
		},
		stubbedSubscribeOutput: &sns.SubscribeOutput{SubscriptionArn: aws.String("NewSubscriptionArn")},
	}
	subscriber := awsSubscriber{
		sns:             snsMock,
		subscriptionArn: aws.String("testSubscriptionArn"),
	}
	subscriber.ensureFilterPolicy(nil)
	if snsMock.spiedUnsubscribeInput == nil {
		t.Error("expected Unsubsribe to be called, but wasn't")
	}
	if snsMock.spiedSubscribeInput == nil {
		t.Error("expected Subscribe to be called, but wasn't")
	}
	if snsMock.spiedSetSubscriptionAttributesInput != nil {
		t.Error("SetSubscriptionAttributesInput was called, but shouldn't have been")
	}
}

// verify that if you pass in a different filter than what was there, it'll update
// the filter policy in SNS
func TestEnsureFilterPolicy_DifferentPolicy_SetsSubscriptionAttributes(t *testing.T) {
	snsMock := mockSNS{
		stubbedGetSubscriptionAttributesOutput: &sns.GetSubscriptionAttributesOutput{
			Attributes: map[string]*string{
				"FilterPolicy": mustEncodeFilterPolicy(t, map[string]string{"foo": "bar"}),
			},
		},
	}
	subscriber := awsSubscriber{sns: &snsMock}
	subscriber.ensureFilterPolicy(map[string]string{"baz": "qux"})
	if snsMock.spiedSubscribeInput != nil {
		t.Error("sns.Subscribe was called, but shouldn't have been")
	}
	if snsMock.spiedSetSubscriptionAttributesInput == nil {
		t.Error("expected SetSubscriptionAttributes to be called, but wasn't")
	}
}

// TestDeleteQueue verifies that a sqs queue can be deleted successfully
func TestDeleteQueue(t *testing.T) {
	sqsMock := mockSQS{}
	s, err := newSubscriber(&mockSNS{}, &sqsMock, "topic", "subscriptionID")
	if err != nil {
		t.Errorf("expected no error from newSubscriber, but got: %v", err)
		return
	}

	s.queueURL = aws.String("testurl")
	sqsMock.stubbedDeleteQueueError = errors.New("test error")
	actualErr := s.deleteQueue()
	actualQueueURL := *sqsMock.spiedDeleteQueueInput.QueueUrl
	expectedQueueURL := *s.queueURL

	// verify that DeleteQueue cathes errors
	if actualErr != sqsMock.stubbedDeleteQueueError {
		t.Errorf("expected error %v, but got %v", sqsMock.stubbedDeleteQueueError, actualErr)
	}

	// verify that the queueUrls match
	if expectedQueueURL != actualQueueURL {
		t.Errorf("AWS queue url was incorrect, expected: \"%s\", actual: \"%s\"", expectedQueueURL, actualQueueURL)
	}

	// verify that DeleteQueue passes successfully
	sqsMock.stubbedDeleteQueueError = nil
	actualErr = s.deleteQueue()
	if actualErr != nil {
		t.Errorf("expected no error from DeleteQueue, but got: %v", actualErr)
	}
}

// TestDeleteSubscription verifies that a sns subscription can be deleted successfully.
func TestDeleteSubscription(t *testing.T) {
	sqsMock := mockSQS{}
	snsMock := mockSNS{}
	s, err := newSubscriber(&snsMock, &sqsMock, "topic", "subscriptionID")
	if err != nil {
		t.Errorf("expected no error from newSubscriber, but got: %v", err)
		return
	}

	s.subscriptionArn = aws.String("testSubscriptionArn")
	snsMock.stubbedUnsubscribeError = errors.New("test error")
	actualErr := s.DeleteSubscription(context.Background())
	actualSubsArn := *snsMock.spiedUnsubscribeInput.SubscriptionArn
	expectedSubsArn := *s.subscriptionArn

	// verify that DeleteSubscription cathes errors
	if actualErr != snsMock.stubbedUnsubscribeError {
		t.Errorf("expected error %v, but got %v", snsMock.stubbedUnsubscribeError, actualErr)
	}

	// verify that the queueUrls match
	if expectedSubsArn != actualSubsArn {
		t.Errorf("AWS subscription arn was incorrect, expected: \"%s\", actual: \"%s\"", expectedSubsArn, actualSubsArn)
	}

	// verify that DeleteSubscription passes successfully
	snsMock.stubbedUnsubscribeError = nil
	actualErr = s.DeleteSubscription(context.Background())
	if actualErr != nil {
		t.Errorf("expected no error from DeleteSubscription, but got: %v", actualErr)
	}
}

// TestExtendAckDeadline verifies that a messages visibility changes correctly
func TestExtendAckDeadline(t *testing.T) {
	expectedMessageID := "test-message"
	testCases := []struct {
		Duration                  time.Duration
		CallsAWS                  bool
		ExpectedErr               error
		ExpectedVisibilityTimeout int64
	}{
		{Duration: 5 * time.Minute, CallsAWS: true, ExpectedVisibilityTimeout: 300},
		{Duration: -1 * time.Second, ExpectedErr: ErrAckDeadlineOutOfRange},
		{Duration: 1000 * time.Minute, ExpectedErr: ErrAckDeadlineOutOfRange},
		{ExpectedErr: errors.New("test stubbed error"), CallsAWS: true},
	}
	for _, tc := range testCases {
		sqsMock := mockSQS{
			stubbedChangeMessageVisibilityError: tc.ExpectedErr,
		}
		s, err := newSubscriber(&mockSNS{}, &sqsMock, "topic", "subscriptionID")
		if err != nil {
			t.Errorf("expected no error from newSubscriber, but got: %v", err)
		}
		s.queueURL = aws.String("testurl")

		actualErr := s.ExtendAckDeadline(context.Background(), expectedMessageID, tc.Duration)
		expectedErr := tc.ExpectedErr
		if expectedErr != actualErr {
			t.Errorf("expected error %v, but got %v", expectedErr, actualErr)
		}
		if sqsMock.spiedChangeMessageVisibilityInput == nil {
			if tc.CallsAWS {
				t.Errorf("expected call to AWS, but none was made")
			}
			continue
		}
		if !tc.CallsAWS {
			t.Errorf("expected not to call AWS, but it was called")
		}
		actualMessageID := *sqsMock.spiedChangeMessageVisibilityInput.ReceiptHandle
		if expectedMessageID != actualMessageID {
			t.Errorf("expected MessageID to be %q, but was %q", expectedMessageID, actualMessageID)
		}
		actualVisibilityTimeout := *sqsMock.spiedChangeMessageVisibilityInput.VisibilityTimeout
		if tc.ExpectedVisibilityTimeout != actualVisibilityTimeout {
			t.Errorf("expected visibility timeout to be %d, but was %d", tc.ExpectedVisibilityTimeout, actualVisibilityTimeout)
		}
		actualQueueURL := *sqsMock.spiedChangeMessageVisibilityInput.QueueUrl
		if *s.queueURL != actualQueueURL {
			t.Errorf("expected queue URL to be %q, but was %q", *s.queueURL, actualQueueURL)
		}
	}
}
