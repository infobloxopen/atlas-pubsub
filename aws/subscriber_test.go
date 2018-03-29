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
	msgChannel, errChannel := s.Start(ctx, nil)

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

	msgChannel, errChannel := s.Start(context.Background(), map[string]string{"foo": "bar"})
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

// verify that no calls to update the subscription happens if the passed-in filter matches the existing filter
func TestEnsureFilterPolicy_NewFilterMatches_NoModificationDone(t *testing.T) {
	filterPolicy := map[string]string{"foo": "bar"}
	snsMock := &mockSNS{
		stubbedGetSubscriptionAttributesOutput: &sns.GetSubscriptionAttributesOutput{Attributes: map[string]*string{"FilterPolicy": mustEncodeFilterPolicy(t, filterPolicy)}},
	}
	subscriber := awsSubscriber{
		sns: snsMock,
	}
	subscriber.ensureFilterPolicy(filterPolicy)
	if snsMock.spiedSubscribeInput != nil {
		t.Error("sns.Subscribe was called, but shouldn't have been")
	}
	if snsMock.spiedSetSubscriptionAttributesInput != nil {
		t.Error("sns.SetSubscriptionAttributes was called, but shouldn't have been")
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

// TestDeleteSubscription verifies that a sqs queue can be deleted successfully
func TestDeleteSubscription(t *testing.T) {
	sqsMock := mockSQS{}
	s, err := newSubscriber(&mockSNS{}, &sqsMock, "topic", "subscriptionID")
	if err != nil {
		t.Errorf("expected no error from newSubscriber, but got: %v", err)
		return
	}

	s.queueURL = aws.String("testurl")
	sqsMock.stubbedDeleteQueueError = errors.New("test error")
	actualErr := s.DeleteSubscription()
	actualQueueURL := *sqsMock.spiedDeleteQueueInput.QueueUrl
	expectedQueueURL := *s.queueURL

	// verify that DeleteSubscription cathes errors
	if actualErr != sqsMock.stubbedDeleteQueueError {
		t.Errorf("expected error %v, but got %v", sqsMock.stubbedDeleteQueueError, actualErr)
	}

	// verify that the queueUrls match
	if expectedQueueURL != actualQueueURL {
		t.Errorf("AWS queue url was incorrect, expected: \"%s\", actual: \"%s\"", expectedQueueURL, actualQueueURL)
	}

	// verify that DeleteSubscription passes successfully
	sqsMock.stubbedDeleteQueueError = nil
	actualErr = s.DeleteSubscription()
	if actualErr != nil {
		t.Errorf("expected no error from DeleteSubscription, but got: %v", actualErr)
	}
}

// TestExtendAckDeadline verifies that a messages visibility changes correctly
func TestExtendAckDeadline(t *testing.T) {
	expectedMessage := []byte("foo")
	sqsMock := mockSQS{
		stubbedReceiveMessageMessages: []*sqs.Message{
			mustWrapIntoSQSMessage(t, expectedMessage, aws.String("testHandle"), nil),
			&sqs.Message{Body: aws.String("testmessage")},
		},
	}
	s, err := newSubscriber(&mockSNS{}, &sqsMock, "topic", "subscriptionID")
	if err != nil {
		t.Errorf("expected no error from newSubscriber, but got: %v", err)
	}
	s.queueURL = aws.String("testurl")
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	msgChannel, errChannel := s.Start(ctx, nil)

	{ // test a successful message
		msg := <-msgChannel
		{ // verify ExtendAckDeadline error is catching errors
			// Pass a good value
			sampleDuration := time.Duration(50) * time.Second
			actual := msg.ExtendAckDeadline(sampleDuration)
			if actual != nil {
				t.Errorf("expected no error, but got %v", actual)
			}
			// Check if parameter values (queueUrl, message) are the same
			actualMessage := msg.Message()
			if !bytes.Equal(expectedMessage, actualMessage) {
				t.Errorf("expected %v, but got %v", expectedMessage, actualMessage)
			}
			actualQueueURL := *sqsMock.spiedChangeMessageVisibilityInput.QueueUrl
			expectedQueueURL := *s.queueURL
			if expectedQueueURL != actualQueueURL {
				t.Errorf("expected %v, but got %v", expectedQueueURL, actualQueueURL)
			}
			// Pass a value above the range
			sqsMock.stubbedChangeMessageVisibilityError = errors.New("The visibility timeout value is out of range. Values can be 0 to 43200 seconds")
			sampleDuration = time.Duration(500000) * time.Second
			actual = msg.ExtendAckDeadline(sampleDuration)
			expected := sqsMock.stubbedChangeMessageVisibilityError
			if actual.Error() != expected.Error() {
				t.Errorf("expected error %v, but got %v", expected, actual)
			}
			// Pass a value below the range
			sampleDuration = time.Duration(-10) * time.Second
			actual = msg.ExtendAckDeadline(sampleDuration)
			if actual.Error() != expected.Error() {
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
}
