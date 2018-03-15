package aws

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// TestStart will run through a single successful message and then one mangled message
func TestStart(t *testing.T) {
	expectedMessage := []byte("foo")
	expectedHandle := "fooHandle"
	expectedMetadata := map[string]string{"foo": "bar"}

	sqsMock := mockSQS{
		stubbedReceiveMessageMessages: []*sqs.Message{
			mustWrapIntoSQSMessage(expectedMessage, aws.String(expectedHandle), expectedMetadata),
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

// Verify that the channel is closed immediately if there are errors trying to
// set a filter policy
func TestStart_EnsureFilter(t *testing.T) {
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
