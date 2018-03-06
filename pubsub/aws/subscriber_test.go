package aws

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/golang/protobuf/proto"
)

func TestStart(t *testing.T) {
	expectedMessages := []string{"foo", "bar", "baz"}
	sqsMock := mockSQS{useStrictMessageOrdering: true}
	snsMock := mockSNS{}
	{ // map the expected messages into sqs messages to be replayed
		msgs := make([]*sqs.Message, len(expectedMessages))
		for i, v := range expectedMessages {
			msgs[i] = mustWrapIntoSQSMessage(&TestProto{Value: v}, nil)
		}
		msgs = append(msgs, &sqs.Message{Body: aws.String("some mangled message")})
		sqsMock.stubbedReceiveMessageMessages = msgs
	}

	s, serr := newAtLeastOnceSubscriber(&snsMock, &sqsMock, "topic", "subscriptionID")
	if serr != nil {
		t.Errorf("expected no error from newSubscriber, but got: %v", serr)
		return
	}
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	msgChannel, errChannel := s.Start(ctx)

	for _, expected := range expectedMessages {
		msg := <-msgChannel
		var actual string
		{
			pb := TestProto{}
			unmarshalErr := proto.Unmarshal(msg.Message(), &pb)
			if unmarshalErr != nil {
				t.Errorf("expected no error from proto.Unmarshal, but got: %v", unmarshalErr)
			}
			actual = pb.Value
		}
		if expected != actual {
			t.Errorf("expected message to be \"%s\", but was \"%s\"", expected, actual)
		}

		sqsMock.stubbedDeleteMessageError = errors.New("expected sqs.DeleteMessage error")
		ackError := msg.Ack()
		if ackError == nil {
			t.Error("expected Ack to return an error, but didn't")
		}
		{
			expectedReceiptHandle := msg.MessageID()
			actualReceiptHandle := *sqsMock.spiedDeleteMessageInput.ReceiptHandle
			if expectedReceiptHandle != actualReceiptHandle {
				t.Errorf("expected sqs.DeleteMessage receiptHandle to be \"%s\", but was \"%s\"", expectedReceiptHandle, actualReceiptHandle)
			}
		}
	}

	err := <-errChannel
	if err == nil {
		t.Error("expected an error, but got nothing")
	}

	stop()
	// hacky way to wait for the control loop to receive the cancellation and
	// close the channel
	time.Sleep(10 * time.Millisecond)

	_, isOpen := <-msgChannel
	if isOpen {
		t.Error("expected channel to be closed, but was open")
	}
}
