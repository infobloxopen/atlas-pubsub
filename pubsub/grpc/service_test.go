package grpc

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/infobloxopen/atlas-pubsub/pubsub"
)

type mockPublisher struct {
	spiedConstructorTopicName string
	stubbedConstructorError   error

	spiedPublishMessage []byte
	stubbedPublishError error
}

func (p *mockPublisher) Publish(ctx context.Context, message []byte) error {
	p.spiedPublishMessage = message
	return p.stubbedPublishError
}

func mockPublisherFactory(mock *mockPublisher) func(context.Context, string) (pubsub.Publisher, error) {
	return func(ctx context.Context, topic string) (pubsub.Publisher, error) {
		mock.spiedConstructorTopicName = topic
		return mock, mock.stubbedConstructorError
	}
}

func TestPublish(t *testing.T) {
	mock := &mockPublisher{}
	service := NewPubSubServer(mockPublisherFactory(mock), nil)
	publishRequest := &PublishRequest{
		Topic:   "testTopic",
		Message: []byte{1, 2, 3, 4, 5}}

	{ // verify an error in constructor propagates through Publish
		expectedErr := errors.New("test constructor error")
		mock.stubbedConstructorError = expectedErr

		_, actualErr := service.Publish(context.Background(), publishRequest)
		if expectedErr != actualErr {
			t.Errorf("constructor error was incorrect:\nexpected:%v\nactual:%v", expectedErr, actualErr)
		}
	}
	{ // verify a Publish error propagates through Publish
		mock.stubbedConstructorError = nil

		expectedErr := errors.New("test publish error")
		mock.stubbedPublishError = expectedErr
		_, actualErr := service.Publish(context.Background(), publishRequest)
		if expectedErr != actualErr {
			t.Errorf("publish error was incorrect:\nexpected:%v\nactual:%v", expectedErr, actualErr)
		}
	}
	{ // verify the topic and message are forwarded to the pubsub.Publisher implementation
		mock.stubbedConstructorError = nil
		mock.stubbedPublishError = nil

		expectedTopic := publishRequest.GetTopic()
		expectedMessage := publishRequest.GetMessage()
		service.Publish(context.Background(), publishRequest)
		if expectedTopic != mock.spiedConstructorTopicName {
			t.Errorf("expected topic name to be %q, but was %q", expectedTopic, mock.spiedConstructorTopicName)
		}
		if !bytes.Equal(expectedMessage, mock.spiedPublishMessage) {
			t.Errorf("expected message to be %v, but was %v", expectedMessage, mock.spiedPublishMessage)
		}
	}
}
