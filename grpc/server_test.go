package grpc

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	pubsub "github.com/infobloxopen/atlas-pubsub"
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

func mockPublisherFactory(mock *mockPublisher) PublisherFactory {
	return func(ctx context.Context, topic string) (pubsub.Publisher, error) {
		mock.spiedConstructorTopicName = topic
		return mock, mock.stubbedConstructorError
	}
}

func TestServerPublish(t *testing.T) {
	mock := &mockPublisher{}
	server := NewPubSubServer(mockPublisherFactory(mock), nil, nil)
	publishRequest := &PublishRequest{
		Topic:   "testTopic",
		Message: []byte{1, 2, 3, 4, 5},
	}

	{ // verify an error in constructor propagates through Publish
		expectedErr := errors.New("test constructor error")
		mock.stubbedConstructorError = expectedErr

		_, actualErr := server.Publish(context.Background(), publishRequest)
		if expectedErr != actualErr {
			t.Errorf("constructor error was incorrect:\nexpected:%v\nactual:%v", expectedErr, actualErr)
		}
		mock.stubbedConstructorError = nil
	}
	{ // verify a Publish error propagates through Publish
		expectedErr := errors.New("test publish error")
		mock.stubbedPublishError = expectedErr
		_, actualErr := server.Publish(context.Background(), publishRequest)
		if expectedErr != actualErr {
			t.Errorf("publish error was incorrect:\nexpected:%v\nactual:%v", expectedErr, actualErr)
		}
		mock.stubbedPublishError = nil
	}
	{ // verify the topic and message are forwarded to the pubsub.Publisher implementation
		expectedTopic := publishRequest.GetTopic()
		expectedMessage := publishRequest.GetMessage()
		server.Publish(context.Background(), publishRequest)
		if expectedTopic != mock.spiedConstructorTopicName {
			t.Errorf("expected topic name to be %q, but was %q", expectedTopic, mock.spiedConstructorTopicName)
		}
		if !bytes.Equal(expectedMessage, mock.spiedPublishMessage) {
			t.Errorf("expected message to be %v, but was %v", expectedMessage, mock.spiedPublishMessage)
		}
	}
}

type mockSubscriber struct {
	spiedConstructorTopicName      string
	spiedConstructorSubscriptionID string
	stubbedConstructorError        error

	spiedStartContext          context.Context
	stubbedStartMessageChannel chan pubsub.Message
	stubbedStartErrorChannel   chan error

	spiedAckMessageMessagID string
	stubbedAckMessageError  error
}

func (s *mockSubscriber) Start(ctx context.Context) (<-chan pubsub.Message, <-chan error) {
	s.spiedStartContext = ctx
	mc := s.stubbedStartMessageChannel
	if mc == nil {
		mc = make(chan pubsub.Message)
	}
	ec := s.stubbedStartErrorChannel
	if ec == nil {
		ec = make(chan error)
	}
	return mc, ec
}

func (s *mockSubscriber) AckMessage(ctx context.Context, messageID string) error {
	s.spiedAckMessageMessagID = messageID
	return s.stubbedAckMessageError
}
func (s *mockSubscriber) ExtendAckDeadline(ctx context.Context, messageID string, newDuration time.Duration) error {
	return nil
}

func mockSubscriberFactory(mock *mockSubscriber) SubscriberFactory {
	return func(ctx context.Context, topic, subscriptionID string) (pubsub.Subscriber, error) {
		mock.spiedConstructorTopicName = topic
		mock.spiedConstructorSubscriptionID = subscriptionID
		return mock, mock.stubbedConstructorError
	}
}

type mockSubscribeServer struct {
	PubSub_SubscribeServer

	stubbedContext context.Context

	spiedSendMessage   []byte
	spiedSendMessageID string
	stubbedSendError   error
}

func (ss *mockSubscribeServer) Send(resp *SubscribeResponse) error {
	ss.spiedSendMessage = resp.GetMessage()
	ss.spiedSendMessageID = resp.GetMessageId()

	return ss.stubbedSendError
}

func (ss *mockSubscribeServer) Context() context.Context {
	return ss.stubbedContext
}

type mockMessage struct {
	messageID string
	message   []byte
}

func (m mockMessage) MessageID() string                     { return m.messageID }
func (m mockMessage) Message() []byte                       { return m.message }
func (m mockMessage) ExtendAckDeadline(time.Duration) error { return nil }
func (m mockMessage) Ack() error                            { return nil }

func TestSubscribe(t *testing.T) {
	var errorHandlerError error
	mock := &mockSubscriber{}
	mockSubscribe := &mockSubscribeServer{}
	server := NewPubSubServer(nil, mockSubscriberFactory(mock), func(err error) { errorHandlerError = err })
	subscribeRequest := &SubscribeRequest{Topic: "testTopic", SubscriptionId: "testSubscriptionID"}

	{ // verify an error in constructor propagates through Start
		expectedErr := errors.New("test subscribe constructor error")
		mock.stubbedConstructorError = expectedErr
		actualErr := server.Subscribe(subscribeRequest, mockSubscribe)
		if expectedErr != actualErr {
			t.Errorf("constructor error was incorrect:\nexpected: %v\nactual: %v", expectedErr, actualErr)
		}
		mock.stubbedConstructorError = nil
	}
	{ // verify topic name passed through successfully
		expectedTopic := subscribeRequest.GetTopic()
		actualTopic := mock.spiedConstructorTopicName
		if expectedTopic != actualTopic {
			t.Errorf("expected topic to be %q, but was %q", expectedTopic, actualTopic)
		}
	}
	{ // verify subscriptionID passed through successfully
		expectedSubID := subscribeRequest.GetSubscriptionId()
		actualSubID := mock.spiedConstructorSubscriptionID
		if expectedSubID != actualSubID {
			t.Errorf("expected subscriptionID to be %q, but was %q", expectedSubID, actualSubID)
		}
	}
	{
		mock.stubbedStartMessageChannel = make(chan pubsub.Message)
		mock.stubbedStartErrorChannel = make(chan error)
		mockSubscribe.stubbedContext = context.Background()
		done := make(chan bool)
		go func() {
			server.Subscribe(subscribeRequest, mockSubscribe)
			done <- true
		}()
		time.Sleep(10 * time.Millisecond)
		{ // verify a message sent through a channel gets sent through PubSub_SubscribeServer
			expectedMessageID := "test messageID"
			expectedMessage := []byte{1, 2, 3, 4}
			mock.stubbedStartMessageChannel <- mockMessage{
				messageID: expectedMessageID,
				message:   expectedMessage,
			}
			time.Sleep(10 * time.Millisecond)
			actualMessageID := mockSubscribe.spiedSendMessageID
			actualMessage := mockSubscribe.spiedSendMessage

			if expectedMessageID != actualMessageID {
				t.Errorf("expected messageID %q, but got %q", expectedMessageID, actualMessageID)
			}
			if !bytes.Equal(expectedMessage, actualMessage) {
				t.Errorf("expected message %v, but got %v", expectedMessage, actualMessage)
			}
		}
		{ // verify errors passed through error channel get handled
			expectedErr := errors.New("test error channel error")
			mock.stubbedStartErrorChannel <- expectedErr
			time.Sleep(10 * time.Millisecond)
			if errorHandlerError != expectedErr {
				t.Errorf("unexpected error channel error:\nexpected: %v\nactual: %v", expectedErr, errorHandlerError)
			}
			errorHandlerError = nil
		}
		{ // verify a send error propagates to the error handler
			expectedErr := errors.New("test send error")
			mockSubscribe.stubbedSendError = expectedErr
			mock.stubbedStartMessageChannel <- mockMessage{}
			time.Sleep(100 * time.Millisecond)
			if expectedErr != errorHandlerError {
				t.Errorf("unexpected send error:\nexpected: %v\nactual: %v", expectedErr, errorHandlerError)
			}
			errorHandlerError = nil
			mockSubscribe.stubbedSendError = nil
		}
		{ // verify Subscribe terminates when the subscriber channel closes
			close(mock.stubbedStartMessageChannel)
			if terminated := <-done; !terminated {
				t.Error("expected subscribe to terminate when channel closed, but didn't")
			}
		}
	}
	{ // verify Subscribe terminates when the context is cancelled
		mock.stubbedStartMessageChannel = make(chan pubsub.Message)
		var cancel context.CancelFunc
		mockSubscribe.stubbedContext, cancel = context.WithCancel(context.Background())
		done := make(chan bool)
		go func() {
			server.Subscribe(subscribeRequest, mockSubscribe)
			done <- true
		}()
		time.Sleep(10 * time.Millisecond)
		cancel()
		if terminated := <-done; !terminated {
			t.Error("expected subscribe to terminate when context was cancelled, but didn't")
		}
	}
}

func TestServerAck(t *testing.T) {
	mock := &mockSubscriber{}
	server := NewPubSubServer(nil, mockSubscriberFactory(mock), nil)
	ackRequest := &AckRequest{
		Topic:          "testTopic",
		SubscriptionId: "testSubID",
		MessageId:      "testMessageID",
	}
	{ // verify an error in constructor propagates through Ack
		expectedErr := errors.New("test constructor error")
		mock.stubbedConstructorError = expectedErr
		_, actualErr := server.Ack(context.Background(), ackRequest)
		if expectedErr != actualErr {
			t.Errorf("publish error was incorrect:\nexpected: %v\nactual: %v", expectedErr, actualErr)
		}
		mock.stubbedConstructorError = nil
	}
	{ // verify topic, subscriptionID and messageID are passed to subscriber
		expectedTopic := ackRequest.GetTopic()
		expectedSubscriptionID := ackRequest.GetSubscriptionId()
		expectedMessageID := ackRequest.GetMessageId()
		server.Ack(context.Background(), ackRequest)
		actualTopic := mock.spiedConstructorTopicName
		actualSubscriptionID := mock.spiedConstructorSubscriptionID
		actualMessageID := mock.spiedAckMessageMessagID

		if expectedTopic != actualTopic {
			t.Errorf("expected topic to be %q, but was %q", expectedTopic, actualTopic)
		}
		if expectedSubscriptionID != actualSubscriptionID {
			t.Errorf("expected subscriptionID to be %q, but was %q", expectedSubscriptionID, actualSubscriptionID)
		}
		if expectedMessageID != actualMessageID {
			t.Errorf("expected messageID to be %q, but was %q", expectedMessageID, actualMessageID)
		}
	}
	{ // verify ack errors are passed to subscriber
		expectedErr := errors.New("test ack error")
		mock.stubbedAckMessageError = expectedErr
		_, actualErr := server.Ack(context.Background(), ackRequest)
		if expectedErr != actualErr {
			t.Errorf("ack error was incorrect:\nexpected: %v\nactual: %v", expectedErr, actualErr)
		}
	}
}
