package grpc

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	pubsub "github.com/infobloxopen/atlas-pubsub"
)

type mockPublisher struct {
	spiedConstructorTopicName string
	stubbedConstructorError   error

	spiedPublishMessage  []byte
	spiedPublishMetadata map[string]string
	stubbedPublishError  error
}

func (p *mockPublisher) Publish(ctx context.Context, message []byte, metadata map[string]string) error {
	p.spiedPublishMessage = message
	p.spiedPublishMetadata = metadata
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
	server := NewPubSubServer(mockPublisherFactory(mock), nil)
	publishRequest := &PublishRequest{
		Topic:    "testTopic",
		Message:  []byte{1, 2, 3, 4, 5},
		Metadata: map[string]string{"foo": "bar"},
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
		expectedMetadata := publishRequest.GetMetadata()
		server.Publish(context.Background(), publishRequest)
		if expectedTopic != mock.spiedConstructorTopicName {
			t.Errorf("expected topic name to be %q, but was %q", expectedTopic, mock.spiedConstructorTopicName)
		}
		if !bytes.Equal(expectedMessage, mock.spiedPublishMessage) {
			t.Errorf("expected message to be %v, but was %v", expectedMessage, mock.spiedPublishMessage)
		}
		if !reflect.DeepEqual(expectedMetadata, mock.spiedPublishMetadata) {
			t.Errorf("expected metadata to be %v, but was %v", expectedMetadata, mock.spiedPublishMetadata)
		}
	}
}

type mockSubscriber struct {
	spiedConstructorTopicName      string
	spiedConstructorSubscriptionID string
	stubbedConstructorError        error

	spiedStartContext          context.Context
	spiedStartFilter           map[string]string
	stubbedStartMessageChannel chan pubsub.Message
	stubbedStartErrorChannel   chan error

	spiedAckMessageMessagID string
	stubbedAckMessageError  error
}

func (s *mockSubscriber) Start(ctx context.Context, filter map[string]string) (<-chan pubsub.Message, <-chan error) {
	s.spiedStartContext = ctx
	s.spiedStartFilter = filter
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

	spiedSendMessage         []byte
	spiedSendMessageID       string
	spiedSendMessageMetadata map[string]string
	stubbedSendError         error
}

func (ss *mockSubscribeServer) Send(resp *SubscribeResponse) error {
	ss.spiedSendMessage = resp.GetMessage()
	ss.spiedSendMessageID = resp.GetMessageId()
	ss.spiedSendMessageMetadata = resp.GetMetadata()
	return ss.stubbedSendError
}

func (ss *mockSubscribeServer) Context() context.Context {
	return ss.stubbedContext
}

type mockMessage struct {
	messageID string
	message   []byte
	metadata  map[string]string
}

func (m mockMessage) MessageID() string                     { return m.messageID }
func (m mockMessage) Message() []byte                       { return m.message }
func (m mockMessage) Metadata() map[string]string           { return m.metadata }
func (m mockMessage) ExtendAckDeadline(time.Duration) error { return nil }
func (m mockMessage) Ack() error                            { return nil }

func setupServerForSubscribeTest() (*SubscribeRequest, *mockSubscriber, *mockSubscribeServer, func() error) {
	mock := &mockSubscriber{
		stubbedStartMessageChannel: make(chan pubsub.Message),
		stubbedStartErrorChannel:   make(chan error),
	}
	mockSubscribe := &mockSubscribeServer{stubbedContext: context.Background()}
	server := NewPubSubServer(nil, mockSubscriberFactory(mock))
	subscribeRequest := &SubscribeRequest{
		Topic:          "testTopic",
		SubscriptionId: "testSubscriptionID",
		Filter:         map[string]string{"foo": "bar"},
	}

	return subscribeRequest, mock, mockSubscribe, func() error { return server.Subscribe(subscribeRequest, mockSubscribe) }
}

func TestServerSubscribe_MainCase(t *testing.T) {
	subscribeRequest, mock, mockSubscribe, test := setupServerForSubscribeTest()
	done := make(chan bool)

	go func() {
		test()
		done <- true
	}()
	time.Sleep(10 * time.Millisecond)
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
	{ // verify filter passed through successfully
		expectedFilter := subscribeRequest.GetFilter()
		actualFilter := mock.spiedStartFilter
		if !reflect.DeepEqual(expectedFilter, actualFilter) {
			t.Errorf("expected filter to be %v, but was %v", expectedFilter, actualFilter)
		}
	}
	{ // verify a message sent through a channel gets sent through PubSub_SubscribeServer
		expectedMessageID := "test messageID"
		expectedMessage := []byte{1, 2, 3, 4}
		expectedMetadata := map[string]string{"foo": "bar"}
		mock.stubbedStartMessageChannel <- mockMessage{
			messageID: expectedMessageID,
			message:   expectedMessage,
			metadata:  expectedMetadata,
		}
		time.Sleep(10 * time.Millisecond)
		actualMessageID := mockSubscribe.spiedSendMessageID
		actualMessage := mockSubscribe.spiedSendMessage
		actualMetadata := mockSubscribe.spiedSendMessageMetadata
		if expectedMessageID != actualMessageID {
			t.Errorf("expected messageID %q, but got %q", expectedMessageID, actualMessageID)
		}
		if !bytes.Equal(expectedMessage, actualMessage) {
			t.Errorf("expected message %v, but got %v", expectedMessage, actualMessage)
		}
		if !reflect.DeepEqual(expectedMetadata, actualMetadata) {
			t.Errorf("expected metadata %v, but got %v", expectedMetadata, actualMetadata)
		}
	}
	{ // verify Subscribe terminates when the subscriber channel closes
		close(mock.stubbedStartMessageChannel)
		if terminated := <-done; !terminated {
			t.Error("expected subscribe to terminate when channel closed, but didn't")
		}
	}
}

func TestServerSubscribe_ContextDone_TerminatesStream(t *testing.T) {
	_, _, mockSubscribe, test := setupServerForSubscribeTest()
	var cancel context.CancelFunc
	mockSubscribe.stubbedContext, cancel = context.WithCancel(context.Background())
	cancel()
	if err := test(); err != nil {
		t.Errorf("didn't expect error, but got %v", err)
	}
}

func TestServerSubscribe_FactoryError_TerminatesStream(t *testing.T) {
	_, mock, _, test := setupServerForSubscribeTest()

	expectedErr := errors.New("test subscribe constructor error")
	mock.stubbedConstructorError = expectedErr
	actualErr := test()
	if expectedErr != actualErr {
		t.Errorf("constructor error was incorrect:\nexpected: %v\nactual: %v", expectedErr, actualErr)
	}
}

func TestServerSubscribe_SendError_TerminatesStream(t *testing.T) {
	_, mock, mockSubscribe, test := setupServerForSubscribeTest()

	expectedErr := errors.New("test send error")
	mockSubscribe.stubbedSendError = expectedErr
	go func() { mock.stubbedStartMessageChannel <- mockMessage{} }()
	actualErr := test()
	if expectedErr != actualErr {
		t.Errorf("unexpected send error:\nexpected: %v\nactual: %v", expectedErr, actualErr)
	}
}

func TestServerSubscribe_ChannelError(t *testing.T) {
	_, mock, _, test := setupServerForSubscribeTest()

	expectedErr := errors.New("test error channel error")
	go func() { mock.stubbedStartErrorChannel <- expectedErr }()
	actualErr := test()
	if actualErr != expectedErr {
		t.Errorf("unexpected error channel error:\nexpected: %v\nactual: %v", expectedErr, actualErr)
	}
}

func TestServerAck(t *testing.T) {
	mock := &mockSubscriber{}
	server := NewPubSubServer(nil, mockSubscriberFactory(mock))
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
