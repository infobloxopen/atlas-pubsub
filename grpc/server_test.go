package grpc

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	google_protobuf "github.com/golang/protobuf/ptypes/wrappers"
	pubsub "github.com/infobloxopen/atlas-pubsub"
)

type mockPublisher struct {
	spiedConstructorTopicName string
	stubbedConstructorError   error

	spiedPublishMessage  []byte
	spiedPublishMetadata map[string]string
	stubbedPublishError  error

	stubbedDeleteTopicError error
}

func (p *mockPublisher) Publish(ctx context.Context, message []byte, metadata map[string]string) error {
	p.spiedPublishMessage = message
	p.spiedPublishMetadata = metadata
	return p.stubbedPublishError
}

func (p *mockPublisher) DeleteTopic(ctx context.Context) error {
	return p.stubbedDeleteTopicError
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

func TestDeleteTopic(t *testing.T) {
	mock := &mockPublisher{}
	server := NewPubSubServer(mockPublisherFactory(mock), nil)
	deleteTopicRequest := &DeleteTopicRequest{
		Topic: "testTopic",
	}
	{
		expectedError := errors.New("test constructore error")
		mock.stubbedConstructorError = expectedError

		_, actualError := server.DeleteTopic(context.Background(), deleteTopicRequest)
		if expectedError != actualError {
			t.Errorf("constructor error was incorrect:\nexpected:%v\nactual:%v", expectedError, actualError)
		}
		mock.stubbedConstructorError = nil
	}

	{
		expectedError := errors.New("test delete topic error")
		mock.stubbedDeleteTopicError = expectedError

		_, actualError := server.DeleteTopic(context.Background(), deleteTopicRequest)
		if expectedError != actualError {
			t.Errorf("delete topic error was incorrect:\nexpected:%v\nactual:%v", expectedError, actualError)
		}

		mock.stubbedDeleteTopicError = nil
	}

	{
		expectedTopic := deleteTopicRequest.GetTopic()
		if expectedTopic != mock.spiedConstructorTopicName {
			t.Errorf("expected topic name to be %q, but was %q", expectedTopic, mock.spiedConstructorTopicName)
		}
	}
}

type mockSubscriber struct {
	spiedConstructorTopicName      string
	spiedConstructorSubscriptionID string
	stubbedConstructorError        error

	spiedStartContext           context.Context
	spiedStartFilter            map[string]string
	spiedStartRetentionPeriod   uint64
	spiedStartVisibilityTimeout uint64
	stubbedStartMessageChannel  chan pubsub.Message
	stubbedStartErrorChannel    chan error

	spiedAckMessageMessagID string
	stubbedAckMessageError  error

	spiedDeleteSubscriptionTopic   string
	spiedDeleteSubscriptionID      string
	stubbedDeleteSubscriptionError error
}

func (s *mockSubscriber) Start(ctx context.Context, opts ...pubsub.Option) (<-chan pubsub.Message, <-chan error) {
	// Default Options
	subscriberOptions := &pubsub.Options{
		VisibilityTimeout: 30 * time.Second,
		RetentionPeriod:   345600 * time.Second,
	}
	for _, opt := range opts {
		opt(subscriberOptions)
	}

	s.spiedStartContext = ctx
	s.spiedStartFilter = subscriberOptions.Filter
	s.spiedStartRetentionPeriod = uint64(subscriberOptions.RetentionPeriod.Seconds())
	s.spiedStartVisibilityTimeout = uint64(subscriberOptions.VisibilityTimeout.Seconds())
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

func (s *mockSubscriber) DeleteSubscription(ctx context.Context) error {
	return s.stubbedDeleteSubscriptionError
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
		Topic:             "testTopic",
		SubscriptionId:    "testSubscriptionID",
		Filter:            map[string]string{"foo": "bar"},
		RetentionPeriod:   &google_protobuf.UInt64Value{Value: uint64(60)},
		VisibilityTimeout: &google_protobuf.UInt64Value{Value: uint64(60)},
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
	{ // verify retention period passed through successfully
		expectedRetentionPeriod := subscribeRequest.GetRetentionPeriod()
		actualRetentionPeriod := mock.spiedStartRetentionPeriod
		if expectedRetentionPeriod.GetValue() != actualRetentionPeriod {
			t.Errorf("expected retention period to be %v, but was %v", expectedRetentionPeriod, actualRetentionPeriod)
		}
	}
	{ // verify visibility timeout passed through successfully
		expectedVisibilityTimeout := subscribeRequest.GetVisibilityTimeout()
		actualVisibilityTimeout := mock.spiedStartVisibilityTimeout
		if expectedVisibilityTimeout.GetValue() != actualVisibilityTimeout {
			t.Errorf("expected visibility timeout to be %v, but was %v", expectedVisibilityTimeout, actualVisibilityTimeout)
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

func TestDeleteSubscription(t *testing.T) {
	mock := &mockSubscriber{}
	server := NewPubSubServer(nil, mockSubscriberFactory(mock))
	deleteSubscriptionRequest := &DeleteSubscriptionRequest{
		Topic:          "testTopic",
		SubscriptionId: "testSubscriptionID",
	}

	{
		expectedError := errors.New("test constructor error")
		mock.stubbedConstructorError = expectedError
		_, actualError := server.DeleteSubscription(context.Background(), deleteSubscriptionRequest)
		if expectedError != actualError {
			t.Errorf("delete subscription error was incorrect:\nexpected: %v\nactual: %v", expectedError, actualError)
		}
		mock.stubbedConstructorError = nil
	}
	{
		expectedError := errors.New("test delete subscription error")
		mock.stubbedDeleteSubscriptionError = expectedError
		_, actualError := server.DeleteSubscription(context.Background(), deleteSubscriptionRequest)
		if expectedError != actualError {
			t.Errorf("delete subscription error was incorrect:\nexpected: %v\nactual: %v", expectedError, actualError)
		}
		mock.stubbedDeleteSubscriptionError = nil
	}
	{
		expectedTopic := deleteSubscriptionRequest.Topic
		expectedSubscriptionID := deleteSubscriptionRequest.SubscriptionId

		actualTopic := mock.spiedConstructorTopicName
		actualSubscriptionID := mock.spiedConstructorSubscriptionID

		if expectedTopic != actualTopic {
			t.Errorf("expected topic to be %q, but was %q", expectedTopic, actualTopic)
		}

		if expectedSubscriptionID != actualSubscriptionID {
			t.Errorf("expected subscriptionID to be %q, but was %q", expectedSubscriptionID, actualSubscriptionID)
		}
	}

}
