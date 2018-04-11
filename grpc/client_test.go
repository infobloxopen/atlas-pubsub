package grpc

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"google.golang.org/grpc"
)

type mockPubSubClient struct {
	spiedPublishRequest *PublishRequest
	stubbedPublishError error

	spiedSubscribeContext  context.Context
	spiedSubscribeRequest  *SubscribeRequest
	stubbedSubscribeClient *mockPubSubSubscribeClient
	stubbedSubscribeError  error

	spiedAckRequest *AckRequest
	stubbedAckError error
}

func (c *mockPubSubClient) Publish(ctx context.Context, in *PublishRequest, opts ...grpc.CallOption) (*PublishResponse, error) {
	c.spiedPublishRequest = in
	return nil, c.stubbedPublishError
}
func (c *mockPubSubClient) Subscribe(ctx context.Context, in *SubscribeRequest, opts ...grpc.CallOption) (PubSub_SubscribeClient, error) {
	c.spiedSubscribeContext = ctx
	c.spiedSubscribeRequest = in
	return c.stubbedSubscribeClient, c.stubbedSubscribeError
}
func (c *mockPubSubClient) Ack(ctx context.Context, in *AckRequest, opts ...grpc.CallOption) (*AckResponse, error) {
	c.spiedAckRequest = in
	return nil, c.stubbedAckError
}

type mockPubSubSubscribeClient struct {
	PubSub_SubscribeClient

	stubContext context.Context

	stubRecvResponse []mockRecvResponse
}

type mockRecvResponse struct {
	response *SubscribeResponse
	err      error
}

func (sc *mockPubSubSubscribeClient) Context() context.Context {
	return sc.stubContext
}

func (sc *mockPubSubSubscribeClient) Recv() (*SubscribeResponse, error) {
	if len(sc.stubRecvResponse) == 0 {
		return nil, nil
	}
	resp := sc.stubRecvResponse[0]
	sc.stubRecvResponse = sc.stubRecvResponse[1:]

	return resp.response, resp.err
}

func TestClientPublish(t *testing.T) {
	mc := mockPubSubClient{}
	cw := grpcClientWrapper{
		topic:  "testTopic",
		client: &mc,
	}
	expectedMessage := []byte{1, 2, 3, 4}
	expectedMetadata := map[string]string{"foo": "bar"}
	{ // verify publish error is forwarded
		expectedErr := errors.New("test publish error")
		mc.stubbedPublishError = expectedErr
		actualErr := cw.Publish(context.Background(), expectedMessage, expectedMetadata)
		if expectedErr != actualErr {
			t.Errorf("publish error was incorrect:\nexpected: %v\nactual: %v", expectedErr, actualErr)
		}
		mc.stubbedPublishError = nil
	}
	{ // verify topic was passed correctly
		expectedTopic := cw.topic
		actualTopic := mc.spiedPublishRequest.GetTopic()
		if expectedTopic != actualTopic {
			t.Errorf("topic was expected to be %q, but was %q", expectedTopic, actualTopic)
		}
	}
	{ // verify message was passed correctly
		actualMessage := mc.spiedPublishRequest.GetMessage()
		if !bytes.Equal(expectedMessage, actualMessage) {
			t.Errorf("expected message to be %v, but was %v", expectedMessage, actualMessage)
		}
	}
	{ // verify metadata was passed correctly
		actualMetadata := mc.spiedPublishRequest.GetMetadata()
		if !reflect.DeepEqual(expectedMetadata, actualMetadata) {
			t.Errorf("expected metadata to be %v, but was %v", expectedMetadata, actualMetadata)
		}
	}
}

func TestClientStart(t *testing.T) {
	expectedErr := errors.New("test Recv error")
	expectedSubscribeResponse := &SubscribeResponse{
		MessageId: "test messageID",
		Message:   []byte{1, 2, 3, 4},
		Metadata:  map[string]string{"foo": "bar"},
	}

	testCtx, cancelTestCtx := context.WithCancel(context.Background())
	msc := &mockPubSubSubscribeClient{
		stubContext: testCtx,
		stubRecvResponse: []mockRecvResponse{
			mockRecvResponse{nil, expectedErr},
			mockRecvResponse{expectedSubscribeResponse, nil},
		},
	}
	mc := mockPubSubClient{stubbedSubscribeClient: msc}
	cw := grpcClientWrapper{
		topic:          "testTopic",
		subscriptionID: "testSubscriptionID",
		client:         &mc,
	}

	c, e := cw.Start(testCtx)
	// TODO: figure out why this thing needs to sleep
	time.Sleep(time.Millisecond)
	{ // verify an error response is sent through the error channel
		select {
		case msg := <-c:
			t.Errorf("expected to receive an error, but got %+v", msg)
		case actualErr := <-e:
			if actualErr != expectedErr {
				t.Errorf("err channel error was incorrect:\nexpected: %v\nactual: %v", expectedErr, actualErr)
			}
		default:
			t.Errorf("no message was sent through a channel")
		}
	}
	{ // verify the success response from PubSub_SubscribeClient.Recv is sent through the message channel
		expectedMessageID := expectedSubscribeResponse.GetMessageId()
		expectedMessage := expectedSubscribeResponse.GetMessage()
		expectedMetadata := expectedSubscribeResponse.GetMetadata()
		msg := <-c
		if expectedMessageID != msg.MessageID() {
			t.Errorf("expected messageID to be %q, but was %q", "test messageID", msg.MessageID())
		}
		if !bytes.Equal(expectedMessage, msg.Message()) {
			t.Errorf("expected message to be %v, but was %v", expectedMessage, msg.Message())
		}
		if !reflect.DeepEqual(expectedMetadata, msg.Metadata()) {
			t.Errorf("expected metadata to be %v, but was %v", expectedMetadata, msg.Metadata())
		}
	}
	{ // verify cancelling the context closes the message channel
		cancelTestCtx()
		time.Sleep(100 * time.Millisecond) // I don't know a better way to do this than sleeping :(
		_, isOpen := <-c
		if !isOpen {
			t.Error("expected channel to close when context is cancelled, but wasn't")
		}
	}
}

func TestClientAck(t *testing.T) {
	mc := mockPubSubClient{}
	cw := grpcClientWrapper{
		topic:          "testTopic",
		subscriptionID: "testSubscriptionID",
		client:         &mc,
	}
	expectedMessageID := "testMessageID"
	{ // verify error is forwarded
		expectedErr := errors.New("test ack error")
		mc.stubbedAckError = expectedErr
		actualErr := cw.AckMessage(context.Background(), expectedMessageID)
		if expectedErr != actualErr {
			t.Errorf("ack error was incorrect:\nexpected: %v\nactual: %v", expectedErr, actualErr)
		}
	}
	{ // verify topic is forwarded
		expectedTopic := cw.topic
		actualTopic := mc.spiedAckRequest.GetTopic()
		if expectedTopic != actualTopic {
			t.Errorf("topic was expected to be %q, but was %q", expectedTopic, actualTopic)
		}
	}
	{ // verify subscriptionID is forwarded
		expectedSubscriptionID := cw.subscriptionID
		actualSubscriptionID := mc.spiedAckRequest.GetSubscriptionId()
		if expectedSubscriptionID != actualSubscriptionID {
			t.Errorf("expected subscriptionID to be %q, but was %q", expectedSubscriptionID, actualSubscriptionID)
		}
	}
	{ // verify messageID is forwarded
		actualMessageID := mc.spiedAckRequest.GetMessageId()
		if expectedMessageID != actualMessageID {
			t.Errorf("expected messageID to be %q, but was %q", expectedMessageID, actualMessageID)
		}
	}
}
