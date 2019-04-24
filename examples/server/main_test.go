package main

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"testing"
	"time"

	pubsubgrpc "github.com/infobloxopen/atlas-pubsub/grpc"
)

type fakePubsub struct {
}

func (f *fakePubsub) Publish(context.Context, *pubsubgrpc.PublishRequest) (*pubsubgrpc.PublishResponse, error) {
	return nil, nil
}

func (f *fakePubsub) Subscribe(req *pubsubgrpc.SubscribeRequest, s pubsubgrpc.PubSub_SubscribeServer) error {
	return nil
}

func (f *fakePubsub) Ack(ctx context.Context, ack *pubsubgrpc.AckRequest) (*pubsubgrpc.AckResponse, error) {
	return nil, nil
}

func getFakePubSubServer(logger *logrus.Logger) (pubsubgrpc.PubSubServer, error) {
	return &fakePubsub{}, nil
}

func TestGetPubSubServer(t *testing.T) {
	t.Run("test main", func(t *testing.T) {

		OriginalPubsub := GetPubSubServer
		GetPubSubServer = getFakePubSubServer
		defer func() { GetPubSubServer = OriginalPubsub }()
		go main()
		time.Sleep(1 * time.Second)
		_, err := grpc.Dial(fmt.Sprintf("0000:%s", viper.Get("server.port")), grpc.WithInsecure())
		if err != nil {
			t.Fatalf("failed to dial to grpc server: %v", err)
		}
	})
}
