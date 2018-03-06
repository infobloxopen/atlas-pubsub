package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"

	"github.com/infobloxopen/atlas-pubsub/pubsub"
	pubsubaws "github.com/infobloxopen/atlas-pubsub/pubsub/aws"
	pubsubgrpc "github.com/infobloxopen/atlas-pubsub/pubsub/grpc"
	"google.golang.org/grpc"
)

var port = flag.String("p", "8080", "the port to listen to")

func main() {
	log.Println("starting aws pubsub server")
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()

	pubsubServer, err := newAWSPubSubServer()
	if err != nil {
		log.Fatalf("failed to create aws pubsub server: %v", err)
	}
	pubsubgrpc.RegisterPubSubServer(grpcServer, pubsubServer)
	grpcServer.Serve(lis)
}

func newAWSPubSubServer() (pubsubgrpc.PubSubServer, error) {
	pubFactory := func(ctx context.Context, topic string) (pubsub.Publisher, error) {
		return pubsubaws.NewPublisher(nil, topic)
	}
	subFactory := func(ctx context.Context, topic, subscriptionID string) (pubsub.AtLeastOnceSubscriber, error) {
		return pubsubaws.NewAtLeastOnceSubscriber(nil, topic, subscriptionID)
	}
	return pubsubgrpc.NewPubSubServer(pubFactory, subFactory), nil
}
