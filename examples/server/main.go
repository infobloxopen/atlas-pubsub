package main

// An example pubsub grpc server. This uses the aws message broker as its
// underlying messaging system.

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"

	pubsub "github.com/infobloxopen/atlas-pubsub"
	pubsubaws "github.com/infobloxopen/atlas-pubsub/aws"
	pubsubgrpc "github.com/infobloxopen/atlas-pubsub/grpc"
	"google.golang.org/grpc"
)

var port = flag.String("p", "8080", "the port to listen to")

func main() {
	flag.Parse()
	log.Printf("starting aws pubsub server on port %s", *port)
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()

	// Checks to see if aws config credentials are valid
	vrperr := pubsubaws.VerifyPermissions()
	if vrperr != nil {
		log.Fatalf("failed to validate aws config %s", vrperr.Error())
	}

	pubsubServer, err := newAWSPubSubServer()
	if err != nil {
		log.Fatalf("failed to create aws pubsub server: %v", err)
	}
	pubsubgrpc.RegisterPubSubServer(grpcServer, pubsubServer)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

// newAWSPubSubServer creates a new grpc PubSub server using the broker
// implementation for AWS
func newAWSPubSubServer() (pubsubgrpc.PubSubServer, error) {
	pubFactory := func(ctx context.Context, topic string) (pubsub.Publisher, error) {
		log.Printf("creating publisher for topic %q", topic)
		return pubsubaws.NewPublisher(topic)
	}
	subFactory := func(ctx context.Context, topic, subscriptionID string) (pubsub.Subscriber, error) {
		log.Printf("creating subscriber for topic %q, subscriptionID %q", topic, subscriptionID)
		return pubsubaws.NewSubscriber(topic, subscriptionID)
	}
	return pubsubgrpc.NewPubSubServer(pubFactory, subFactory, nil), nil
}
