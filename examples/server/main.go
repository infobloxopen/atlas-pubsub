package main

// An example pubsub grpc server. This uses the aws message broker as its
// underlying messaging system.

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	pubsub "github.com/infobloxopen/atlas-pubsub"
	pubsubaws "github.com/infobloxopen/atlas-pubsub/aws"
	pubsubgrpc "github.com/infobloxopen/atlas-pubsub/grpc"
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

	cfg := aws.NewConfig().WithCredentials(credentials.NewEnvCredentials())

	// Checks to see if aws config credentials are valid
	vrperr := pubsubaws.VerifyPermissions(cfg)
	if vrperr != nil {
		log.Fatalf("failed to validate aws config %s", vrperr.Error())
	}

	pubsubServer, err := newAWSPubSubServer(cfg)
	if err != nil {
		log.Fatalf("failed to create aws pubsub server: %v", err)
	}
	pubsubgrpc.RegisterPubSubServer(grpcServer, pubsubServer)
	grpcServer.Serve(lis)
}

// newAWSPubSubServer creates a new grpc PubSub server using the broker
// implementation for AWS
func newAWSPubSubServer(config *aws.Config) (pubsubgrpc.PubSubServer, error) {
	pubFactory := func(ctx context.Context, topic string) (pubsub.Publisher, error) {
		return pubsubaws.NewPublisher(config, topic)
	}
	subFactory := func(ctx context.Context, topic, subscriptionID string) (pubsub.Subscriber, error) {
		return pubsubaws.NewSubscriber(config, topic, subscriptionID)
	}
	return pubsubgrpc.NewPubSubServer(pubFactory, subFactory, nil), nil
}
