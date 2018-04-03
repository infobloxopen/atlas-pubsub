package main

// An example pubsub grpc server. This uses the aws message broker as its
// underlying messaging system.

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"

	"github.com/aws/aws-sdk-go/aws/session"
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
	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}
	// Checks to see if aws config credentials are valid
	log.Print("checking server for AWS permissions")
	if err := pubsubaws.VerifyPermissions(sess); err != nil {
		log.Fatalf("AWS permissions check failed: %v", err)
	}
	log.Print("server has proper AWS permissions")

	pubFactory := func(ctx context.Context, topic string) (pubsub.Publisher, error) {
		return pubsubaws.NewPublisher(sess, topic)
	}
	subFactory := func(ctx context.Context, topic, subscriptionID string) (pubsub.Subscriber, error) {
		return pubsubaws.NewSubscriber(sess, topic, subscriptionID)
	}
	return pubsubgrpc.NewPubSubServer(pubFactory, subFactory), nil
}
