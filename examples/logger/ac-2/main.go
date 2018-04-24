package main

// A simple example publisher app. Writes a hello message to a grpc pubsub server.
// You can run the server provided in this examples section.

import (
	"flag"
	"log"
	"github.com/infobloxopen/atlas-pubsub/examples/logger"
	pubsubgrpc "github.com/infobloxopen/atlas-pubsub/grpc"
	"google.golang.org/grpc"
	"fmt"
	"context"
)

var url = flag.String("url", ":8080", "the grpc url to the pubsub server")
var topic = flag.String("topic", logger.DefaultTopicName, "the topic to publish to")

func main() {
	flag.Parse()
	conn, err := grpc.Dial(*url, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("failed to dial to grpc server: %v", err)
	}
	p := pubsubgrpc.NewPublisher(*topic, conn)

	// Message for Ophid - 0

	for ophid := 0 ; ophid < 2; ophid++ {

		for i := 2; i < 4; i++ {

			msg := fmt.Sprintf("Ophid-%d, log message %d", ophid, i)

			p.Publish(context.Background(), []byte(msg), nil)

		}
	}



}
