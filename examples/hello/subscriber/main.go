package main

// A simple example subscriber app. Listens to messages from a grpc PubSub server.
// You can run the server provided in this examples section.

import (
	"context"
	"flag"
	"log"

	"github.com/infobloxopen/atlas-pubsub/examples/hello"
	pubsubgrpc "github.com/infobloxopen/atlas-pubsub/pubsub/grpc"
	"google.golang.org/grpc"
)

var topic = flag.String("topic", hello.DefaultTopicName, "the topic to subscribe to")
var subscriptionID = flag.String("subID", hello.DefaultSubscriberID, "the subscription ID for the topic")

func main() {
	flag.Parse()
	conn, err := grpc.Dial(":8080", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("failed to dial to grpc server: %v", err)
	}
	s := pubsubgrpc.NewSubscriber(*topic, *subscriptionID, conn)

	c, e := s.Start(context.Background())

	for {
		select {
		case msg, isOpen := <-c:
			if !isOpen {
				log.Fatalln("subscription channel closed")
				return
			}
			greeting := string(msg.Message())
			log.Printf("received message: %q", greeting)
			if err := msg.Ack(); err != nil {
				log.Fatalf("failed to ack messageID %q: %v", msg.MessageID(), err)
			}
		case err := <-e:
			log.Fatalf("encountered error reading subscription: %v", err)
		}
	}
}
