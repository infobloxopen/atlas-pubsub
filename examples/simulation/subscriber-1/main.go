package main

// A simple example subscriber app. Listens to messages from a grpc PubSub server.
// You can run the server provided in this examples section.

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/infobloxopen/atlas-pubsub/examples/simulation"
	pubsubgrpc "github.com/infobloxopen/atlas-pubsub/grpc"
	"google.golang.org/grpc"
	"fmt"
)

// Subscription Message
var url = flag.String("url", ":8080", "the grpc url to the pubsub server")
var topic = flag.String("topic", simulation.RequestTopic, "the topic to subscribe to")
var subscriptionID = flag.String("subID", simulation.RequesttSubscriberID1, "the subscription ID for the topic")
var dstFilter = flag.String("dst", "subscriber-1", "Listen for this destination")


// Publishing

var respTopic = flag.String("responseTopic", simulation.ResponseTopic, "the topic to publish to")


func subscriber(conn *grpc.ClientConn) {
	fmt.Printf("subscriber-1: In")

	s := pubsubgrpc.NewSubscriber(*topic, *subscriptionID, conn)

	p := pubsubgrpc.NewPublisher(*respTopic, conn)


	md := make(map[string]string)
	if dstFilter != nil && *dstFilter != "" {
		md["dst"] = *dstFilter
		log.Printf("Only receiving messages written in %q", *dstFilter)
	}
	c, e := s.Start(context.Background(), md)

	for {
		select {
		case msg, isOpen := <-c:
			if !isOpen {
				log.Println("subscription channel closed")
				return
			}
			greeting := string(msg.Message())
			log.Printf("received message: %q", greeting)
			if err := msg.Ack(); err != nil {
				log.Fatalf("failed to ack messageID %q: %v", msg.MessageID(), err)
			}
			greeting = "Echo Back -" + greeting
			fmt.Printf("Echoing Back message %s", greeting)
			p.Publish(context.Background(), []byte(greeting), md)
		case err := <-e:
			log.Printf("encountered error reading subscription: %v", err)
		}
	}

	fmt.Printf("subscriber-1: Out\n")

}
func main() {
	fmt.Printf("Request Topic is -------> %s\n", simulation.RequestTopic)

	flag.Parse()
	conn, err := grpc.Dial(*url, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("failed to dial to grpc server: %v", err)
	}
	go subscriber(conn)

	time.Sleep(60 * time.Second)

}
