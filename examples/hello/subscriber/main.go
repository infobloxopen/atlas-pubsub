package main

// A simple example subscriber app. Listens to messages from a grpc PubSub server.
// You can run the server provided in this examples section.

import (
	"context"
	"flag"
	"log"

	"github.com/infobloxopen/atlas-pubsub/examples/hello"
	pubsubgrpc "github.com/infobloxopen/atlas-pubsub/grpc"
	"google.golang.org/grpc"
)

var url = flag.String("url", ":8080", "the grpc url to the pubsub server")
var topic = flag.String("topic", hello.DefaultTopicName, "the topic to subscribe to")
var subscriptionID = flag.String("subID", hello.DefaultSubscriberID, "the subscription ID for the topic")
var languageFilter = flag.String("lang", "", "if present, will only show messages with metadata tagged for the given language")

func main() {
	log.Println("Inside Subscriber Example")
	flag.Parse()
	conn, err := grpc.Dial(*url, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("failed to dial to grpc server: %v", err)
	}
	// s := pubsubgrpc.NewSubscriber(*topic, *subscriptionID, conn)

<<<<<<< HEAD
<<<<<<< HEAD
	md := make(map[string]string)
	if languageFilter != nil && *languageFilter != "" {
		md["language"] = *languageFilter
		log.Printf("Only receiving messages written in %q", *languageFilter)
	}
	c, e := s.Start(context.Background(), md)
=======
	s := pubsubgrpc.NewSubscriber(*topic, "testroman", conn)
>>>>>>> Changed kubernetes yaml files to work with correct url and update glide to dep.
=======
	s := pubsubgrpc.NewSubscriber(*topic, "testroman", conn)
>>>>>>> 8e70c05260787b3699751f50ffe3d01757a6acc1

	c, e := s.Start(context.Background())
	for {
		log.Println("Inside for loop")
		select {
		case msg, isOpen := <-c:
			log.Println("Inside if msg received")
			if !isOpen {
				// log.Fatalln("subscription channel closed")

				log.Println("Not open")
				return
			}
			greeting := string(msg.Message())
			log.Printf("received message: %q", greeting)

			log.Println("Inside received message " + greeting)
			if err := msg.Ack(); err != nil {
				log.Fatalf("failed to ack messageID %q: %v", msg.MessageID(), err)
			}
		case err := <-e:
			log.Println("Inside error")
			log.Fatalf("encountered error reading subscription: %v", err)
		}
	}
}
