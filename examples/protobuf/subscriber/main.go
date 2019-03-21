package main

// A simple example subscriber app. Listens to messages from a grpc PubSub server.
// You can run the server provided in this examples section.

import (
	"context"
	"flag"
	"github.com/golang/protobuf/proto"
	"github.com/infobloxopen/atlas-pubsub/examples/protobuf"
	"log"
	"strconv"

	pubsub "github.com/infobloxopen/atlas-pubsub"
	pubsubgrpc "github.com/infobloxopen/atlas-pubsub/grpc"
	"google.golang.org/grpc"
)

var url = flag.String("url", ":8080", "the grpc url to the pubsub server")
var topic = flag.String("topic", protobuf.DefaultTopicName, "the topic to subscribe to")
var subscriptionID = flag.String("subID", protobuf.DefaultSubscriberID, "the subscription ID for the topic")

func main() {
	flag.Parse()
	log.Printf("subscribing to server at %s with topic %q and subscriptionID %q", *url, *topic, *subscriptionID)
	conn, err := grpc.Dial(*url, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("failed to dial to grpc server: %v", err)
	}
	s := pubsubgrpc.NewSubscriber(*topic, *subscriptionID, conn)

	c, e := s.Start(context.Background(), pubsub.Filter(nil))

	for {
		select {
		case msg, isOpen := <-c:
			if !isOpen {
				log.Println("subscription channel closed")
				return
			}
			receivedPerson := &Person{}
			err = proto.Unmarshal(msg.Message(), receivedPerson)
			if err != nil {
				log.Fatal("Unmarshalling error: ", err)
			}

			log.Printf("                   (Marshalled): %q", msg.Message())
			log.Printf("Received message (Unmarshalled): %q %q %q %q", receivedPerson.GetName(), strconv.Itoa(int(receivedPerson.GetAge())), receivedPerson.EmployeeInfo.GetLocation(), receivedPerson.EmployeeInfo.GetTitle())
			go func() {
				if err := msg.Ack(); err != nil {
					log.Fatalf("failed to ack messageID %q: %v", msg.MessageID(), err)
				}
			}()
		case err := <-e:
			log.Printf("encountered error reading subscription: %v", err)
		}
	}
}
