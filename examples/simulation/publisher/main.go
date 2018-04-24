package main

// A simple example publisher app. Writes a hello message to a grpc pubsub server.
// You can run the server provided in this examples section.

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	pubsubgrpc "github.com/infobloxopen/atlas-pubsub/grpc"
	"google.golang.org/grpc"
	"github.com/infobloxopen/atlas-pubsub/examples/simulation"
)

// Constants
var url = flag.String("url", ":8080", "the grpc url to the pubsub server")



func subscriber(conn *grpc.ClientConn) {

	fmt.Printf("subscriber: In\n")
	responseTopic := flag.String("respTopic", simulation.ResponseTopic, "the topic to subscribe to")
	responseSubscriptionID := flag.String("responseSubID", simulation.ResponseSubscriberID1, "the subscription ID for the topic")
	dstFilter := flag.String("responseDestination", "subscriber-1", "Listen for this destination")

	fmt.Printf("Subscriber: Response Topic is -------> %s\n", simulation.ResponseTopic)
	s := pubsubgrpc.NewSubscriber(*responseTopic, *responseSubscriptionID, conn)

	fmt.Printf("Subscribed.....")
	md := make(map[string]string)
	if dstFilter != nil && *dstFilter != "" {
		md["dst"] = *dstFilter
		log.Printf("Only receiving messages written in %q", *dstFilter)
	}
	c, e := s.Start(context.Background(), md)
	fmt.Printf("Here ...................")
	for {
		select {
		case msg, isOpen := <-c:
			if !isOpen {
				log.Println("subscription channel closed")
				return
			}
			greeting := string(msg.Message())
			log.Printf("Subscriber --- received response message: %q", greeting)
			if err := msg.Ack(); err != nil {
				log.Fatalf("failed to ack messageID %q: %v", msg.MessageID(), err)
			}
		case err := <-e:
			log.Printf("encountered error reading subscription: %v", err)
		}
	}

	fmt.Printf("subscriber: Out")
}


func publisher(conn *grpc.ClientConn) {


	reqTopic := flag.String("reqTopic", simulation.RequestTopic, "the topic to publish to")

	fmt.Printf("publisher: In")
	p := pubsubgrpc.NewPublisher(*reqTopic, conn)

	for i := 0; ; i++ {
		g := simulation.Messages[i%len(simulation.Messages)]

		msg := fmt.Sprintf("%s %s", g.Message, time.Now())
		md := map[string]string{"dst": g.Dst}
		//log.Printf("publishing %q %v", msg, md)
		p.Publish(context.Background(), []byte(msg), md)
		time.Sleep(time.Second)
	}
	fmt.Printf("publisher: In")


}


func main() {
	flag.Parse()
	fmt.Printf("Request Topic is %s\n", simulation.RequestTopic)

	conn, err := grpc.Dial(*url, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("failed to dial to grpc server: %v", err)
	}

	go publisher(conn)
	go subscriber(conn)

	time.Sleep(60 * time.Second)
}
