package main

// A simple example publisher app. Writes a hello message to a grpc pubsub server.
// You can run the server provided in this examples section.

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/infobloxopen/atlas-pubsub/examples/hello"
	pubsubgrpc "github.com/infobloxopen/atlas-pubsub/grpc"
	"google.golang.org/grpc"
)

var topic = flag.String("topic", hello.DefaultTopicName, "the topic to publish to")

func main() {
	flag.Parse()
	conn, err := grpc.Dial(":8080", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("failed to dial to grpc server: %v", err)
	}
	for i := 0; ; i++ {
		p := pubsubgrpc.NewPublisher(*topic, conn)
		g := hello.Greetings[i%len(hello.Greetings)]

		msg := fmt.Sprintf("%s %s", g.Message, time.Now())
		md := map[string]string{"language": g.Language}
		log.Printf("printing %q %v", msg, md)
		p.Publish(context.Background(), []byte(msg), md)
		time.Sleep(time.Second)
	}
}
