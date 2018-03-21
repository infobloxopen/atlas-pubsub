package main

// A simple example publisher app. Writes a hello message to a grpc pubsub server.
// You can run the server provided in this examples section.

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/infobloxopen/atlas-pubsub/examples/unicast"
	pubsubgrpc "github.com/infobloxopen/atlas-pubsub/grpc"
	"google.golang.org/grpc"
)

var url = flag.String("url", ":8080", "the grpc url to the pubsub server")
var topic = flag.String("topic", unicast.DefaultTopicName, "the topic to publish to")

func main() {
	flag.Parse()
	conn, err := grpc.Dial(*url, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("failed to dial to grpc server: %v", err)
	}
	p := pubsubgrpc.NewPublisher(*topic, conn)

	for i := 0; ; i++ {
		g := unicast.Messages[i%len(unicast.Messages)]

		msg := fmt.Sprintf("%s %s", g.Message, time.Now())
		md := map[string]string{"dst": g.Dst}
		log.Printf("publishing %q %v", msg, md)
		p.Publish(context.Background(), []byte(msg), md)
		time.Sleep(time.Second)
	}
}
