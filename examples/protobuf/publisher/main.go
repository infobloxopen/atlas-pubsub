package main

// A simple example publisher app. Writes a make protobuf message to a grpc pubsub server.
// You can run the server provided in this examples section.

import (
	"context"
	"flag"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/infobloxopen/atlas-pubsub/examples/protobuf"
	"log"
	"time"

	pubsubgrpc "github.com/infobloxopen/atlas-pubsub/grpc"
	"google.golang.org/grpc"
)

var url = flag.String("url", ":8080", "the grpc url to the pubsub server")
var topic = flag.String("topic", protobuf.DefaultTopicName, "the topic to publish to")

func main() {
	flag.Parse()
	log.Printf("publishing protobuf messages to %s with topic %q", *url, *topic)
	conn, err := grpc.Dial(*url, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("failed to dial to grpc server: %v", err)
	}
	p := pubsubgrpc.NewPublisher(*topic, conn)

	for i := 0; ; i++ {
		g := protobuf.People[i%len(protobuf.People)]
		person := &Person{
			Name: g.Name,
			Age: g.Age,
			EmployeeInfo: &EmployeeInfo{
				Location: g.Location,
				Title: g.Title,
			},
		}

		data, err := proto.Marshal(person)
		if err != nil {
			log.Fatal("Marshalling error: ", err)
		}

		msg := fmt.Sprintf("%s %v %s %s %s", person.Name, person.Age, person.EmployeeInfo.Location, person.EmployeeInfo.Title, time.Now())
		md := map[string]string{"Location": person.EmployeeInfo.Location}
		log.Printf("publishing %q %v", msg, md)
		if err := p.Publish(context.Background(), data, md); err != nil {
			log.Printf("error publishing: %v", err)
			return
		}
		time.Sleep(time.Second * 3)
	}
}
