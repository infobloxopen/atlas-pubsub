# Simple Hello World Pub/Sub implementation
 In order for this demo to work, you must have your AWS credentials present in the form that the [AWS SDK for Go](https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/sessions.html) requires.

1. Create the person.pb.go files for both publisher and subscriber:
```
cd ~/go/src/github.com/infobloxopen/atlas-pubsub/examples/protobuf
protoc --go_out=publisher/ person.proto
protoc --go_out=subscriber/ person.proto 
```
2. Run the gRPC server provided in this example:
```
cd ~/go/src/github.com/infobloxopen/atlas-pubsub/examples/server/
go run main.go
```
3. In a separate terminal, run the subscriber provided in this example:
```
cd ~/go/src/github.com/infobloxopen/atlas-pubsub/examples/protobuf/subscriber
go run main.go person.pb.go
```
4. In another separate terminal, run the publisher provided in this example:
```
cd ~/go/src/github.com/infobloxopen/atlas-pubsub/examples/protobuf/publisher
go run main.go person.pb.go
```
The publisher will send a message every second. You can experiment by spinning up more publishers or subscribers, both with the same `subscriptionID` and with different ones.
