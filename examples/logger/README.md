# Multiple Listener pubsub implementation
This example demonstrates how to use multiple listeners with Pubsub

1. Run the gRPC server provided in this example:
```
cd ~/go/src/github.com/infobloxopen/atlas-pubsub/examples/server/
go run main.go
```
2. In separate terminals, run the subscriber samples provided in this example:
```
cd ~/go/src/github.com/infobloxopen/atlas-pubsub/examples/multiple/subscriber-1
go run main.go

cd ~/go/src/github.com/infobloxopen/atlas-pubsub/examples/multiple/subscriber-2
go run main.go

cd ~/go/src/github.com/infobloxopen/atlas-pubsub/examples/multiple/subscriber-3
go run main.go

cd ~/go/src/github.com/infobloxopen/atlas-pubsub/examples/multiple/subscriber-all
go run main.go
```

3. In another separate terminal, run the publisher provided in this example:
```
cd ~/go/src/github.com/infobloxopen/atlas-pubsub/examples/multiple/publisher
go run main.go
```
The publisher will send a message every second,subscriber-1, subscribe-2 and subscriber-3 will get their respective messages. subscriber-all gets all the messages.
