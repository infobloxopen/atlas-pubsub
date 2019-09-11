package protobuf

// DefaultTopicName is the default topic name to use for this example
const DefaultTopicName = "example_protobuf"

// DefaultSubscriberID is the default id to use for this example
const DefaultSubscriberID = "example_protobuf_subscriberid"

var People = []Person{
	Person{"Phillip J. Fry", "New New York", "Delivery Boy", 24},
	Person{"Bart Simpson", "Springfield", "Student", 10},
	Person{"Davos Seaworth", "Dragonstone", "Onion Knight", 48},
}

type Person struct {
	Name, Location, Title string
	Age                   int32
}
