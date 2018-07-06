package multiple

// DefaultTopicName is the default topic name to use for this example
const DefaultTopicName = "unicast_4_demo"

// DefaultSubscriberID is the default id to use for this example
const DefaultSubscriberID1 = "unicast_4_demo_subscriberid-1"
const DefaultSubscriberID2 = "unicast_4_demo_subscriberid-2"
const DefaultSubscriberID3 = "unicast_4_demo_subscriberid-3"
const DefaultSubscriberIDAll = "unicast_4_demo_subscriberid-all"

// Greetings are different languages that the example will use. The language
// will be set in the metadata and subscribers can filter based on what language
// they want to listen for
var Messages = []message{
	message{"subscriber-1", "This is message for subscribe 1"},
	message{"subscriber-2", "This is message for subscribe 2"},
	message{"subscriber-3", "This is message for subscriber 3"},
}

type message struct {
	Dst, Message string
}
