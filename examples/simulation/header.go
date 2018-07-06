package simulation

// DefaultTopicName is the default topic name to use for this example
const RequestTopic = "pankaj_request_topic"
const ResponseTopic = "pankaj_response_topic"

// DefaultSubscriberID is the default id to use for this example
const RequesttSubscriberID1 = "pankaj_request_subscriberid-1"
const RequestSubscriberID2 = "pankaj_request_subscriberid-2"
const RequestSubscriberID3 = "pankaj_request_subscriberid-3"
const RequestSubscriberIDAll = "pankaj_request_subscriberid-all"

// DefaultSubscriberID is the default id to use for this example
const ResponseSubscriberID1 = "pankaj_response_subscriberid-1"

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
