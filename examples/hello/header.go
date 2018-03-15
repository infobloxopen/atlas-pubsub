package hello

// DefaultTopicName is the default topic name to use for this example
const DefaultTopicName = "example_hello"

// DefaultSubscriberID is the default id to use for this example
const DefaultSubscriberID = "example_hello_subscriberid"

// Greetings are different languages that the example will use. The language
// will be set in the metadata and subscribers can filter based on what language
// they want to listen for
var Greetings = []greeting{
	greeting{"english", "Hello!"},
	greeting{"russian", "Здравствуйте!"},
	greeting{"spanish", "¡Hola!"},
}

type greeting struct {
	Language, Message string
}
