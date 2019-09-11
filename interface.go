package pubsub

import (
	"context"
	"time"
)

// Publisher defines the basic interface for publishing messages to a message
// broker
type Publisher interface {
	// Publishes the given message to the message broker. The topic should be
	// known to the publisher prior to making this call
	Publish(context.Context, []byte, map[string]string) error

	//DeleteTopic free all resources associated with topic
	DeleteTopic(ctx context.Context) error
}

// Subscriber defines the interface for a subscriber with at-least-
// once message delivery semantics
type Subscriber interface {
	// Start creates a channel to the message broker for receiving messages
	Start(ctx context.Context, options ...Option) (<-chan Message, <-chan error)
	// AckMessage will delete the given message from its respective message queue
	AckMessage(ctx context.Context, messageID string) error
	// ExtendAckDeadline will postpone resending the given in-flight message for
	// the specified duration
	ExtendAckDeadline(ctx context.Context, messageID string, newDuration time.Duration) error

	//DeleteSubscription free all resources associated with subscription
	DeleteSubscription(ctx context.Context) error
}

// Message contains the payload for a message with at-least-once
// delivery semantics
type Message interface {
	// MessageID returns the ID that uniquely identifies this message. You can use
	// this to Ack or extend the ack deadline from the Subscriber
	MessageID() string
	// Message returns the payload from the message
	Message() []byte
	// Metadata returns the metadata associated with this message
	Metadata() map[string]string
	// ExtendAckDeadline extends the duration that a message can remain in-flight
	// before it will get added back to the message queue for redelivery. Call
	// this if processing the message will take longer than the existing time window.
	ExtendAckDeadline(time.Duration) error
	// Ack will signal to the message broker that this given message has been
	// processed and can be deleted
	Ack() error
}
