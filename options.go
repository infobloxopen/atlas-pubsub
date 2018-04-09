package pubsub

import "time"

// Options defines functional options available to a Subscriber
type Options struct {
	VisibilityTimeout time.Duration
	RetentionPeriod   time.Duration
	Filter            map[string]string

	// Other options go here:

}

// Option function
type Option func(*Options)

// VisibilityTimeout will postpone resending the message in-flight for the specified duration,
// period of time during which subsriber prevents other consumers from receiving and processing the message
func VisibilityTimeout(timeDuration time.Duration) Option {
	return func(args *Options) {
		args.VisibilityTimeout = timeDuration
	}
}

// RetentionPeriod sets the default retention period for a subscriber, in other words
// the length of time a message can stay in the queue
func RetentionPeriod(timeDuration time.Duration) Option {
	return func(args *Options) {
		args.RetentionPeriod = timeDuration
	}
}

// Filter will only show messages based on the filter
func Filter(filter map[string]string) Option {
	return func(args *Options) {
		args.Filter = filter
	}
}
