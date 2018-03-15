package aws

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

// TestNewPublisher verifies that aws creates a topic for the given topic name,
// while handling invalid characters and string lengths appropriately, and
// forwarding any errors encountered
func TestNewPublisher(t *testing.T) {
	{ // basic input validation tests
		topicInputs := []string{
			"",
			strings.Repeat("a", topicNameMaxLength+1),
		}
		for _, input := range topicInputs {
			if _, err := newPublisher(&mockSNS{}, input); err == nil {
				t.Errorf("expected passing an invalid topic name to return an error, but didn't. \n topic input: \"%s\"", input)
			}
		}
	}

	{ // verify inputted topic names map properly to aws-appropriate names
		spy := mockSNS{}
		newPublisher(&spy, "foo")
		if spy.spiedCreateTopicInput == nil {
			t.Error("sns.CreateTopic was not called")
		} else {
			actual := *spy.spiedCreateTopicInput.Name
			expected := "pubsub__foo"
			if actual != expected {
				t.Errorf("SNS topic name was incorrect, expected:\"%s\"actual:\"%s\"", expected, actual)
			}
		}
	}

	{ // verify aws errors are passed through
		expectedErrorMessage := "some AWS error"
		mock := mockSNS{stubbedCreateTopicError: errors.New(expectedErrorMessage)}
		_, err := newPublisher(&mock, "foo")
		if err == nil {
			t.Error("Expected newPublisher to return an error, but didn't")
		} else {
			actual := err.Error()
			if actual != expectedErrorMessage {
				t.Errorf("Error message was incorrect, expected: \"%s\", actual: \"%s\"", expectedErrorMessage, actual)
			}
		}
	}
}

func TestPublish(t *testing.T) {
	msg := []byte{1, 2, 3, 4}
	metadata := map[string]string{"foo": "bar"}
	spy := mockSNS{stubbedPublishError: errors.New("test publish error")}

	p := publisher{sns: &spy, topicArn: "foo"}
	p.Publish(context.Background(), msg, metadata)
	spiedInput := spy.spiedPublishInput

	{ // verify the topic arn is what the publisher is set to
		expected := p.topicArn
		actual := *spiedInput.TopicArn
		if expected != actual {
			t.Errorf("AWS topic arn was incorrect, expected: \"%s\", actual: \"%s\"", expected, actual)
		}
	}
	{ // verify the message looks the way it's supposed to
		expected := *encodeToSNSMessage(msg)
		actual := *spiedInput.Message
		if expected != actual {
			t.Errorf("Base64-encoded message was not in expected format: \nexpected: \"%s\"\nactual:  \"%s\"", expected, actual)
		}
	}
	{ // verify the metadata looks the way it's supposed to
		expected := encodeMessageAttributes(metadata)
		actual := spiedInput.MessageAttributes
		if !reflect.DeepEqual(expected, actual) {
			t.Errorf("expected messageAttributes to be %v, but was %v", expected, actual)
		}
	}
	{ // verify publish errors are forwarded
		actual := p.Publish(context.Background(), msg, nil)
		expected := spy.stubbedPublishError
		if expected != actual {
			t.Errorf("expected publish error %v, but got %v", expected, actual)
		}
	}
}
