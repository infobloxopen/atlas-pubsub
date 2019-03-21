# Simple Hello World Subscriber implementation
 In order for this demo to work, you must have your AWS credentials present in the form that the [AWS SDK for Go](https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/sessions.html) requires.

The subscriber allows you to add options based on your needs. 
The current options include: 
- Filter Policy: will only show messages with with the given policy.
- Message Retention Period: The length of time, in seconds, for which the subscriber retains a message. 
- Visibility Timeout:  The length of time, in seconds, which the subscriber prevents other consumers from receiving and processing the message. 

1. Running a basic subscriber with no functional options: 
```
c, e := s.Start(context.Background())
```
2. Running subscriber with functional options:
```
c, e := s.Start(context.Background(), opts ...pubsub.Option)
```
  * Example with one option:
```
md := make(map[string]string)
if locationFilter != nil && *locationFilter != "" {
	md["Location"] = *locationFilter
	log.Printf("Only receiving messages for employees in %q", *locationFilter)
}
c, e := s.Start(context.Background(), pubsub.Filter(md))
```
  * Example with multiple options: 
```
md := make(map[string]string)
if locationFilter != nil && *locationFilter != "" {
	md["Location"] = *locationFilter
	log.Printf("Only receiving messages for employees in %q", *locationFilter)
}

retentionPeriod := 100 * time.Minute  
visibilityTimeout := 200 * time.Second 

c, e := s.Start(context.Background(), pubsub.Filter(md), pubsub.RetentionPeriod(retentionPeriod), pubsub.VisibilityTimeout(visibilityTimeout))
```