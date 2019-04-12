# AWS Broker implementation

This uses SNS and SQS to produce a pub/sub broker implementation. To use this broker, you must provide AWS credentials. All queue permissions management is handled by the credentials, and all AWS-related errors are forwarded to the consumer.

## Topic/Queue naming convention
All SNS topics are prefixed with `pubsub__`.

All SQS queues are prefixed with `pubsub__<topic>_`

The SNS topics and SQS queues are created on-demand whenever the topic is first published or subscribed to.

Two subscribers to the same topic with the same subscriptionID will share messages from the same message queue.

## Required AWS Permissions
*SNS*
* CreateTopic
* Subscribe
* Unsubscribe
* Publish
* GetSubscriptionAttributes
* SetSubscriptionAttributes
 
*SQS*
* GetQueueUrl
* CreateQueue
* GetQueueAttributes
* SetQueueAttributes
* ReceiveMessage
* DeleteMessage
