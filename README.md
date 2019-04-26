# atlas-pubsub

[![Build Status](https://img.shields.io/travis/infobloxopen/atlas-pubsub/master.svg?label=build)](https://travis-ci.org/infobloxopen/atlas-pubsub)
[![Code Coverage](https://img.shields.io/codecov/c/github/infobloxopen/atlas-pubsub/master.svg)](https://codecov.io/github/infobloxopen/atlas-pubsub?branch=master)


## Pub/Sub implementations
Each package within this directory is a different implementation of a pub/sub message broker that complies with the interface declared in `interface.go`.

### aws
This package provides a SNS+SQS implementation that can be used by providing AWS credentials.

### grpc
This package provides a gRPC server implementation that wraps a pub/sub broker implementation.

## Using localstack

Localstack can be used as replacement for actual AWS environment in development environment.
To know more [LocalStack](https://github.com/localstack/localstack)

Once Localstack is up. Pubsub server can be started with below arguments. 

```
SNS will be running in http://localhost:4575
SQS will be running in http://localhost:4576

go run examples/server/main.go examples/server/config.go --sns.endpoint http://localhost:4575 --sqs.endpoint http://localhost:4576

```
