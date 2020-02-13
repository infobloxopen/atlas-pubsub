# atlas-pubsub

[![Build Status](https://img.shields.io/travis/infobloxopen/atlas-pubsub/master.svg?label=build)](https://travis-ci.org/infobloxopen/atlas-pubsub)
[![Code Coverage](https://img.shields.io/codecov/c/github/infobloxopen/atlas-pubsub/master.svg)](https://codecov.io/github/infobloxopen/atlas-pubsub?branch=master)
[![GoDoc](https://godoc.org/github.com/infobloxopen/atlas-pubsub?status.svg)](https://godoc.org/github.com/infobloxopen/atlas-pubsub)


## Pub/Sub implementations
Each package within this directory is a different implementation of a pub/sub message broker that complies with the interface declared in `interface.go`.

### aws
This package provides a SNS+SQS implementation that can be used by providing AWS credentials.

### grpc
This package provides a gRPC server implementation that wraps a pub/sub broker implementation.
