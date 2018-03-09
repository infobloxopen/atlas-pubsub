# Pub/Sub implementations
Each package within this directory is a different implementation of a pub/sub message broker that complies with the interface declared in `interface.go`.

### aws
This package provides a SNS+SQS implementation that can be used by providing AWS credentials.

### grpc
This package provides a gRPC server implementation that wraps a pub/sub broker implementation.
