# Build stage
FROM golang:1.25-alpine3.21 AS builder
LABEL stage=pubsub-intermediate
WORKDIR /go/src/github.com/infobloxopen/atlas-pubsub

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o bin/pubsub ./examples/server

# Final runtime image using distroless
FROM gcr.io/distroless/static-debian12:nonroot-amd64 AS runner
WORKDIR /app
COPY --from=builder /go/src/github.com/infobloxopen/atlas-pubsub/bin/pubsub /app/pubsub

ENV AWS_REGION=us-east-1

# gRPC port
EXPOSE 80

USER nonroot

ENTRYPOINT ["/app/pubsub"]
