# build the server binary
FROM golang:1.11.0 AS builder
LABEL stage=intermediate
WORKDIR /go/src/github.com/infobloxopen/atlas-pubsub
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o pubsub ./cmd


# run the server binary
FROM alpine:latest AS runner
WORKDIR /usr/local/bin

COPY --from=builder /go/src/github.com/infobloxopen/atlas-pubsub/pubsub .

# Allows to verify certificates
RUN apk update --no-cache && apk add --no-cache ca-certificates

ENV AWS_REGION us-east-1

# gRPC port
EXPOSE 80

ENTRYPOINT ["pubsub"]
