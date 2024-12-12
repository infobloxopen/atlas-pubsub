FROM cgr.dev/chainguard/wolfi-base:latest

COPY bin/pubsub /usr/local/bin/pubsub

# Allows to verify certificates
RUN apk update --no-cache && apk add --no-cache ca-certificates

RUN apk upgrade

ENV AWS_REGION us-east-1

# gRPC port
EXPOSE 80

ENTRYPOINT ["/usr/local/bin/pubsub"]
