FROM gcr.io/distroless/static-debian12:nonroot-amd64

COPY bin/pubsub /usr/local/bin/pubsub

ENV AWS_REGION=us-east-1

# gRPC port
EXPOSE 80

# Run as non-root user
USER nonroot

ENTRYPOINT ["/usr/local/bin/pubsub"]
