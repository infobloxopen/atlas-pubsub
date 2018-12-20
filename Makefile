VERSION := $(shell git describe --dirty=-dirty --always --tags)

APP_NAME := atlas-pubsub

DEFAULT_REGISTRY := infoblox
REGISTRY ?=$(DEFAULT_REGISTRY)
DEFAULT_NAMESPACE := atlas
PUBSUB_NAMESPACE ?=$(DEFAULT_NAMESPACE)

# Buildtool
BUILDER := docker run --rm -v $(CURDIR):/go/src/$(REPO) -w /go/src/$(REPO) $(BUILDTOOL_IMAGE)

IMAGE_NAME := $(REGISTRY)/$(APP_NAME):$(VERSION)
IMAGE_NAME_PUB := $(REGISTRY)/$(APP_NAME)-pub:$(VERSION)
IMAGE_NAME_SUB := $(REGISTRY)/$(APP_NAME)-sub:$(VERSION)

default: image

# formats the repo
fmt:
	@echo "Running 'go fmt ...'"
	@go fmt -x "./..."

vendor:
	dep ensure -v

vendor-update:
	@echo "Getting dependencies..."
	@dep ensure


# --- Docker commands ---
# Builds the docker image
image:
	@docker build -t $(IMAGE_NAME) -f docker/Dockerfile .

# Builds the hello example images
image-example:
	@docker build -t $(IMAGE_NAME_PUB) -f docker/Dockerfile.publisher .
	@docker build -t $(IMAGE_NAME_SUB) -f docker/Dockerfile.subscriber .

# Pushes the image to docker
push: image
	@docker push $(IMAGE_NAME)

# Pushes the hello example images to docker
push-example: image-example
	@docker push $(IMAGE_NAME_PUB)
	@docker push $(IMAGE_NAME_SUB)

# Runs the tests
test:
	echo "" > coverage.txt
	for d in `go list ./... | grep -v vendor`; do \
                t=$$(date +%s); \
                go test -v -coverprofile=cover.out -covermode=atomic $$d || exit 1; \
                echo "Coverage test $$d took $$(($$(date +%s)-t)) seconds"; \
                if [ -f cover.out ]; then \
                        cat cover.out >> coverage.txt; \
                        rm cover.out; \
                fi; \
        done

# --- Kuberenetes deployment ---
# Deploy the pubsub service in kubernetes
deploy-server:
	cat deploy/pubsub.yaml | sed 's/\$$PUBSUB_NAMESPACE/$(PUBSUB_NAMESPACE)/g' | @kubectl create -f -

# Deploys the hello example publisher and subscriber
deploy-example:
	cat deploy/pubsub-pub.yaml | sed 's/\$$PUBSUB_NAMESPACE/$(PUBSUB_NAMESPACE)/g' | @kubectl create -f -

	cat deploy/pubsub-sub.yaml | sed 's/\$$PUBSUB_NAMESPACE/$(PUBSUB_NAMESPACE)/g' | @kubectl create -f -

# Removes the kubernetes pod
remove:
	cat deploy/pubsub.yaml | sed 's/\$$PUBSUB_NAMESPACE/$(PUBSUB_NAMESPACE)/g' | @kubectl delete -f -

# Removes the example hello publisher and subscriber pods
remove-example:
	cat deploy/pubsub-pub.yaml | sed 's/\$$PUBSUB_NAMESPACE/$(PUBSUB_NAMESPACE)/g' | @kubectl delete -f -

	cat deploy/pubsub-sub.yaml | sed 's/\$$PUBSUB_NAMESPACE/$(PUBSUB_NAMESPACE)/g' | @kubectl delete -f -

temp-create-pubsub:
	cat pubsub.yaml | envsubst ' $AWS_ACCESS_KEY$AWS_ACCESS_KEY_ID$AWS_SECRET_ACCESS_KEY$AWS_SECRET_KEY'| ks create -f pubsub.yaml
