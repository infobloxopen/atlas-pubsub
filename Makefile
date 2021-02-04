VERSION := $(shell git describe --dirty=-dirty --always --long --tags)

APP_NAME := atlas.pubsub

# Absolute github repository name.
REPO := github.com/infobloxopen/atlas-pubsub

SRC = examples/server

# Source directory path relative to $GOPATH/src.
SRCDIR = $(REPO)/$(SRC)

# Output binary name.
BIN = pubsub

# Build directory absolute path.
BINDIR = $(CURDIR)/bin

# Utility docker image to generate Go files from .proto definition.
# https://github.com/infobloxopen/buildtool
BUILDTOOL_IMAGE := infoblox/buildtool
DEFAULT_REGISTRY := infobloxcto
REGISTRY ?=$(DEFAULT_REGISTRY)
DEFAULT_NAMESPACE := atlas
PUBSUB_NAMESPACE ?=$(DEFAULT_NAMESPACE)

# Buildtool
BUILDER := docker run --rm -v $(CURDIR):/go/src/$(REPO) -w /go/src/$(REPO) $(BUILDTOOL_IMAGE)

IMAGE_NAME := $(REGISTRY)/$(APP_NAME):$(VERSION)
IMAGE_NAME_PUB := $(REGISTRY)/$(APP_NAME)-pub:$(VERSION)
IMAGE_NAME_SUB := $(REGISTRY)/$(APP_NAME)-sub:$(VERSION)

PROJECT_ROOT := github.com/infobloxopen/atlas-pubsub
# configuration for the protobuf gentool
SRCROOT_ON_HOST      := $(shell dirname $(abspath $(lastword $(MAKEFILE_LIST))))
SRCROOT_IN_CONTAINER := /home/go/src/$(PROJECT_ROOT)
DOCKERPATH           := /home/go/src
DOCKER_RUNNER        := docker run --rm
DOCKER_RUNNER        += -v $(SRCROOT_ON_HOST):$(SRCROOT_IN_CONTAINER)
DOCKER_GENERATOR     := infoblox/atlas-gentool:v19
GENERATOR            := $(DOCKER_RUNNER) $(DOCKER_GENERATOR)

default: build

build: fmt bin
	@$(BUILDER) go build $(GO_BUILD_FLAGS) -o "bin/$(BIN)" "$(SRCDIR)"

# Builds example hello publisher and subscriber
build-example: fmt bin build 
	@$(BUILDER) go build $(GO_BUILD_FLAGS) -o "bin/$(BIN)-pub" "$(REPO)/examples/hello/publisher"
	@$(BUILDER) go build $(GO_BUILD_FLAGS) -o "bin/$(BIN)-sub" "$(REPO)/examples/hello/subscriber"



# formats the repo
fmt:
	$(BUILDER) go fmt -x "$(REPO)/..."

deps:
	@echo "Getting dependencies..."
	@dep ensure

bin:
	mkdir -p "$(BINDIR)"

clean:
	@rm -rf "$(BINDIR)"
	@rm -rf .glide

show-image-name:
	@echo $(IMAGE_NAME)

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

# Deployes the hello example publisher and subscriber 
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

vendor:
	glide update -v

temp-create-pubsub:
	cat pubsub.yaml | envsubst ' $AWS_ACCESS_KEY$AWS_ACCESS_KEY_ID$AWS_SECRET_ACCESS_KEY$AWS_SECRET_KEY'| ks create -f pubsub.yaml

protobuf:
	$(GENERATOR) \
		-I/home/go/src/github.com/infobloxopen/atlas-pubsub \
		--go_out="plugins=grpc:$(SRCROOT_IN_CONTAINER)" \
	    grpc/server.proto
