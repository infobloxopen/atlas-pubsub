VERSION := $(shell git describe --dirty=-dirty --always)

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

# Buildtool
BUILDER := docker run --rm -v $(CURDIR):/go/src/$(REPO) -w /go/src/$(REPO) $(BUILDTOOL_IMAGE)

IMAGE_NAME := $(REGISTRY)/$(APP_NAME):$(VERSION)
IMAGE_NAME_PUB := $(REGISTRY)/$(APP_NAME)-pub:$(VERSION)
IMAGE_NAME_SUB := $(REGISTRY)/$(APP_NAME)-sub:$(VERSION)

default: build

build: fmt bin
	@$(BUILDER) go build $(GO_BUILD_FLAGS) -o "bin/$(BIN)" "$(SRCDIR)"

# Builds example hello publisher and subscriber
build-example: fmt bin build 
	@$(BUILDER) go build $(GO_BUILD_FLAGS) -o "bin/$(BIN)-pub" "$(REPO)/examples/hello/publisher"
	@$(BUILDER) go build $(GO_BUILD_FLAGS) -o "bin/$(BIN)-sub" "$(REPO)/examples/hello/subscriber"

# formats the repo
fmt:
	@echo "Running 'go fmt ...'"
	@go fmt -x "$(REPO)/..."

deps:
	@echo "Getting dependencies..."
	@dep ensure

bin:
	mkdir -p "$(BINDIR)"

clean:
	@rm -rf "$(BINDIR)"
	@rm -rf .glide

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
	@kubectl create -f deploy/pubsub.yaml

# Deployes the hello example publisher and subscriber 
deploy-example:
	@kubectl create -f deploy/pubsub-pub.yaml

	@kubectl create -f deploy/pubsub-sub.yaml

# Removes the kubernetes pod
remove:
	@kubectl delete -f deploy/pubsub.yaml

# Removes the example hello publisher and subscriber pods
remove-example:
	@kubectl delete -f deploy/pubsub-pub.yaml

	@kubectl delete -f deploy/pubsub-sub.yaml

vendor:
	glide update -v

temp-create-pubsub:
	cat pubsub.yaml | envsubst ' $AWS_ACCESS_KEY$AWS_ACCESS_KEY_ID$AWS_SECRET_ACCESS_KEY$AWS_SECRET_KEY'| ks create -f pubsub.yaml
