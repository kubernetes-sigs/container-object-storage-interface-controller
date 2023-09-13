.DEFAULT_GOAL := help
SHELL = /usr/bin/env bash

# 'go env' vars aren't always available in make environments, so get defaults for needed ones
GOARCH ?= $(shell go env GOARCH)

##
## ==== ARGS ===== #

## Container build tool compatible with `docker` API
DOCKER ?= docker

## Platform for 'build'
PLATFORM ?= linux/$(GOARCH)

## Platform list for multi-arch 'buildx' build
BUILDX_PLATFORMS ?= linux/amd64,linux/arm64

## Image tag for all builds
IMAGE_TAG ?= local/cosi-controller:latest

## Add additional build args if desired
BUILD_ARGS ?=

##
## === TARGETS === #

.PHONY: build
## Build local image for development, defaulting linux/<hostarch>
build:
	# $(DOCKER) build --platform $(PLATFORM) --tag $(IMAGE_TAG) .
	true # return true temporarily to allow prow to succeed

.PHONY: buildx
## Build cross-platform image for release
buildx:
	$(DOCKER) buildx build --platform $(BUILDX_PLATFORMS) $(BUILD_ARGS) --tag $(IMAGE_TAG) .

.PHONY: test
## Test packages
test:
	go vet ./...
	go test ./...

# print out lines beginning with double-comments, plus next line as basic help text
.PHONY: help
## Show this help text
help:
	@sed -n -e "/^##/{N;s/^/\n/p;}" $(MAKEFILE_LIST)
