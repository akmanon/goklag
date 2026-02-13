APP_NAME := goklag
CMD_PATH := ./cmd/app
BIN_DIR := bin

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
BUILD_DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

LDFLAGS := -s -w -X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.buildDate=$(BUILD_DATE)

.PHONY: help tidy test build build-linux build-linux-arm64 clean

help:
	@echo "Available targets:"
	@echo "  make tidy              - Download/update dependencies"
	@echo "  make test              - Run unit tests"
	@echo "  make build             - Build for host platform"
	@echo "  make build-linux       - Build production Linux amd64 binary in bin/"
	@echo "  make build-linux-arm64 - Build production Linux arm64 binary in bin/"
	@echo "  make clean             - Remove bin artifacts"

tidy:
	go mod tidy

test:
	go test ./...

build: | $(BIN_DIR)
	go build -trimpath -o $(BIN_DIR)/$(APP_NAME) $(CMD_PATH)

build-linux: | $(BIN_DIR)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/$(APP_NAME)-linux-amd64 $(CMD_PATH)

build-linux-arm64: | $(BIN_DIR)
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/$(APP_NAME)-linux-arm64 $(CMD_PATH)

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

clean:
	rm -rf $(BIN_DIR)
