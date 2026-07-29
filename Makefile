SHELL := /bin/bash

MODULE := github.com/AnishK05/arbiter-distributed-scheduler
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
LDFLAGS := -X '$(MODULE)/internal/buildinfo.Version=$(VERSION)' -X '$(MODULE)/internal/buildinfo.Commit=$(COMMIT)'

BIN_DIR := bin
GOBIN := $(shell go env GOPATH)/bin

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

.PHONY: tools
tools: ## Install/upgrade the local proto + lint toolchain (buf, protoc-gen-go, protoc-gen-go-grpc, golangci-lint)
	go install github.com/bufbuild/buf/cmd/buf@latest
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest

.PHONY: proto
proto: ## Regenerate Go code from proto/arbiter/v1/*.proto into gen/
	cd proto && "$(GOBIN)/buf" generate

.PHONY: build
build: ## Build the scheduler, worker, and arbiterctl binaries into ./bin
	mkdir -p $(BIN_DIR)
	go build -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/scheduler ./cmd/scheduler
	go build -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/worker ./cmd/worker
	go build -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/arbiterctl ./cmd/arbiterctl

.PHONY: run-scheduler
run-scheduler: ## Run the scheduler binary from source
	go run -ldflags "$(LDFLAGS)" ./cmd/scheduler

.PHONY: run-worker
run-worker: ## Run the worker binary from source
	go run -ldflags "$(LDFLAGS)" ./cmd/worker

.PHONY: test
test: ## Run all Go tests
	go test ./... -race -count=1

.PHONY: vet
vet: ## Run go vet
	go vet ./...

.PHONY: lint
lint: ## Run golangci-lint
	"$(GOBIN)/golangci-lint" run ./...

.PHONY: fmt
fmt: ## gofmt all Go source
	gofmt -l -w .

.PHONY: tidy
tidy: ## Tidy go.mod/go.sum
	go mod tidy

.PHONY: up
up: ## Start the local dev stack (Postgres + Redis + 1 scheduler + 1 worker) in the background
	docker compose -f deploy/docker-compose.yml up -d --build

.PHONY: down
down: ## Stop the local dev stack
	docker compose -f deploy/docker-compose.yml down

.PHONY: phase4-up
phase4-up: ## Start the Phase 4 stack (dev stack + 5 equal-capacity workers)
	docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.phase4.yml up -d --build

.PHONY: phase4-down
phase4-down: ## Stop the Phase 4 stack
	docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.phase4.yml down

.PHONY: phase6-up
phase6-up: ## Start the Phase 6 HA stack (3 scheduler replicas + 1 worker)
	docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.phase6.yml up -d --build

.PHONY: phase6-down
phase6-down: ## Stop the Phase 6 HA stack
	docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.phase6.yml down

.PHONY: demo-up
demo-up: ## Start the full demo cluster (schedulers, 10 workers, pg, redis, prometheus, grafana, dashboard)
	docker compose -f deploy/docker-compose.demo.yml up --build

.PHONY: demo-down
demo-down: ## Stop the full demo cluster
	docker compose -f deploy/docker-compose.demo.yml down

.PHONY: clean
clean: ## Remove build artifacts
	rm -rf $(BIN_DIR)
