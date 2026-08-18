SHELL := /bin/sh

BIN        ?= bin/outbox
PKG        ?= ./...
COVERAGE   ?= coverage.out
VERSION    ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT     ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
BUILD_DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

MODULE  := github.com/efureev/go-outbox
LDFLAGS := -s -w \
	-X 'main.version=$(VERSION)' \
	-X 'main.commit=$(COMMIT)' \
	-X 'main.date=$(BUILD_DATE)'

.DEFAULT_GOAL := help
.PHONY: help build run test test-integration test-all cover lint fmt tidy up down logs psql image clean

help: ## List the available targets
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Build the binary
	go build -trimpath -ldflags "$(LDFLAGS)" -o $(BIN) ./cmd/outbox

run: ## Run the dispatcher against the compose stack
	go run ./cmd/outbox run

test: ## Run the unit tests
	go test -race $(PKG)

test-integration: ## Run the integration tests (needs `make up`)
	go test -race -tags integration -timeout 10m ./test/integration/...

test-all: test test-integration ## Run every test

cover: ## Run every test with coverage
	go test -race -tags integration -timeout 10m -covermode=atomic \
		-coverpkg=$(MODULE)/internal/...,$(MODULE)/pkg/... \
		-coverprofile=$(COVERAGE) $(PKG) ./test/integration/...
	go tool cover -func=$(COVERAGE) | tail -1

lint: ## Run the linter
	golangci-lint run

fmt: ## Format the code
	gofmt -w -s .

tidy: ## Tidy the module
	go mod tidy

up: ## Start PostgreSQL, RabbitMQ and Redpanda
	docker compose up -d
	@echo "waiting for the containers to become healthy..."
	@for i in $$(seq 1 60); do \
		unhealthy=$$(docker compose ps --format '{{.Health}}' | grep -cv healthy || true); \
		[ "$$unhealthy" = "0" ] && { echo "ready"; exit 0; }; \
		sleep 2; \
	done; \
	echo "containers did not become healthy in time"; docker compose ps; exit 1

down: ## Stop the containers and remove their volumes
	docker compose down -v

logs: ## Follow the container logs
	docker compose logs -f

psql: ## Open a psql shell on the development database
	docker compose exec postgres psql -U outbox -d outbox

image: ## Build the container image
	docker build \
		--build-arg VERSION=$(VERSION) \
		--build-arg COMMIT=$(COMMIT) \
		--build-arg BUILD_DATE=$(BUILD_DATE) \
		-t go-outbox:$(VERSION) .

clean: ## Remove build artefacts
	rm -rf bin $(COVERAGE)
