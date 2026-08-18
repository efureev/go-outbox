SHELL := /bin/sh

BIN        ?= bin/outbox
PKG        ?= ./...
COVERAGE   ?= coverage.out
# Fixed counts rather than durations: the harness would otherwise spend the ramp
# rebuilding fixtures for tiny values of b.N. The two suites need very different
# counts — a throughput iteration is one message among thousands in flight, a
# latency iteration is one message waited on from end to end.
BENCHTIME     ?= 3000x
BENCHLATENCY  ?= 200x
BENCHCOUNT    ?= 3
VERSION    ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT     ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
BUILD_DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

MODULE  := github.com/efureev/go-outbox
LDFLAGS := -s -w \
	-X 'main.version=$(VERSION)' \
	-X 'main.commit=$(COMMIT)' \
	-X 'main.date=$(BUILD_DATE)'

.DEFAULT_GOAL := help
.PHONY: help build run test test-integration test-all bench bench-throughput bench-latency cover lint lint-host fmt fmt-host tidy up down logs psql image clean clean-cache

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

bench: bench-throughput bench-latency ## Run every benchmark (needs `make up`)

bench-throughput: ## Benchmark sustained delivery
	go test -tags integration -run '^$$' -bench 'BenchmarkDrain' \
		-benchtime $(BENCHTIME) -count $(BENCHCOUNT) -timeout 60m ./test/integration/...

bench-latency: ## Benchmark insert-to-broker latency
	go test -tags integration -run '^$$' -bench 'BenchmarkNotifyLatency' \
		-benchtime $(BENCHLATENCY) -count $(BENCHCOUNT) -timeout 60m ./test/integration/...

cover: ## Run every test with coverage
	go test -race -tags integration -timeout 10m -covermode=atomic \
		-coverpkg=$(MODULE)/internal/...,$(MODULE)/pkg/... \
		-coverprofile=$(COVERAGE) $(PKG) ./test/integration/...
	go tool cover -func=$(COVERAGE) | tail -1

# Both run in the container pinned in docker-compose.yml, so a developer and CI
# see the same findings. --user keeps anything written to the working tree —
# formatted files, caches — owned by the caller rather than by root.
# Outside the repository, so nothing that walks the working tree trips over
# third-party sources. Created here rather than by Docker, which would make it
# root-owned on Linux.
LINT_CACHE ?= $(HOME)/.cache/go-outbox-lint
export LINT_CACHE

LINT_RUN = @mkdir -p "$(LINT_CACHE)" && docker compose run --rm --user "$(shell id -u):$(shell id -g)" lint

lint: ## Run the linter, pinned to the CI version
	$(LINT_RUN) golangci-lint run

lint-host: ## Run the linter from PATH (faster; may differ from CI)
	golangci-lint run

fmt: ## Format the code, pinned to the CI version
	$(LINT_RUN) golangci-lint fmt

fmt-host: ## Format the code using the toolchain on PATH
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

clean-cache: ## Remove the lint container's caches
	rm -rf "$(LINT_CACHE)"
