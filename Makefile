SHELL := /bin/bash
# Load .env if present so `make run` works without exporting variables.
ifneq (,$(wildcard .env))
include .env
export
endif

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)
TEST_DB ?= postgres://webevent:webevent@localhost:5433/postgres?sslmode=disable

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

.PHONY: db-up
db-up: ## Start Postgres in Docker and wait until healthy
	docker compose up -d --wait postgres

.PHONY: db-down
db-down: ## Stop Postgres (keeps data)
	docker compose down

.PHONY: db-reset
db-reset: ## Destroy and recreate the local database volume
	docker compose down -v && docker compose up -d --wait postgres

.PHONY: build
build: ## Build all binaries into ./bin
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/ ./cmd/...

.PHONY: run
run: ## Run the server locally
	go run ./cmd/webevent-server serve

.PHONY: migrate
migrate: ## Apply migrations and exit
	go run ./cmd/webevent-server migrate

.PHONY: test
test: ## Run all tests (unit + integration) with the race detector
	WEBEVENT_TEST_DATABASE_URL="$(TEST_DB)" go test -race -count=1 ./...

.PHONY: test-e2e
test-e2e: ## Run end-to-end tests against the real server binary (slow)
	WEBEVENT_TEST_DATABASE_URL="$(TEST_DB)" go test -race -count=1 -tags e2e -timeout 15m ./test/e2e/...

.PHONY: fuzz
fuzz: ## Run each fuzz target for 30s
	go test -run XXX -fuzz FuzzVerifyNeverPanics -fuzztime 30s ./pkg/webhook
	go test -run XXX -fuzz FuzzValidateEndpointURL -fuzztime 30s ./internal/api
	go test -run XXX -fuzz FuzzNormalizeEventTypes -fuzztime 30s ./internal/api
	go test -run XXX -fuzz FuzzIdempotencyKey -fuzztime 30s ./internal/api

.PHONY: bench
bench: ## Run database benchmarks
	WEBEVENT_TEST_DATABASE_URL="$(TEST_DB)" go test -run XXX -bench . -benchtime 3s ./internal/store

.PHONY: test-unit
test-unit: ## Run tests that need no database
	WEBEVENT_TEST_DATABASE_URL= go test -race -count=1 ./...

.PHONY: cover
cover: ## Run tests with coverage report
	WEBEVENT_TEST_DATABASE_URL="$(TEST_DB)" go test -race -count=1 -coverpkg=./... -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1

.PHONY: lint
lint: ## Run golangci-lint
	golangci-lint run ./...

.PHONY: fmt
fmt: ## Format code
	gofmt -s -w .

.PHONY: tidy
tidy: ## Tidy go.mod/go.sum
	go mod tidy

.PHONY: check
check: fmt tidy lint test ## Everything CI runs, locally
