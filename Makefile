SHELL := /bin/bash
BIN   := bin
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)

# A local Postgres for the integration tests. Override to point at your own.
TEST_DB ?= postgres://primeflow:primeflow@localhost:5432/primeflow?sslmode=disable

.PHONY: all build test test-unit test-integration lint fmt vet run-server run-worker docker clean tidy

all: build

build: ## Build the server CLI and the example worker
	@mkdir -p $(BIN)
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o $(BIN)/primeflow ./cmd/primeflow
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o $(BIN)/primex-worker ./examples/primex-worker
	@echo "built $(BIN)/primeflow and $(BIN)/primex-worker ($(VERSION))"

test-unit: ## Run the tests that need no database
	go test ./... -count=1

# -p 1 matters: the integration packages each reset the same database, so they
# must not run concurrently.
test-integration: ## Run every test, including the ones that need Postgres
	PRIMEFLOW_TEST_DATABASE_URL="$(TEST_DB)" go test ./... -count=1 -p 1

test: test-integration

vet:
	go vet ./...

fmt:
	gofmt -l -w .

lint: fmt vet ## Format and vet; add golangci-lint here if you use it

tidy:
	go mod tidy

run-server: build ## Run the API, UI, scheduler and automations
	PRIMEFLOW_DATABASE_URL="$(TEST_DB)" $(BIN)/primeflow server

run-worker: build ## Run the example worker
	PRIMEFLOW_DATABASE_URL="$(TEST_DB)" PRIMEFLOW_QUEUES=default,vcd,metering $(BIN)/primex-worker

docker: ## Build the container image
	docker build --build-arg VERSION=$(VERSION) -t detroitttttxp/primeflow:$(VERSION) -t detroitttttxp/primeflow:latest .

clean:
	rm -rf $(BIN)
