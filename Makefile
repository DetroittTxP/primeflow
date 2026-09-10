SHELL := /bin/bash
BIN   := bin
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)

# A local Postgres for the integration tests. Override to point at your own.
TEST_DB ?= postgres://primeflow:primeflow@localhost:5432/primeflow?sslmode=disable

# Test-data seeding. SEED_RUNS queues that many demo runs, which only go
# anywhere if a worker is up; the queues, deployments and accounts land either
# way. SEED_ARGS is the escape hatch: SEED_ARGS=-no-users, and so on.
SEED_PASSWORD ?= primeflow-demo
SEED_RUNS     ?= 0
SEED_ARGS     ?=

# The published image. Both the server and the worker run it; a self-hosted
# install pulls it and needs nothing else from this repository.
IMAGE     ?= detroitttttxp/primeflow
PLATFORMS ?= linux/amd64,linux/arm64

.PHONY: all build test test-unit test-integration lint fmt vet run-server run-worker seed seed-compose dev dev-down docker docker-push clean tidy

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

# Named directories rather than `.`: this tree also holds air's scratch dir and,
# on a working copy, agent worktree checkouts of the repo itself. `go vet ./...`
# skips both on its own -- each is a nested module -- but gofmt knows nothing
# about modules and would happily rewrite whatever it walked into.
fmt:
	gofmt -l -w ./cmd ./examples ./internal ./pkg

lint: fmt vet ## Format and vet; add golangci-lint here if you use it

tidy:
	go mod tidy

run-server: build ## Run the API, UI, scheduler and automations
	PRIMEFLOW_DATABASE_URL="$(TEST_DB)" $(BIN)/primeflow server

run-worker: build ## Run the example worker
	PRIMEFLOW_DATABASE_URL="$(TEST_DB)" PRIMEFLOW_QUEUES=default,vcd,metering $(BIN)/primex-worker

seed: build ## Seed queues, deployments and demo accounts into $(TEST_DB)
	PRIMEFLOW_DATABASE_URL="$(TEST_DB)" $(BIN)/primeflow seed \
		-password "$(SEED_PASSWORD)" -runs $(SEED_RUNS) $(SEED_ARGS)

# The compose image already carries the binary and the in-network DSN, so this
# needs no Go toolchain on the host. --build is what keeps a stack that was
# started before this command existed from failing on "unknown command seed";
# it is a cached no-op once the image is current.
seed-compose: ## Seed the docker-compose stack's database
	docker compose run --rm --build server seed \
		-password "$(SEED_PASSWORD)" -runs $(SEED_RUNS) $(SEED_ARGS)

# The compose stack rebuilt on every save. Dockerfile.dev keeps the Go
# toolchain in the image and air watches the bind-mounted source; the caches are
# named volumes, so `dev-down` is cheap to undo.
DEV_COMPOSE := docker compose -f docker-compose.yml -f docker-compose.dev.yml

dev: ## Bring the stack up with hot reload (air rebuilds server and workers on save)
	$(DEV_COMPOSE) up --build

dev-down: ## Stop the hot-reload stack (keeps the build caches)
	$(DEV_COMPOSE) down

docker: ## Build the container image for this machine
	docker build --build-arg VERSION=$(VERSION) -t $(IMAGE):$(VERSION) -t $(IMAGE):latest .

# What a self-hosted user pulls, so it has to carry both architectures: a plain
# `docker build` on an arm64 machine produces an amd64 binary (TARGETARCH
# defaults to amd64 in the Dockerfile) inside an arm64-labelled image, which
# runs under emulation and misleads everyone who inspects it later. buildx with
# an explicit platform list makes the manifest and the binaries agree.
docker-push: ## Build and push the multi-arch image (docker login first)
	docker buildx build --platform $(PLATFORMS) --build-arg VERSION=$(VERSION) \
		-t $(IMAGE):$(VERSION) -t $(IMAGE):latest --push .

clean:
	rm -rf $(BIN)
