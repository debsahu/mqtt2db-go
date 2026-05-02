SHELL := /usr/bin/env bash

BINARY      := mqtt2db-go
PKG         := github.com/debsahu/mqtt2db-go
CMD         := ./cmd/$(BINARY)
BIN_DIR     := bin
DOCKER_IMG  := mqtt2db-go
DOCKER_TAG  ?= dev

VERSION     ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT      ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
BUILD_DATE  ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS     := -s -w \
	-X '$(PKG)/internal/version.Version=$(VERSION)' \
	-X '$(PKG)/internal/version.Commit=$(COMMIT)' \
	-X '$(PKG)/internal/version.BuildDate=$(BUILD_DATE)'

.PHONY: help
help:
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## Build the binaries (mqtt2db-go + migrate) into ./bin
	@mkdir -p $(BIN_DIR)
	go build -trimpath -ldflags="$(LDFLAGS)" -o $(BIN_DIR)/$(BINARY) $(CMD)
	go build -trimpath -ldflags="$(LDFLAGS)" -o $(BIN_DIR)/migrate ./cmd/migrate

.PHONY: migrate
migrate: ## Run migrations: make migrate ARGS="up" (or "down N" / "version")
	go run ./cmd/migrate $(ARGS)

.PHONY: run
run: ## Run the service against config.dev.yaml
	go run $(CMD) --config=config.dev.yaml

.PHONY: test
test: ## Run unit tests with race detector
	go test -race -count=1 ./internal/...

.PHONY: test-integration
test-integration: ## Run integration tests (requires Docker)
	go test -race -count=1 -tags=integration ./test/integration/...

.PHONY: test-e2e
test-e2e: ## End-to-end test: full pipeline against real Comqtt+Postgres+RustFS
	go test -count=1 -timeout=10m -tags=integration -run TestE2E ./test/integration/...

.PHONY: test-k8s
test-k8s: ## kind + helm rollout test (requires kind, helm, kubectl)
	go test -count=1 -timeout=15m -tags='integration k8s' -run TestK8s ./test/integration/...

.PHONY: stress
stress: ## Run a stress scenario: make stress SCENARIO=steady|burst|soak|pause
	@./test/stress/scenarios.sh $(SCENARIO)

.PHONY: stress-slowdown
stress-slowdown: ## Sustained-slowdown stress (Milestone 13). Three scenarios via toxiproxy.
	# Each scenario runs in its own go-test invocation so toxiproxy
	# doesn't observe leftover docker port reservations from the previous
	# test in the same process.
	go test -tags=slowdown -count=1 -timeout=45m -v -run='^TestSlowdown_Moderate$$' ./test/stress/slowdown/
	go test -tags=slowdown -count=1 -timeout=45m -v -run='^TestSlowdown_Severe$$'   ./test/stress/slowdown/
	go test -tags=slowdown -count=1 -timeout=45m -v -run='^TestSlowdown_Outage$$'   ./test/stress/slowdown/

.PHONY: stress-slowdown-quick
stress-slowdown-quick: ## Same scenarios, abbreviated timings — for development feedback.
	SLOWDOWN_WARMUP=20s SLOWDOWN_TOXIC=60s SLOWDOWN_DRAIN=60s SLOWDOWN_RATE=2000 \
	    go test -tags=slowdown -count=1 -timeout=20m -v -run='^TestSlowdown_Moderate$$' ./test/stress/slowdown/
	SLOWDOWN_WARMUP=20s SLOWDOWN_TOXIC=60s SLOWDOWN_DRAIN=60s SLOWDOWN_RATE=2000 \
	    go test -tags=slowdown -count=1 -timeout=20m -v -run='^TestSlowdown_Severe$$'   ./test/stress/slowdown/
	SLOWDOWN_WARMUP=20s SLOWDOWN_TOXIC=60s SLOWDOWN_DRAIN=60s SLOWDOWN_RATE=2000 \
	    go test -tags=slowdown -count=1 -timeout=20m -v -run='^TestSlowdown_Outage$$'   ./test/stress/slowdown/

.PHONY: stress-oscillation
stress-oscillation: ## Oscillation stress (Milestone 14a). Repeated slow/fast cycles via toxiproxy.
	# Each scenario runs in its own go-test invocation so toxiproxy
	# state from a previous run doesn't leak into the next.
	go test -tags=slowdown -count=1 -timeout=90m -v  -run='^TestSlowdown_OscillationFast$$' ./test/stress/slowdown/
	go test -tags=slowdown -count=1 -timeout=120m -v -run='^TestSlowdown_OscillationSlow$$' ./test/stress/slowdown/

.PHONY: stress-oscillation-quick
stress-oscillation-quick: ## Same scenarios, abbreviated timings — for development feedback.
	# 3 cycles of 15s slow / 15s clean at 2K msg/s. Fast feedback,
	# enough cycles to exercise the ratchet + mode-stability checks.
	SLOWDOWN_WARMUP=20s SLOWDOWN_DRAIN=60s SLOWDOWN_RATE=2000 \
	    SLOWDOWN_OSC_SLOW=15s SLOWDOWN_OSC_CLEAN=15s SLOWDOWN_OSC_CYCLES=3 \
	    go test -tags=slowdown -count=1 -timeout=20m -v -run='^TestSlowdown_OscillationFast$$' ./test/stress/slowdown/
	# Slow-cycle quick params: 75s clean is JUST above the
	# RecoveryWindow=60s default, so the recovery assertion in
	# TestSlowdown_OscillationSlow is structurally testable. Don't
	# shrink below RecoveryWindow or the assertion becomes
	# impossible-by-design.
	SLOWDOWN_WARMUP=20s SLOWDOWN_DRAIN=60s SLOWDOWN_RATE=2000 \
	    SLOWDOWN_OSC_SLOW=75s SLOWDOWN_OSC_CLEAN=75s SLOWDOWN_OSC_CYCLES=3 \
	    go test -tags=slowdown -count=1 -timeout=20m -v -run='^TestSlowdown_OscillationSlow$$' ./test/stress/slowdown/

.PHONY: stress-realistic
stress-realistic: ## Realistic schema sweep (Milestone 14b). All M13 + M14a scenarios @ telemetry_wide + 4KB payloads.
	# Five scenarios in sequence, each with SLOWDOWN_SCHEMA=wide so
	# the harness applies the embedded telemetry_wide DDL and points
	# the Copier + verification queries at it. 4 KB payloads.
	SLOWDOWN_SCHEMA=wide SLOWDOWN_PAYLOAD_BYTES=4096 \
	    go test -tags=slowdown -count=1 -timeout=45m -v -run='^TestSlowdown_Moderate$$' ./test/stress/slowdown/
	SLOWDOWN_SCHEMA=wide SLOWDOWN_PAYLOAD_BYTES=4096 \
	    go test -tags=slowdown -count=1 -timeout=45m -v -run='^TestSlowdown_Severe$$'   ./test/stress/slowdown/
	SLOWDOWN_SCHEMA=wide SLOWDOWN_PAYLOAD_BYTES=4096 \
	    go test -tags=slowdown -count=1 -timeout=45m -v -run='^TestSlowdown_Outage$$'   ./test/stress/slowdown/
	SLOWDOWN_SCHEMA=wide SLOWDOWN_PAYLOAD_BYTES=4096 \
	    go test -tags=slowdown -count=1 -timeout=90m -v -run='^TestSlowdown_OscillationFast$$' ./test/stress/slowdown/
	SLOWDOWN_SCHEMA=wide SLOWDOWN_PAYLOAD_BYTES=4096 \
	    go test -tags=slowdown -count=1 -timeout=120m -v -run='^TestSlowdown_OscillationSlow$$' ./test/stress/slowdown/

.PHONY: stress-realistic-quick
stress-realistic-quick: ## Same wide-schema sweep, abbreviated timings — for development feedback.
	SLOWDOWN_SCHEMA=wide SLOWDOWN_PAYLOAD_BYTES=4096 \
	    SLOWDOWN_WARMUP=20s SLOWDOWN_TOXIC=60s SLOWDOWN_DRAIN=60s SLOWDOWN_RATE=2000 \
	    go test -tags=slowdown -count=1 -timeout=20m -v -run='^TestSlowdown_Moderate$$' ./test/stress/slowdown/
	SLOWDOWN_SCHEMA=wide SLOWDOWN_PAYLOAD_BYTES=4096 \
	    SLOWDOWN_WARMUP=20s SLOWDOWN_TOXIC=60s SLOWDOWN_DRAIN=60s SLOWDOWN_RATE=2000 \
	    go test -tags=slowdown -count=1 -timeout=20m -v -run='^TestSlowdown_Severe$$'   ./test/stress/slowdown/
	SLOWDOWN_SCHEMA=wide SLOWDOWN_PAYLOAD_BYTES=4096 \
	    SLOWDOWN_WARMUP=20s SLOWDOWN_TOXIC=60s SLOWDOWN_DRAIN=60s SLOWDOWN_RATE=2000 \
	    go test -tags=slowdown -count=1 -timeout=20m -v -run='^TestSlowdown_Outage$$'   ./test/stress/slowdown/
	SLOWDOWN_SCHEMA=wide SLOWDOWN_PAYLOAD_BYTES=4096 \
	    SLOWDOWN_WARMUP=20s SLOWDOWN_DRAIN=60s SLOWDOWN_RATE=2000 \
	    SLOWDOWN_OSC_SLOW=15s SLOWDOWN_OSC_CLEAN=15s SLOWDOWN_OSC_CYCLES=3 \
	    go test -tags=slowdown -count=1 -timeout=20m -v -run='^TestSlowdown_OscillationFast$$' ./test/stress/slowdown/
	# 75s clean window > RecoveryWindow=60s so the recovery
	# assertion in TestSlowdown_OscillationSlow is structurally
	# testable. See stress-oscillation-quick for the same constraint.
	SLOWDOWN_SCHEMA=wide SLOWDOWN_PAYLOAD_BYTES=4096 \
	    SLOWDOWN_WARMUP=20s SLOWDOWN_DRAIN=60s SLOWDOWN_RATE=2000 \
	    SLOWDOWN_OSC_SLOW=75s SLOWDOWN_OSC_CLEAN=75s SLOWDOWN_OSC_CYCLES=3 \
	    go test -tags=slowdown -count=1 -timeout=20m -v -run='^TestSlowdown_OscillationSlow$$' ./test/stress/slowdown/

.PHONY: cover
cover: ## Generate coverage report at coverage.out / coverage.html
	go test -race -coverprofile=coverage.out ./internal/...
	go tool cover -html=coverage.out -o coverage.html

.PHONY: lint
lint: ## Run golangci-lint
	golangci-lint run ./...

.PHONY: hooks
hooks: ## Enable repo git hooks (pre-push lint+vet+test)
	git config core.hooksPath .githooks
	@echo "git hooks enabled at .githooks/ — bypass with 'git push --no-verify' or SKIP_PRE_PUSH=1"

.PHONY: fmt
fmt: ## Format Go sources with gofmt and goimports
	gofmt -s -w .
	@command -v goimports >/dev/null 2>&1 && goimports -w -local $(PKG) . || true

.PHONY: tidy
tidy: ## Sync go.mod / go.sum
	go mod tidy

.PHONY: vet
vet: ## go vet
	go vet ./...

.PHONY: golang-base
golang-base: ## Build the pinned golang:1.26-alpine base image (run once)
	docker build -t golang:1.26-alpine deploy/golang-1.26-alpine/

.PHONY: comqtt-image
comqtt-image: ## Build the pinned Comqtt v2.6.2 image (run once)
	docker build -t comqtt:v2.6.2 deploy/comqtt/

.PHONY: docker
docker: ## Build Docker image $(DOCKER_IMG):$(DOCKER_TAG)
	docker build -t $(DOCKER_IMG):$(DOCKER_TAG) .

.PHONY: compose-up
compose-up: ## Start local dev stack (Comqtt, Postgres, PgBouncer, RustFS)
	docker compose -f deploy/docker-compose.yml up -d

.PHONY: compose-down
compose-down: ## Stop local dev stack
	docker compose -f deploy/docker-compose.yml down -v

.PHONY: loadtest
loadtest: ## Run load generator (override RATE/DURATION/DEVICES)
	go run ./test/loadgen --rate=$(or $(RATE),5000) --duration=$(or $(DURATION),60s) --devices=$(or $(DEVICES),1000)

.PHONY: clean
clean: ## Remove build artifacts
	rm -rf $(BIN_DIR) coverage.out coverage.html
