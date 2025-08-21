# Obelisk Message Broker - Makefile

.PHONY: build run test clean dev help

# Default target
all: build

## Build
build: ## Build the binary
	go build -o obelisk cmd/Obelisk/main.go

## Run
run: kill ## Run the server
	go run cmd/Obelisk/main.go

dev: kill ## Run in development mode with auto-restart
	go run cmd/Obelisk/main.go &

## Test
test: kill ## Run all tests
	go test -v -timeout 60s ./...

## Clean up
clean: ## Clean build artifacts
	rm -f obelisk
	go clean

## Help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-10s\033[0m %s\n", $$1, $$2}'
