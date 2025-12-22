# BusTub-Go Makefile

.PHONY: all build test clean fmt lint

# Default target
all: build

# Build all packages
build:
	go build ./...

# Run all tests
test:
	go test ./...

# Run tests with race detector
test-race:
	go test -race ./...

# Run tests with coverage
test-coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

# Run benchmarks
bench:
	go test -bench=. ./...

# Format code
fmt:
	go fmt ./...

# Lint code (requires golangci-lint)
lint:
	golangci-lint run

# Clean build artifacts
clean:
	go clean ./...
	rm -f coverage.out coverage.html

# Run the shell
run:
	go run cmd/bustub/main.go

# Generate mocks (if using mockgen)
mocks:
	go generate ./...
