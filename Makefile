.PHONY: build run docker-build docker-up docker-down clean deps fmt vet

# Build the binary
build:
	go build -o bin/tpwtp-collector ./cmd/collector

# Run locally
run:
	go run ./cmd/collector

# Build Docker image
docker-build:
	docker build -t tpwtp-collector:latest .

# Start local dev stack (postgres + collector)
docker-up:
	docker compose up -d

# Stop local dev stack
docker-down:
	docker compose down

# Clean build artifacts
clean:
	rm -rf bin/
	go clean

# Download dependencies
deps:
	go mod download
	go mod tidy

# Format code
fmt:
	go fmt ./...

# Vet code
vet:
	go vet ./...

# macOS service management
install:
	sudo ./deployments/macos/deploy.sh install

update:
	sudo ./deployments/macos/deploy.sh update

uninstall:
	sudo ./deployments/macos/deploy.sh uninstall
