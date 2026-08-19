.PHONY: build cli controller agent proto console clean test tidy ci

BIN_DIR := bin

build: cli controller agent

cli:
	go build -o $(BIN_DIR)/groundplane ./cmd/groundplane

controller:
	go build -o $(BIN_DIR)/controller ./cmd/controller

agent:
	go build -o $(BIN_DIR)/agent ./cmd/agent

proto:
	protoc \
		--go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		proto/agent.proto

console:
	cd console && npm install && npm run build

test:
	go test ./... -count=1 -race -coverprofile=coverage.out -covermode=atomic

tidy:
	go mod tidy

# ci mirrors docs/standards.md, section 14, exactly — a task is
# not complete until this passes locally, same as the CI pipeline. The
# `go generate` line is commented out until proto/agentpb and an
# OpenAPI-generated client actually exist to regenerate.
ci:
	go mod tidy && git diff --exit-code go.mod go.sum
	test -z "$$(gofmt -l .)"
	golines --max-len=120 --no-reformat-tags --list-files ./internal/ ./pkg/ ./cmd/
	go vet ./...
	go test ./... -count=1 -race -coverprofile=coverage.out -covermode=atomic
	# go generate ./...

clean:
	rm -rf $(BIN_DIR) console/dist
