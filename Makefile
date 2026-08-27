.PHONY: build cli controller controller-dev agent agent-image agent-image-smoke proto api generate console console-toolchain console-verify console-release-smoke backupstage-host-acceptance backupstage-host-acceptance-compile c15-connector-acceptance s3compatible-minio-acceptance s3compatible-minio-acceptance-compile architecture-check swarm-check clean test tidy ci

BIN_DIR := bin
NODE_VERSION := 24.19.0
NPM_VERSION := 11.17.0
AGENT_IMAGE ?= groundplane-agent:dev
AGENT_VERSION ?= dev
DOCKER ?= docker

build: cli controller agent

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

cli: | $(BIN_DIR)
	go build -o $(BIN_DIR)/groundplane ./cmd/groundplane

controller: console | $(BIN_DIR)
	go build -tags groundplane_console -o $(BIN_DIR)/controller ./cmd/controller

controller-dev: | $(BIN_DIR)
	go build -o $(BIN_DIR)/controller-dev ./cmd/controller

agent: | $(BIN_DIR)
	go build -o $(BIN_DIR)/agent ./cmd/agent

agent-image:
	$(DOCKER) build --pull --file Dockerfile.agent --build-arg AGENT_VERSION="$(AGENT_VERSION)" --tag "$(AGENT_IMAGE)" .

agent-image-smoke: agent-image
	@set -eu; \
		image="$(AGENT_IMAGE)"; \
		test "$$($(DOCKER) image inspect "$$image" --format '{{json .Config.Entrypoint}}')" = '["/usr/local/bin/groundplane-agent"]'; \
		test "$$($(DOCKER) image inspect "$$image" --format '{{json .Config.Cmd}}')" = 'null'; \
		test "$$($(DOCKER) image inspect "$$image" --format '{{.Config.User}}')" = '0:0'; \
		docker_version="$$($(DOCKER) run --rm --entrypoint docker "$$image" --version | awk '{gsub(/,/, "", $$3); print $$3}')"; \
		compose_version="$$($(DOCKER) run --rm --entrypoint docker "$$image" compose version --short)"; \
		test "$$docker_version" = '29.1.3'; \
		test "$$compose_version" = '2.40.3'

proto:
	protoc \
		--plugin=protoc-gen-go="$$(go tool -n protoc-gen-go)" \
		--plugin=protoc-gen-go-grpc="$$(go tool -n protoc-gen-go-grpc)" \
		--go_out=. --go_opt=module=github.com/AlanD20/groundplane \
		--go-grpc_out=. --go-grpc_opt=module=github.com/AlanD20/groundplane \
		proto/agent.proto

api: console-toolchain
	test -x console/node_modules/.bin/openapi-typescript
	go run ./internal/openapigen -output openapi.json
	go tool oapi-codegen -config internal/cli/apiclient/generated/oapi-codegen.yaml openapi.json
	cd console && npm run generate:api

generate: proto api

console-toolchain:
	test "$$(cat .node-version)" = "$(NODE_VERSION)"
	test "$$(node --version)" = "v$(NODE_VERSION)"
	test "$$(npm --version)" = "$(NPM_VERSION)"
	node -e 'if (require("./console/package.json").packageManager !== "npm@$(NPM_VERSION)") process.exit(1)'

console: console-toolchain
	cd console && npm ci && npm test && npm run build
	$(MAKE) console-verify

console-verify:
	test ! -d console/public/assets
	test -f console/dist/index.html
	test -n "$$(find console/dist/assets -type f -print -quit)"
	@find console/dist/assets -type f -print | awk '!/-[0-9a-f]{8}\.[^/]+$$/ { print "unfingerprinted Console asset: " $$0 > "/dev/stderr"; failed=1 } END { exit failed }'
	@find console/dist/assets -type f -print | while IFS= read -r file; do \
		base="$${file##*/}"; \
		case "$$base" in \
			*-[0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f].*) ;; \
			*) echo "unfingerprinted Console asset: $$file" >&2; exit 1 ;; \
		esac; \
	done

console-release-smoke: | $(BIN_DIR)
	@set -eu; \
		controller="$(CURDIR)/$(BIN_DIR)/controller"; \
		smoke="$(CURDIR)/$(BIN_DIR)/console-release-smoke.test"; \
		test -x "$$controller"; \
		go version -m "$$controller" | grep -F -- '-tags=groundplane_console' >/dev/null; \
		test -f console/dist/index.html; \
		asset_file="$$(find console/dist/assets -type f -print | LC_ALL=C sort | head -n 1)"; \
		test -n "$$asset_file"; \
		index_hash="$$(sha256sum console/dist/index.html | cut -d' ' -f1)"; \
		asset_hash="$$(sha256sum "$$asset_file" | cut -d' ' -f1)"; \
		asset_path="/$${asset_file#console/dist/}"; \
		trap 'rm -f "$$smoke"' EXIT HUP INT TERM; \
		go test -c -tags groundplane_console -o "$$smoke" ./internal/app; \
		rm -rf console/dist; \
		GROUNDPLANE_CONSOLE_INDEX_SHA256="$$index_hash" \
		GROUNDPLANE_CONSOLE_ASSET_PATH="$$asset_path" \
		GROUNDPLANE_CONSOLE_ASSET_SHA256="$$asset_hash" \
		"$$smoke" -test.run '^TestProductionConsoleReleaseSmoke$$'

test:
	go test ./... -count=1 -race -coverprofile=coverage.out -covermode=atomic

c15-connector-acceptance: console-toolchain
	go test -race -count=1 -run '^(TestC15|TestConnector|TestPrepareConnector|TestParseConnectorDocumentRequiresExplicitPathStyle)' \
		./pkg/api ./internal/common/s3connector ./internal/core ./internal/app ./internal/infra/etcd \
		./internal/controller ./internal/cli ./internal/cli/apiclient
	go test -race -count=1 ./internal/controller/secretvalue ./internal/infra/age ./internal/infra/s3compatible
	cd console && npm test
	cd console && npm run build

tidy:
	go mod tidy

backupstage-host-acceptance-compile:
	@test "$$(go env GOOS)" = linux || { echo "backupstage host acceptance requires Linux" >&2; exit 1; }
	go test -race -tags backupstage_mount_acceptance -run '^$$' ./internal/infra/backupstage

backupstage-host-acceptance:
	@set -eu; \
		test "$$(go env GOOS)" = linux || { echo "backupstage host acceptance requires Linux" >&2; exit 1; }; \
		if test "$$(id -u)" = 0; then \
			go test -tags backupstage_mount_acceptance -count=1 -race ./internal/infra/backupstage; \
		else \
			sudo -n env "PATH=$$PATH" go test -tags backupstage_mount_acceptance -count=1 -race \
				./internal/infra/backupstage; \
	fi

s3compatible-minio-acceptance-compile:
	@test "$$(go env GOOS)" = linux || { echo "S3-compatible MinIO acceptance requires Linux" >&2; exit 1; }
	go test -race -tags s3compatible_minio_acceptance -run '^$$' ./internal/infra/s3compatible

s3compatible-minio-acceptance:
	@set -eu; \
		test "$$(go env GOOS)" = linux || { echo "S3-compatible MinIO acceptance requires Linux" >&2; exit 1; }; \
		test -n "$${GROUNDPLANE_MINIO_ENDPOINT:-}" || { echo "GROUNDPLANE_MINIO_ENDPOINT is required" >&2; exit 1; }; \
		test -n "$${GROUNDPLANE_MINIO_BUCKET:-}" || { echo "GROUNDPLANE_MINIO_BUCKET is required" >&2; exit 1; }; \
		test -n "$${GROUNDPLANE_MINIO_ACCESS_KEY:-}" || { echo "GROUNDPLANE_MINIO_ACCESS_KEY is required" >&2; exit 1; }; \
		test -n "$${GROUNDPLANE_MINIO_SECRET_KEY:-}" || { echo "GROUNDPLANE_MINIO_SECRET_KEY is required" >&2; exit 1; }; \
		go test -race -count=1 -tags s3compatible_minio_acceptance \
			-run '^TestMinIOLiveBackupObjectLifecycle$$' ./internal/infra/s3compatible

# ci mirrors docs/standards.md, section 14, exactly — a task is
# not complete until this passes locally, same as the CI pipeline. The
# `go generate` line is commented out until proto/agentpb and an
# OpenAPI-generated client actually exist to regenerate.
architecture-check:
	go test ./internal/architecturecheck -count=1
	go run ./cmd/architecture-check -root . -baseline architecture-baseline.json

swarm-check:
	test -n "$(SWARM_MANIFEST)"
	@if test -n "$(SWARM_LANE)"; then \
		go run ./cmd/swarm-check -root . -manifest "$(SWARM_MANIFEST)" -lane "$(SWARM_LANE)"; \
	else \
		go run ./cmd/swarm-check -root . -manifest "$(SWARM_MANIFEST)"; \
	fi

ci: console | $(BIN_DIR)
	$(MAKE) generate
	git diff --exit-code openapi.json internal/cli/apiclient/generated/client.gen.go console/src/lib/api.generated.ts proto/agentpb
	go mod tidy && git diff --exit-code go.mod go.sum
	test -z "$$(gofmt -l .)"
	test -z "$$(golines --max-len=120 --no-reformat-tags --list-files ./internal/ ./pkg/ ./cmd/ ./console/)"
	$(MAKE) architecture-check
	GOTOOLCHAIN=go1.26.0 go tool staticcheck -tags groundplane_console ./...
	go vet -tags groundplane_console ./...
	go test -tags groundplane_console ./... -count=1 -race -coverprofile=coverage.out -covermode=atomic
	$(MAKE) backupstage-host-acceptance
	go build -tags groundplane_console -o $(BIN_DIR)/controller ./cmd/controller
	$(MAKE) console-release-smoke
	$(MAKE) agent-image-smoke

clean:
	rm -rf $(BIN_DIR) console/dist
