.PHONY: build cli controller controller-dev agent proto console console-toolchain console-verify console-release-smoke clean test tidy ci

BIN_DIR := bin
NODE_VERSION := 24.19.0
NPM_VERSION := 11.17.0

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

proto:
	protoc \
		--plugin=protoc-gen-go="$$(go tool -n protoc-gen-go)" \
		--plugin=protoc-gen-go-grpc="$$(go tool -n protoc-gen-go-grpc)" \
		--go_out=. --go_opt=module=github.com/AlanD20/groundplane \
		--go-grpc_out=. --go-grpc_opt=module=github.com/AlanD20/groundplane \
		proto/agent.proto

console-toolchain:
	test "$$(cat .node-version)" = "$(NODE_VERSION)"
	test "$$(node --version)" = "v$(NODE_VERSION)"
	test "$$(npm --version)" = "$(NPM_VERSION)"
	node -e 'if (require("./console/package.json").packageManager !== "npm@$(NPM_VERSION)") process.exit(1)'

console: console-toolchain
	cd console && npm ci && npm run build
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

tidy:
	go mod tidy

# ci mirrors docs/standards.md, section 14, exactly — a task is
# not complete until this passes locally, same as the CI pipeline. The
# `go generate` line is commented out until proto/agentpb and an
# OpenAPI-generated client actually exist to regenerate.
ci: console | $(BIN_DIR)
	go mod tidy && git diff --exit-code go.mod go.sum
	test -z "$$(gofmt -l .)"
	test -z "$$(golines --max-len=120 --no-reformat-tags --list-files ./internal/ ./pkg/ ./cmd/ ./console/)"
	go vet -tags groundplane_console ./...
	go test -tags groundplane_console ./... -count=1 -race -coverprofile=coverage.out -covermode=atomic
	go build -tags groundplane_console -o $(BIN_DIR)/controller ./cmd/controller
	$(MAKE) console-release-smoke
	# go generate ./...

clean:
	rm -rf $(BIN_DIR) console/dist
