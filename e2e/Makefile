# Makefile for sds-node-configurator E2E tests

# storage-e2e PrepareBootstrapConfig writes under the extracted module in GOMODCACHE; shared caches (e.g. /opt/.../go/pkg/mod) are often read-only on self-hosted runners.
E2E_GOMODCACHE ?= $(CURDIR)/.gomodcache
E2E_GOCACHE ?= $(CURDIR)/.gocache

.PHONY: help
help: ## Show help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-24s\033[0m %s\n", $$1, $$2}'

.PHONY: deps
deps: ## Download dependencies and patch storage-e2e in module cache
	go mod download
	go mod tidy
	@$(MAKE) fix-mod-permissions

.PHONY: fix-mod-permissions
fix-mod-permissions: ## Writable storage-e2e in GOMODCACHE + optional patches (bootstrap temp/, reconnect, VM cleanup on bootstrap fail)
	@GOMODCACHE=$${GOMODCACHE:-$(CURDIR)/.gomodcache}; \
	GOPATH=$${GOPATH:-$$HOME/go}; \
	mkdir -p "$$GOMODCACHE" "$(E2E_GOCACHE)"; \
	for root in "$$GOMODCACHE" "$$GOPATH/pkg/mod"; do \
		[ -d "$$root" ] || continue; \
		for dir in "$$root"/github.com/deckhouse/storage-e2e@*; do \
			if [ -d "$$dir" ]; then \
				echo "Fixing permissions for $$dir"; \
				chmod -R u+w "$$dir" 2>/dev/null || true; \
				mkdir -p "$$dir/temp/cluster" 2>/dev/null || true; \
				cfg="$$dir/internal/config/config.go"; \
				if [ -f "$$cfg" ] && grep -q 'SecretsWaitTimeout   = 2 \* time\.Minute' "$$cfg" 2>/dev/null; then \
					sed -i 's/SecretsWaitTimeout   = 2 \* time\.Minute/SecretsWaitTimeout   = 15 * time.Minute/' "$$cfg"; \
					echo "Patched SecretsWaitTimeout to 15m in $$dir"; \
				fi; \
				RP="$(CURDIR)/patches/storage-e2e-reconnect-after-add-nodes.patch"; \
				if [ -f "$$RP" ] && [ -f "$$dir/pkg/cluster/cluster.go" ] && ! grep -q 'refresh SSH tunnel after node bootstrap' "$$dir/pkg/cluster/cluster.go" 2>/dev/null; then \
					patch -p0 --batch -d "$$dir" -i "$$RP" || exit 1; \
					echo "Applied reconnect-after-add-nodes patch in $$dir"; \
				fi; \
				BP="$(CURDIR)/patches/storage-e2e-cleanup-vms-on-prepare-bootstrap-fail.patch"; \
				if [ -f "$$BP" ] && [ -f "$$dir/pkg/cluster/cluster.go" ] && ! grep -q 'cluster-state.json was saved at step 6' "$$dir/pkg/cluster/cluster.go" 2>/dev/null; then \
					patch -p0 --batch -d "$$dir" -i "$$BP" || exit 1; \
					echo "Applied prepare-bootstrap-fail VM cleanup patch in $$dir"; \
				fi; \
			fi; \
		done; \
	done

.PHONY: test
test: ## Run all tests (TestSdsNodeConfigurator) with writable GOMODCACHE
	@mkdir -p "$(E2E_GOMODCACHE)" "$(E2E_GOCACHE)"
	GOMODCACHE=$(E2E_GOMODCACHE) GOCACHE=$(E2E_GOCACHE) go mod download
	GOMODCACHE=$(E2E_GOMODCACHE) GOCACHE=$(E2E_GOCACHE) $(MAKE) fix-mod-permissions
	GOMODCACHE=$(E2E_GOMODCACHE) GOCACHE=$(E2E_GOCACHE) go test -v -count=1 -timeout 60m ./tests/

.PHONY: test-focus
test-focus: ## Run specific test (make test-focus FOCUS="TestSdsNodeConfigurator")
	@if [ -z "$(FOCUS)" ]; then \
		echo "Error: specify FOCUS=<test name>"; \
		exit 1; \
	fi
	@mkdir -p "$(E2E_GOMODCACHE)" "$(E2E_GOCACHE)"
	GOMODCACHE=$(E2E_GOMODCACHE) GOCACHE=$(E2E_GOCACHE) go mod download
	GOMODCACHE=$(E2E_GOMODCACHE) GOCACHE=$(E2E_GOCACHE) $(MAKE) fix-mod-permissions
	GOMODCACHE=$(E2E_GOMODCACHE) GOCACHE=$(E2E_GOCACHE) go test -v -count=1 -timeout 60m ./tests/ -run "$(FOCUS)"

.PHONY: clean
clean: ## Clean test cache
	go clean -testcache

.PHONY: lint
lint: ## Run golangci-lint
	golangci-lint run ./...

.DEFAULT_GOAL := help
