# Makefile for sds-node-configurator E2E tests (Common Scheduler Extender + module scenarios)

.PHONY: help
help: ## Show help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: deps
deps: ## Download dependencies
	go mod download
	go mod tidy
	@$(MAKE) fix-mod-permissions

# === Local run ===
# CI smoke uses !stress-test (everything except max-VG stress), matching the storage-e2e
# reusable workflow default. Override: make test-go GINKGO_LABEL_FILTER=stress-test
GINKGO_LABEL_FILTER ?= !stress-test
E2E_TEST_TIMEOUT ?= 90m
# CI workflow uses go test -timeout 3h30m regardless of this var.

.PHONY: test
test: ## Run E2E smoke (!stress-test, без max-VG stress) — как CI
	$(MAKE) test-go

.PHONY: test-go
test-go: ## Как в CI smoke: TestSdsNodeConfigurator + -ginkgo.label-filter=!stress-test
	go test -v -count=1 -timeout "$(E2E_TEST_TIMEOUT)" ./tests/ -run '^TestSdsNodeConfigurator$$' \
		-ginkgo.label-filter="$(GINKGO_LABEL_FILTER)"

.PHONY: test-stress
test-stress: ## Только max-VG stress (label stress-test)
	go test -v -count=1 -timeout 240m ./tests/ -run '^TestSdsNodeConfigurator$$' \
		-ginkgo.label-filter=stress-test

.PHONY: test-focus
test-focus: ## Run by test name (make test-focus FOCUS="TestSdsNodeConfigurator")
	@if [ -z "$(FOCUS)" ]; then \
		echo "Error: specify FOCUS=<test name>"; \
		exit 1; \
	fi
	go test -v -count=1 -timeout 60m ./tests/ -run "$(FOCUS)" \
		-ginkgo.label-filter="$(GINKGO_LABEL_FILTER)"

.PHONY: install-ginkgo
install-ginkgo: ## Install Ginkgo CLI
	go install github.com/onsi/ginkgo/v2/ginkgo@latest

.PHONY: clean
clean: ## Clean test cache
	go clean -testcache

.PHONY: fix-mod-permissions
fix-mod-permissions: ## Writable storage-e2e in GOMODCACHE or GOPATH (PrepareBootstrapConfig writes temp/ under module path)
	@GOMODCACHE=$${GOMODCACHE:-$(CURDIR)/.gomodcache}; \
	GOPATH=$${GOPATH:-$$HOME/go}; \
	for root in "$$GOMODCACHE" "$$GOPATH/pkg/mod"; do \
		[ -d "$$root" ] || continue; \
		for dir in "$$root"/github.com/deckhouse/storage-e2e@*; do \
			if [ -d "$$dir" ]; then \
				echo "Fixing permissions for $$dir"; \
				chmod -R +w "$$dir" 2>/dev/null || true; \
				mkdir -p "$$dir/temp/cluster" 2>/dev/null || true; \
				cfg="$$dir/internal/config/config.go"; \
				if [ -f "$$cfg" ] && grep -q 'SecretsWaitTimeout   = 2 \* time\.Minute' "$$cfg" 2>/dev/null; then \
					sed -i 's/SecretsWaitTimeout   = 2 \* time\.Minute/SecretsWaitTimeout   = 15 * time.Minute/' "$$cfg"; \
					echo "Patched SecretsWaitTimeout to 15m in $$dir"; \
				fi; \
				if [ -f "$$cfg" ] && grep -q 'DockerInstallTimeout     = 10 \* time\.Minute' "$$cfg" 2>/dev/null; then \
					sed -i 's/DockerInstallTimeout     = 10 \* time\.Minute/DockerInstallTimeout     = 30 * time.Minute/' "$$cfg"; \
					echo "Patched DockerInstallTimeout to 30m in $$dir"; \
				fi; \
			fi; \
		done; \
	done

.PHONY: check-env
check-env: ## Print the env the suite/SDK actually reads (e2e.Connect + cfg)
	@echo "# SDK cluster connection (required by e2e.Connect):"
	@echo "E2E_TEST_CLUSTER_PROVIDER:            $${E2E_TEST_CLUSTER_PROVIDER}"
	@echo "E2E_CLUSTER_CONFIG_YAML_PATH:         $${E2E_CLUSTER_CONFIG_YAML_PATH}"
	@echo "# dvp base-cluster (provider=dvp):"
	@echo "E2E_DVP_BASE_CLUSTER_SSH_USER:        $${E2E_DVP_BASE_CLUSTER_SSH_USER}"
	@echo "E2E_DVP_BASE_CLUSTER_SSH_HOST:        $${E2E_DVP_BASE_CLUSTER_SSH_HOST}"
	@echo "E2E_DVP_BASE_CLUSTER_STORAGE_CLASS:   $${E2E_DVP_BASE_CLUSTER_STORAGE_CLASS}"
	@echo "# suite config (cfg.Load / cfg.LoadStress):"
	@echo "TEST_CLUSTER_NAMESPACE:               $${TEST_CLUSTER_NAMESPACE}"
	@echo "MODULES_MODULE_TAG:                   $${MODULES_MODULE_TAG}"
	@echo "GINKGO_LABEL_FILTER:                  $(GINKGO_LABEL_FILTER)"

.PHONY: lint
lint: ## Run golangci-lint
	golangci-lint run ./...

.DEFAULT_GOAL := help
