# Makefile for sds-node-configurator E2E tests

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: deps
deps: ## Install dependencies
	go mod download
	go mod tidy

.PHONY: test
test: ## Run all e2e tests
	ginkgo -v --progress ./tests/

.PHONY: test-verbose
test-verbose: ## Run tests with verbose output
	ginkgo -v --progress ./tests/

.PHONY: test-focus
test-focus: ## Run a specific test (usage: make test-focus FOCUS="test name")
	@if [ -z "$(FOCUS)" ]; then \
		echo "Error: specify FOCUS=<test name>"; \
		exit 1; \
	fi
	ginkgo -v --progress --focus="$(FOCUS)" ./tests/

.PHONY: install-ginkgo
install-ginkgo: ## Install Ginkgo CLI
	go install github.com/onsi/ginkgo/v2/ginkgo@latest

.PHONY: test-ginkgo
test-ginkgo: ## Run tests via Ginkgo CLI
	ginkgo -v --progress ./tests/

.PHONY: test-watch
test-watch: ## Run tests in watch mode (auto-restart on changes)
	ginkgo watch -v ./tests/

.PHONY: clean
clean: ## Clean temporary files
	go clean -testcache

.PHONY: check-env
check-env: ## Check environment variables for tests
	@echo "Checking environment variables for E2E tests:"
	@echo "  E2E_NODE_NAME:      ${E2E_NODE_NAME}"
	@echo "  E2E_DEVICE_PATH:    ${E2E_DEVICE_PATH}"
	@echo "  E2E_DEVICE_SERIAL:  ${E2E_DEVICE_SERIAL}"
	@echo ""
	@if [ -z "${E2E_NODE_NAME}" ]; then \
		echo "⚠️  E2E_NODE_NAME not set, default will be used: worker-0"; \
	fi
	@if [ -z "${E2E_DEVICE_PATH}" ]; then \
		echo "⚠️  E2E_DEVICE_PATH not set, default will be used: /dev/sdb"; \
	fi
	@if [ -z "${E2E_DEVICE_SERIAL}" ]; then \
		echo "⚠️  E2E_DEVICE_SERIAL not set (recommended for BlockDevice name verification)"; \
	fi

.PHONY: lint
lint: ## Run linter on code
	golangci-lint run ./...

# Example usage with parameters
.PHONY: example-test
example-test: ## Example of running test with parameters
	@echo "Running E2E test with example parameters:"
	E2E_NODE_NAME=worker-0 \
	E2E_DEVICE_PATH=/dev/sdb \
	E2E_DEVICE_SERIAL=TEST-001 \
	$(MAKE) test

.DEFAULT_GOAL := help

