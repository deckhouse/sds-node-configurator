# Makefile for sds-node-configurator E2E tests

.PHONY: help
help: ## Show help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.PHONY: deps
deps: ## Download dependencies
	go mod download
	go mod tidy

.PHONY: test
test: ## Run all tests
	go test -v -count=1 -timeout 60m ./tests/

.PHONY: test-focus
test-focus: ## Run specific test (make test-focus FOCUS="TestName")
	@if [ -z "$(FOCUS)" ]; then \
		echo "Error: specify FOCUS=<test name>"; \
		exit 1; \
	fi
	go test -v -count=1 -timeout 60m ./tests/ -run "$(FOCUS)"

.PHONY: clean
clean: ## Clean test cache
	go clean -testcache

.PHONY: lint
lint: ## Run linter
	golangci-lint run ./...

.DEFAULT_GOAL := help
