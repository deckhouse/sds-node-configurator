# Makefile for sds-node-configurator E2E tests (Common Scheduler Extender + module scenarios)

E2E_IMAGE ?= e2e-tests:latest

.PHONY: help
help: ## Show help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: deps
deps: ## Download dependencies
	go mod download
	go mod tidy

# === Run in cluster (Job) ===

.PHONY: deploy-rbac
deploy-rbac: ## Apply RBAC manifests
	kubectl apply -f manifests/rbac.yaml

.PHONY: run-in-cluster
run-in-cluster: deploy-rbac ## Run tests as a Job in the cluster
	@kubectl delete job e2e-tests -n d8-sds-node-configurator --ignore-not-found
	E2E_IMAGE=$(E2E_IMAGE) \
	envsubst < manifests/job.yaml | kubectl apply -f -
	@echo "Job created. Logs: make logs"

.PHONY: logs
logs: ## Follow test pod logs
	kubectl logs -n d8-sds-node-configurator -l job-name=e2e-tests -f

.PHONY: status
status: ## Job status
	kubectl get job e2e-tests -n d8-sds-node-configurator
	@echo ""
	kubectl get pods -n d8-sds-node-configurator -l job-name=e2e-tests

.PHONY: cleanup
cleanup: ## Delete Job and RBAC
	kubectl delete -f manifests/job.yaml --ignore-not-found
	kubectl delete -f manifests/rbac.yaml --ignore-not-found

# === Local run ===
# CI smoke uses label e2e-tests (everything except max-VG stress). Override: make test-go GINKGO_LABEL_FILTER=stress-test
GINKGO_LABEL_FILTER ?= e2e-tests
E2E_TEST_TIMEOUT ?= 90m
# CI workflow uses go test -timeout 3h30m regardless of this var.

.PHONY: test
test: ## Run E2E smoke (label e2e-tests, без max-VG stress) — как CI
	$(MAKE) test-go

.PHONY: test-go
test-go: ## Как в CI smoke: TestSdsNodeConfigurator + -ginkgo.label-filter=e2e-tests
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

.PHONY: check-env
check-env: ## Print relevant env vars
	@echo "E2E_IMAGE:                 $(E2E_IMAGE)"
	@echo "TEST_CLUSTER_CREATE_MODE:  $${TEST_CLUSTER_CREATE_MODE}"
	@echo "TEST_CLUSTER_NAMESPACE:    $${TEST_CLUSTER_NAMESPACE}"
	@echo "TEST_CLUSTER_STORAGE_CLASS: $${TEST_CLUSTER_STORAGE_CLASS}"
	@echo "SSH_HOST:                  $${SSH_HOST}"
	@echo "SSH_USER:                  $${SSH_USER}"
	@echo "KUBE_CONFIG_PATH:          $${KUBE_CONFIG_PATH}"

.PHONY: lint
lint: ## Run golangci-lint
	golangci-lint run ./...

.DEFAULT_GOAL := help
