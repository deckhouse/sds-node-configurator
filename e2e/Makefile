# Makefile for sds-node-configurator E2E tests (Common Scheduler Extender + module scenarios)

E2E_IMAGE ?= e2e-tests:latest

.PHONY: help
help: ## Show help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: deps
deps: ## Download dependencies
	go mod download
	go mod tidy
	@$(MAKE) fix-mod-permissions

# === Docker image ===

.PHONY: docker-build
docker-build: ## Build Docker image with tests (context: repo root)
	docker build -t $(E2E_IMAGE) -f Dockerfile ..

.PHONY: docker-push
docker-push: ## Push image to registry
	docker push $(E2E_IMAGE)

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

.PHONY: test
test: ## Run full E2E suite (TestE2E — все Ginkgo-спеки в пакете)
	go test -v -count=1 -timeout 60m ./tests/

.PHONY: test-go
test-go: ## Как в CI smoke: только TestE2E (Common Scheduler, затем Sds Node Configurator)
	go test -v -count=1 -timeout 60m ./tests/ -run '^TestE2E$$'

.PHONY: test-focus
test-focus: ## Run by test name (make test-focus FOCUS="TestE2E")
	@if [ -z "$(FOCUS)" ]; then \
		echo "Error: specify FOCUS=<test name>"; \
		exit 1; \
	fi
	go test -v -count=1 -timeout 60m ./tests/ -run "$(FOCUS)"

.PHONY: install-ginkgo
install-ginkgo: ## Install Ginkgo CLI
	go install github.com/onsi/ginkgo/v2/ginkgo@latest

.PHONY: clean
clean: ## Clean test cache
	go clean -testcache

.PHONY: fix-mod-permissions
fix-mod-permissions: ## Workaround: writable storage-e2e module in GOPATH
	@GOPATH=$${GOPATH:-$$HOME/go}; \
	for dir in $$GOPATH/pkg/mod/github.com/deckhouse/storage-e2e@*; do \
		if [ -d "$$dir" ]; then \
			echo "Fixing permissions for $$dir"; \
			chmod -R +w "$$dir" 2>/dev/null || true; \
			mkdir -p "$$dir/temp/cluster" 2>/dev/null || true; \
		fi; \
	done

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
