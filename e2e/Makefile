# Makefile для E2E тестов sds-node-configurator (Common Scheduler Extender)

E2E_IMAGE ?= e2e-tests:latest

.PHONY: help
help: ## Показать справку
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: deps
deps: ## Установить зависимости
	go mod download
	go mod tidy
	@$(MAKE) fix-mod-permissions

# === Сборка образа ===

.PHONY: docker-build
docker-build: ## Собрать Docker образ с тестами
	docker build -t $(E2E_IMAGE) -f Dockerfile ..

.PHONY: docker-push
docker-push: ## Отправить образ в registry
	docker push $(E2E_IMAGE)

# === Запуск в кластере ===

.PHONY: deploy-rbac
deploy-rbac: ## Создать RBAC ресурсы в кластере
	kubectl apply -f manifests/rbac.yaml

.PHONY: run-in-cluster
run-in-cluster: deploy-rbac ## Запустить тесты в кластере как Job
	@kubectl delete job e2e-tests -n d8-sds-node-configurator --ignore-not-found
	E2E_IMAGE=$(E2E_IMAGE) \
	envsubst < manifests/job.yaml | kubectl apply -f -
	@echo "Job создан. Для просмотра логов: make logs"

.PHONY: logs
logs: ## Показать логи тестов
	kubectl logs -n d8-sds-node-configurator -l job-name=e2e-tests -f

.PHONY: status
status: ## Показать статус Job
	kubectl get job e2e-tests -n d8-sds-node-configurator
	@echo ""
	kubectl get pods -n d8-sds-node-configurator -l job-name=e2e-tests

.PHONY: cleanup
cleanup: ## Удалить Job и RBAC ресурсы
	kubectl delete -f manifests/job.yaml --ignore-not-found
	kubectl delete -f manifests/rbac.yaml --ignore-not-found

# === Локальный запуск ===

.PHONY: test
test: ## Запустить тесты локально (требуется kubeconfig и storage-e2e переменные окружения)
	@echo "Для локального запуска необходимо настроить переменные окружения из config/test_exports_storage_e2e"
	@echo "Запуск: source config/test_exports_storage_e2e && ginkgo -v --progress ./tests/"
	ginkgo -v --progress ./tests/

.PHONY: test-focus
test-focus: ## Запустить конкретный тест (make test-focus FOCUS="имя теста")
	@if [ -z "$(FOCUS)" ]; then \
		echo "Ошибка: укажите FOCUS=<имя теста>"; \
		exit 1; \
	fi
	ginkgo -v --progress --focus="$(FOCUS)" ./tests/

.PHONY: install-ginkgo
install-ginkgo: ## Установить Ginkgo CLI
	go install github.com/onsi/ginkgo/v2/ginkgo@latest

.PHONY: clean
clean: ## Очистить кэш тестов
	go clean -testcache

.PHONY: fix-mod-permissions
fix-mod-permissions: ## Исправить права на модуль storage-e2e (workaround для read-only GOPATH)
	@GOPATH=$${GOPATH:-$$HOME/go}; \
	for dir in $$GOPATH/pkg/mod/github.com/deckhouse/storage-e2e@*; do \
		if [ -d "$$dir" ]; then \
			echo "Fixing permissions for $$dir"; \
			chmod -R +w "$$dir" 2>/dev/null || true; \
			mkdir -p "$$dir/temp/cluster" 2>/dev/null || true; \
		fi; \
	done

.PHONY: check-env
check-env: ## Проверить переменные окружения
	@echo "Переменные окружения для E2E тестов:"
	@echo "  E2E_IMAGE:                 $(E2E_IMAGE)"
	@echo "  TEST_CLUSTER_CREATE_MODE:  $${TEST_CLUSTER_CREATE_MODE}"
	@echo "  TEST_CLUSTER_NAMESPACE:    $${TEST_CLUSTER_NAMESPACE}"
	@echo "  TEST_CLUSTER_STORAGE_CLASS:$${TEST_CLUSTER_STORAGE_CLASS}"
	@echo "  SSH_HOST:                  $${SSH_HOST}"
	@echo "  SSH_USER:                  $${SSH_USER}"
	@echo "  KUBE_CONFIG_PATH:          $${KUBE_CONFIG_PATH}"

.PHONY: lint
lint: ## Проверить код линтером
	golangci-lint run ./...

.DEFAULT_GOAL := help
