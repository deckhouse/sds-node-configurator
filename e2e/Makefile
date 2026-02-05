# Makefile для E2E тестов sds-node-configurator

.PHONY: help
help: ## Показать эту справку
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: deps
deps: ## Установить зависимости
	go mod download
	go mod tidy

.PHONY: test
test: ## Запустить все e2e тесты
	go test -v -timeout 15m ./tests/

.PHONY: test-verbose
test-verbose: ## Запустить тесты с подробным выводом
	go test -v -timeout 15m ./tests/ -ginkgo.v

.PHONY: test-focus
test-focus: ## Запустить конкретный тест (использование: make test-focus FOCUS="имя теста")
	@if [ -z "$(FOCUS)" ]; then \
		echo "Ошибка: укажите FOCUS=<имя теста>"; \
		exit 1; \
	fi
	go test -v -timeout 15m ./tests/ -ginkgo.focus="$(FOCUS)"

.PHONY: install-ginkgo
install-ginkgo: ## Установить Ginkgo CLI
	go install github.com/onsi/ginkgo/v2/ginkgo@latest

.PHONY: test-ginkgo
test-ginkgo: ## Запустить тесты через Ginkgo CLI
	ginkgo -v --progress ./tests/

.PHONY: test-watch
test-watch: ## Запустить тесты в режиме наблюдения (автоперезапуск при изменениях)
	ginkgo watch -v ./tests/

.PHONY: clean
clean: ## Очистить временные файлы
	go clean -testcache

.PHONY: check-env
check-env: ## Проверить переменные окружения для тестов
	@echo "Проверка переменных окружения для E2E тестов:"
	@echo "  E2E_NODE_NAME:      ${E2E_NODE_NAME}"
	@echo "  E2E_DEVICE_PATH:    ${E2E_DEVICE_PATH}"
	@echo "  E2E_DEVICE_SERIAL:  ${E2E_DEVICE_SERIAL}"
	@echo ""
	@if [ -z "${E2E_NODE_NAME}" ]; then \
		echo "⚠️  E2E_NODE_NAME не задан, будет использовано значение по умолчанию: worker-0"; \
	fi
	@if [ -z "${E2E_DEVICE_PATH}" ]; then \
		echo "⚠️  E2E_DEVICE_PATH не задан, будет использовано значение по умолчанию: /dev/vdb"; \
	fi
	@if [ -z "${E2E_DEVICE_SERIAL}" ]; then \
		echo "⚠️  E2E_DEVICE_SERIAL не задан (рекомендуется задать для проверки имени BlockDevice)"; \
	fi

.PHONY: lint
lint: ## Проверить код линтером
	golangci-lint run ./...

# Примеры использования с параметрами
.PHONY: example-test
example-test: ## Пример запуска теста с параметрами
	@echo "Запуск E2E теста с примером параметров:"
	E2E_NODE_NAME=worker-0 \
	E2E_DEVICE_PATH=/dev/vdb \
	E2E_DEVICE_SERIAL=TEST-001 \
	$(MAKE) test

.DEFAULT_GOAL := help

