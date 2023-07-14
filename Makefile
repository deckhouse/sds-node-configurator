export NODE_NAME=test-node

APP-NAME=storage-configurator
REGISTRY=registry.flant.com/deckhouse/$(APP-NAME)
TEST-ARGS=-race -timeout 30s -count 1

run: ## run go
	go run -race ./cmd/bc/main.go

build: ## build docker image
	docker build -t $(REGISTRY):latest .
.PHONY: docker image build

test: ## run go tests
	go test $(TEST-ARGS) ./internal/...
.PHONY: test

test-cover: ## run go tests with total coverage calculating
	go test $(TEST-ARGS) ./internal/... -coverprofile cover.out
	go tool cover -func cover.out
	rm cover.out

lint: ## run linter for project
	golangci-lint run --timeout 10m
.PHONY: test

push: ## build docker image
	docker push  $(REGISTRY):latest
.PHONY: docker image push

remove: ## remove docker image
	docker image rm $(REGISTRY):latest
.PHONY: docker image remove


