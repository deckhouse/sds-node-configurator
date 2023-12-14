export NODE_NAME=test-node

APP-NAME=sds-node-configurator
REGISTRY=registry.flant.com/deckhouse/storage/$(APP-NAME)
TEST-ARGS=-race -timeout 30s -count 1
USER := $(shell whoami)
DAEMONSET_NAME=sds-node-configurator
NAMESPACE=d8-sds-node-configurator
SYSTEM_NAMESPACE=d8-system
PULL_SECRET_NAME=fox-registry
CONTAINER_NAME=sds-node-configurator-agent

run: ## run go
	go run -race ./cmd/bc/main.go

build: ## build docker image
	docker build -t $(REGISTRY):latest .
.PHONY: docker image build

test: ## run go tests
	go test $(TEST-ARGS) ./...
.PHONY: test

test-cover: ## run go tests with total coverage calculating
	go test $(TEST-ARGS) ./... -coverprofile cover.out
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


switch_to_local_dev:
	kubectl -n $(SYSTEM_NAMESPACE) scale deployment deckhouse --replicas=0
	kubectl -n $(NAMESPACE) patch daemonset $(DAEMONSET_NAME) -p \
		'{"spec": {"template": {"spec": {"containers": [{"name": "$(CONTAINER_NAME)", "image": "$(REGISTRY):$(USER)"}]}}}}'
	kubectl  -n $(NAMESPACE) patch daemonset $(DAEMONSET_NAME) --type='json' \
		-p='[{"op": "add", "path": "/spec/template/spec/imagePullSecrets", "value": [{"name": "$(PULL_SECRET_NAME)"}]}]'
.PHONY: switch_to_local_dev

fox_build_and_push:
	docker build . -t $(REGISTRY):$(USER) -f ./dev/Dockerfile
	docker push $(REGISTRY):$(USER)
.PHONY: fox_build_and_push

switch_to_remote_dev:
	kubectl -n $(SYSTEM_NAMESPACE) scale deployment deckhouse --replicas=1

.PHONY: switch_to_remote_dev

redeploy:
	kubectl -n $(NAMESPACE) rollout restart daemonset $(DAEMONSET_NAME)
.PHONY: redeploy
