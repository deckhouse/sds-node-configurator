export NODE_NAME=test-node
APP-NAME=storage-configurator

run: ## run go
	go run  ./cmd/bc/main.go

build: ## build docker image
	docker build -t $(APP-NAME):latest .
.PHONY: docker image build

remove: ## remove docker image
	docker image rm $(APP-NAME):latest
.PHONY: docker image remove
