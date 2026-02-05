module github.com/deckhouse/sds-node-configurator/e2e

go 1.24.9

require (
	github.com/deckhouse/sds-node-configurator/api v0.0.0-20250130211935-b68366dfd0f8
	github.com/onsi/ginkgo/v2 v2.21.0
	github.com/onsi/gomega v1.35.1
	k8s.io/api v0.33.0
	k8s.io/apimachinery v0.33.0
	k8s.io/client-go v0.33.0
	sigs.k8s.io/controller-runtime v0.20.1
)

replace github.com/deckhouse/sds-node-configurator/api => ../api

