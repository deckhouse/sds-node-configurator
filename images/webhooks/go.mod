module github.com/deckhouse/sds-node-configurator/images/webhooks

go 1.24.5

require (
	github.com/deckhouse/sds-node-configurator/api v0.0.0-20250206203415-a9ffd855f5a3
	github.com/deckhouse/sds-node-configurator/lib/go/common v0.0.0-00010101000000-000000000000
	github.com/sirupsen/logrus v1.9.3
	github.com/slok/kubewebhook/v2 v2.7.0
	k8s.io/apimachinery v0.33.0
)

replace github.com/deckhouse/sds-node-configurator/api => ../../api

replace github.com/deckhouse/sds-node-configurator/lib/go/common => ../../lib/go/common

require (
	github.com/fxamacker/cbor/v2 v2.7.0 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/x448/float16 v0.8.4 // indirect
	golang.org/x/net v0.39.0 // indirect
	golang.org/x/sys v0.32.0 // indirect
	golang.org/x/text v0.24.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	k8s.io/api v0.33.0 // indirect
	k8s.io/client-go v0.32.1 // indirect
	k8s.io/klog/v2 v2.130.1 // indirect
	k8s.io/utils v0.0.0-20241210054802-24370beab758 // indirect
	sigs.k8s.io/json v0.0.0-20241014173422-cfa47c3a1cc8 // indirect
	sigs.k8s.io/randfill v1.0.0 // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.6.0 // indirect
	sigs.k8s.io/yaml v1.4.0 // indirect
)
