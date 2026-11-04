/*
Copyright 2026 Flant JSC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package crdtests checks what kube-apiserver admits and rejects once the
// generated crds/ are installed. It is a module of its own so that envtest, the
// controller-runtime client and sds-common-lib stay out of the requirements of
// the api module, which other modules import.
package crdtests

import (
	"os"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-common-lib/crdtest"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
)

var k8sClient client.Client

func TestMain(m *testing.M) {
	os.Exit(crdtest.Main(m, crdtest.Options{
		CRDPaths:    []string{"../../crds"},
		AddToScheme: []func(*runtime.Scheme) error{v1alpha1.AddToScheme},
	}, func(env *crdtest.Env) { k8sClient = env.Client }))
}
