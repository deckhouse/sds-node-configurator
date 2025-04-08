/*
Copyright 2025 Flant JSC

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

package test_utils

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
)

func NewFakeClient(statusSubresources ...client.Object) client.WithWatch {
	s := scheme.Scheme
	_ = metav1.AddMetaToScheme(s)
	_ = v1alpha1.AddToScheme(s)

	return fake.
		NewClientBuilder().
		WithScheme(s).
		WithStatusSubresource(statusSubresources...).
		Build()
}
