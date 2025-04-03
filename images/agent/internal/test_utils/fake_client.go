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
