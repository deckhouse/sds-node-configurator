package controller_test

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"storage-configurator/api/v1alpha1"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestController(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Controller Suite")
}

func NewFakeClient() client.WithWatch {
	//var objs []runtime.Object
	//objs = append(objs, objects...)

	s := scheme.Scheme
	_ = metav1.AddMetaToScheme(s)
	_ = v1alpha1.AddToScheme(s)

	builder := fake.NewClientBuilder().
		WithScheme(s)
	//WithRuntimeObjects(objs...)

	cl := builder.Build()
	return cl
}
