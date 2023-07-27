package controller_test

import (
	"context"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"storage-configurator/api/v1alpha1"
)

var _ = Describe("Contriller", func() {

	It("Create ListDevice Object", func() {
		ctx := context.Background()

		listDevice := &v1alpha1.BlockDeviceList{
			TypeMeta: metav1.TypeMeta{
				Kind:       v1alpha1.OwnerReferencesKind,
				APIVersion: v1alpha1.TypeMediaAPIVersion,
			},
			ListMeta: metav1.ListMeta{},
			Items:    []v1alpha1.BlockDevice{},
		}

		cl := NewFakeClient(listDevice)
		err := cl.List(ctx, listDevice)
		Expect(err).NotTo(HaveOccurred())
	})

})

func NewFakeClient(objects ...runtime.Object) client.WithWatch {
	var objs []runtime.Object
	objs = append(objs, objects...)

	s := scheme.Scheme
	_ = metav1.AddMetaToScheme(s)
	_ = v1alpha1.AddToScheme(s)

	builder := fake.NewClientBuilder().
		WithScheme(s).
		WithRuntimeObjects(objs...)

	cl := builder.Build()
	return cl
}
