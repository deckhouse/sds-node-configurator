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

package hooks_common

import (
	"context"
	"errors"
	"testing"

	extv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/deckhouse/deckhouse/pkg/log"
	"github.com/deckhouse/sds-node-configurator/hooks/go/consts"
)

// log.NewNop() returns a *log.Logger, which satisfies pkg.Logger.
var testLogger = log.NewNop()

const migrationFinalizer = "storage.deckhouse.io/sds-node-configurator"

var backupGVK = schema.GroupVersionKind{
	Group:   consts.APIGroup,
	Version: "v1alpha1",
	Kind:    consts.ObsoleteLVMVolumeGroupBackupKind,
}

func newScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = extv1.AddToScheme(scheme)
	return scheme
}

func backupCRD() *extv1.CustomResourceDefinition {
	return &extv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: consts.ObsoleteLVMVolumeGroupBackupCRDName},
	}
}

func backup(name string, finalizers ...string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{Object: map[string]any{}}
	obj.SetGroupVersionKind(backupGVK)
	obj.SetName(name)
	if len(finalizers) > 0 {
		obj.SetFinalizers(finalizers)
	}
	return obj
}

func getBackup(t *testing.T, cl client.Client, name string) *unstructured.Unstructured {
	t.Helper()
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(backupGVK)
	if err := cl.Get(context.Background(), client.ObjectKey{Name: name}, obj); err != nil {
		t.Fatalf("get %s %s: %v", backupGVK.Kind, name, err)
	}
	return obj
}

func assertBackupCRDGone(t *testing.T, cl client.Client) {
	t.Helper()
	name := consts.ObsoleteLVMVolumeGroupBackupCRDName
	got := &extv1.CustomResourceDefinition{}
	if err := cl.Get(context.Background(), client.ObjectKey{Name: name}, got); !apierrors.IsNotFound(err) {
		t.Errorf("CRD %s should be deleted, got err=%v", name, err)
	}
}

func assertBackupCRDPresent(t *testing.T, cl client.Client) {
	t.Helper()
	name := consts.ObsoleteLVMVolumeGroupBackupCRDName
	got := &extv1.CustomResourceDefinition{}
	if err := cl.Get(context.Background(), client.ObjectKey{Name: name}, got); err != nil {
		t.Errorf("CRD %s should still be present, got err=%v", name, err)
	}
}

// TestRemoveObsoleteCRDs_NoCRD covers the common case in any cluster that never went through
// the LvmVolumeGroup -> LVMVolumeGroup migration, plus every converge after the first
// successful cleanup: the hook must be a silent no-op, not an error.
func TestRemoveObsoleteCRDs_NoCRD(t *testing.T) {
	cl := fake.NewClientBuilder().WithScheme(newScheme()).Build()
	removeObsoleteCRDs(context.Background(), cl, testLogger)
	assertBackupCRDGone(t, cl)
}

// TestRemoveObsoleteCRDs_StripsAndDeletes is the migrated-cluster path: leftover backups carry
// the finalizer the migration copied from the LVG, and no controller removes it anymore, so
// they must be stripped before the CRD is deleted.
func TestRemoveObsoleteCRDs_StripsAndDeletes(t *testing.T) {
	withFinalizer := backup("vg-1", migrationFinalizer)
	withExtraFinalizers := backup("vg-2", migrationFinalizer, "example.com/other")
	clean := backup("vg-3")

	cl := fake.NewClientBuilder().WithScheme(newScheme()).
		WithObjects(backupCRD(), withFinalizer, withExtraFinalizers, clean).
		Build()

	removeObsoleteCRDs(context.Background(), cl, testLogger)

	// The fake client does not cascade CRD deletion, so the objects are still readable and can
	// be asserted on directly. Every finalizer must be gone, not just the migration one: the
	// kind is going away, so nothing would ever process any of them.
	for _, name := range []string{"vg-1", "vg-2", "vg-3"} {
		if got := getBackup(t, cl, name).GetFinalizers(); len(got) != 0 {
			t.Errorf("%s finalizers = %v, want none", name, got)
		}
	}

	assertBackupCRDGone(t, cl)

	// Idempotent: a second converge with the CRD gone must stay a no-op.
	removeObsoleteCRDs(context.Background(), cl, testLogger)
}

// TestRemoveObsoleteCRDs_StripFailureKeepsCRD guards the ordering contract: deleting the CRD
// while a leftover still holds a finalizer would wedge it in Terminating forever, since no
// controller is left to release it.
func TestRemoveObsoleteCRDs_StripFailureKeepsCRD(t *testing.T) {
	cl := fake.NewClientBuilder().WithScheme(newScheme()).
		WithObjects(backupCRD(), backup("vg-1", migrationFinalizer)).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(_ context.Context, _ client.WithWatch, _ client.Object, _ client.Patch, _ ...client.PatchOption) error {
				return errors.New("patch boom")
			},
		}).
		Build()

	removeObsoleteCRDs(context.Background(), cl, testLogger)

	assertBackupCRDPresent(t, cl)
	if got := getBackup(t, cl, "vg-1").GetFinalizers(); len(got) != 1 {
		t.Errorf("vg-1 finalizers = %v, want the finalizer kept after a failed patch", got)
	}
}

// TestRemoveObsoleteCRDs_ListFailureKeepsCRD: an unreadable leftover set says nothing about
// whether finalizers are held, so the CRD must survive until the next converge can check.
func TestRemoveObsoleteCRDs_ListFailureKeepsCRD(t *testing.T) {
	cl := fake.NewClientBuilder().WithScheme(newScheme()).
		WithObjects(backupCRD()).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(_ context.Context, _ client.WithWatch, _ client.ObjectList, _ ...client.ListOption) error {
				return errors.New("list boom")
			},
		}).
		Build()

	removeObsoleteCRDs(context.Background(), cl, testLogger)

	assertBackupCRDPresent(t, cl)
}

// TestRemoveObsoleteCRDs_NoMatchStillDeletes: if the apiserver serves no such kind, no object
// of it can be reached or created, so there is nothing to strip and the CRD may go.
func TestRemoveObsoleteCRDs_NoMatchStillDeletes(t *testing.T) {
	cl := fake.NewClientBuilder().WithScheme(newScheme()).
		WithObjects(backupCRD()).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(_ context.Context, _ client.WithWatch, _ client.ObjectList, _ ...client.ListOption) error {
				return &meta.NoKindMatchError{GroupKind: backupGVK.GroupKind(), SearchedVersions: []string{backupGVK.Version}}
			},
		}).
		Build()

	removeObsoleteCRDs(context.Background(), cl, testLogger)

	assertBackupCRDGone(t, cl)
}

// TestRemoveObsoleteCRDs_ConcurrentObjectDeletionTolerated: a leftover vanishing mid-sweep
// (hand-deleted, or the cascade of a concurrent CRD delete) already satisfies the goal, so it
// must not hold the CRD back.
func TestRemoveObsoleteCRDs_ConcurrentObjectDeletionTolerated(t *testing.T) {
	cl := fake.NewClientBuilder().WithScheme(newScheme()).
		WithObjects(backupCRD(), backup("vg-1", migrationFinalizer)).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(_ context.Context, _ client.WithWatch, obj client.Object, _ client.Patch, _ ...client.PatchOption) error {
				return apierrors.NewNotFound(schema.GroupResource{
					Group:    backupGVK.Group,
					Resource: "lvmvolumegroupbackups",
				}, obj.GetName())
			},
		}).
		Build()

	removeObsoleteCRDs(context.Background(), cl, testLogger)

	assertBackupCRDGone(t, cl)
}
