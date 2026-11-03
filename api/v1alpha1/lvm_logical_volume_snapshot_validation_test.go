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

package v1alpha1_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
)

const sdsNodeConfiguratorFinalizer = "storage.deckhouse.io/sds-node-configurator"

func TestLLVSCreateWithoutActualSnapshotName(t *testing.T) {
	llvs := createLLVS(t, "llvs-create-omit-actual", v1alpha1.LVMLogicalVolumeSnapshotSpec{
		LVMLogicalVolumeName: "src-llv",
	})
	t.Logf("Create without actualSnapshotNameOnTheNode: Accepted")
	t.Logf("spec JSON (omitempty): %s", mustJSON(t, llvs.Spec))
}

func TestLLVSUpdateAddFinalizerWithEmptyActual(t *testing.T) {
	ctx := context.Background()
	name := "llvs-update-finalizer-empty-actual"
	_ = createLLVS(t, name, v1alpha1.LVMLogicalVolumeSnapshotSpec{
		LVMLogicalVolumeName: "src-llv",
	})
	t.Logf("Create without actual: Accepted")

	got := getLLVS(t, name)
	got.Finalizers = append(got.Finalizers, sdsNodeConfiguratorFinalizer)
	got.Spec.ActualSnapshotNameOnTheNode = ""
	t.Logf("before Update: finalizers=%v spec JSON=%s", got.Finalizers, mustJSON(t, got.Spec))

	if err := k8sClient.Update(ctx, got); err != nil {
		t.Fatalf("Update with finalizer and empty actual (regression for minLength reject): %v", err)
	}
	t.Logf("Update (finalizer + empty actual): Accepted — omitempty/minLength contract OK")

	after := getLLVS(t, name)
	if len(after.Finalizers) == 0 || after.Finalizers[0] != sdsNodeConfiguratorFinalizer {
		t.Fatalf("finalizer not persisted: %#v", after.Finalizers)
	}
	t.Logf("after Get: finalizers=%v actual=%q", after.Finalizers, after.Spec.ActualSnapshotNameOnTheNode)
}

func TestLLVSActualSnapshotNameImmutable(t *testing.T) {
	ctx := context.Background()
	name := "llvs-actual-immutable"
	_ = createLLVS(t, name, v1alpha1.LVMLogicalVolumeSnapshotSpec{
		LVMLogicalVolumeName:        "src-llv",
		ActualSnapshotNameOnTheNode: "snap-a",
	})
	t.Logf("Create with actual=snap-a: Accepted")

	got := getLLVS(t, name)
	got.Spec.ActualSnapshotNameOnTheNode = "snap-b"
	t.Logf("trying Update actual snap-a -> snap-b")

	err := k8sClient.Update(ctx, got)
	if err == nil {
		t.Fatal("expected Reject when changing actualSnapshotNameOnTheNode, got success")
	}
	if !apierrors.IsInvalid(err) && !apierrors.IsBadRequest(err) {
		t.Fatalf("expected Invalid/BadRequest, got %T: %v", err, err)
	}
	if !strings.Contains(strings.ToLower(err.Error()), "immutable") &&
		!strings.Contains(err.Error(), "actualSnapshotNameOnTheNode") {
		t.Fatalf("error should mention immutable / field name, got: %v", err)
	}
	t.Logf("Update rename: Rejected as expected: %v", err)
}

func TestLLVSCreateWithExplicitEmptyActual(t *testing.T) {
	llvs := createLLVS(t, "llvs-create-empty-actual", v1alpha1.LVMLogicalVolumeSnapshotSpec{
		LVMLogicalVolumeName:        "src-llv",
		ActualSnapshotNameOnTheNode: "",
	})
	t.Logf("Create with explicit actual=\"\": Accepted")
	t.Logf("spec JSON (empty string omitted by omitempty): %s", mustJSON(t, llvs.Spec))
}

func createLLVS(t *testing.T, name string, spec v1alpha1.LVMLogicalVolumeSnapshotSpec) *v1alpha1.LVMLogicalVolumeSnapshot {
	t.Helper()
	llvs := &v1alpha1.LVMLogicalVolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       spec,
	}
	t.Cleanup(func() { deleteLLVS(t, name) })

	if err := k8sClient.Create(context.Background(), llvs); err != nil {
		t.Fatalf("Create LLVS %q: %v", name, err)
	}
	return llvs
}

func getLLVS(t *testing.T, name string) *v1alpha1.LVMLogicalVolumeSnapshot {
	t.Helper()
	got := &v1alpha1.LVMLogicalVolumeSnapshot{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Name: name}, got); err != nil {
		t.Fatalf("Get LLVS %q: %v", name, err)
	}
	return got
}

func deleteLLVS(t *testing.T, name string) {
	t.Helper()
	ctx := context.Background()
	cur := &v1alpha1.LVMLogicalVolumeSnapshot{}
	key := client.ObjectKey{Name: name}
	if err := k8sClient.Get(ctx, key, cur); err != nil {
		return
	}
	if len(cur.Finalizers) > 0 {
		cur.Finalizers = nil
		_ = k8sClient.Update(ctx, cur)
	}
	_ = k8sClient.Delete(ctx, cur)
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	return string(b)
}
