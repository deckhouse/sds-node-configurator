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

package crdtests

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
)

func TestEnvtestLLVSCRDInstalled(t *testing.T) {
	ctx := context.Background()
	llvs := &v1alpha1.LVMLogicalVolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name: "envtest-smoke-llvs",
		},
		Spec: v1alpha1.LVMLogicalVolumeSnapshotSpec{
			LVMLogicalVolumeName: "envtest-smoke-llv",
		},
	}

	if err := k8sClient.Create(ctx, llvs); err != nil {
		t.Fatalf("Create LLVS without actualSnapshotNameOnTheNode: %v", err)
	}

	got := &v1alpha1.LVMLogicalVolumeSnapshot{}
	key := client.ObjectKey{Name: llvs.Name}
	if err := k8sClient.Get(ctx, key, got); err != nil {
		t.Fatalf("Get LLVS after Create: %v", err)
	}
	if got.Spec.LVMLogicalVolumeName != "envtest-smoke-llv" {
		t.Fatalf("unexpected lvmLogicalVolumeName: %q", got.Spec.LVMLogicalVolumeName)
	}

	if err := k8sClient.Delete(ctx, got); err != nil {
		t.Fatalf("Delete LLVS: %v", err)
	}
}
