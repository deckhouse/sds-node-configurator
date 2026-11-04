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

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
)

// The API server keeps a size given as a number as a number; the typed client
// has to read such objects back, one by one and in a list.
func TestEnvtestNumericSizeIsReadByTheTypedClient(t *testing.T) {
	ctx := context.Background()

	lvg := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "storage.deckhouse.io/v1alpha1",
		"kind":       "LVMVolumeGroup",
		"metadata":   map[string]any{"name": "envtest-numeric-size"},
		"spec": map[string]any{
			"type":                  "Local",
			"local":                 map[string]any{"nodeName": "node-a"},
			"actualVGNameOnTheNode": "vg-numeric",
			"blockDeviceSelector":   map[string]any{"matchLabels": map[string]any{"kubernetes.io/hostname": "node-a"}},
			"thinPools":             []any{map[string]any{"name": "tp", "size": int64(1073741824)}},
		},
	}}
	if err := k8sClient.Create(ctx, lvg); err != nil {
		t.Fatalf("create LVMVolumeGroup with a numeric thin pool size: %v", err)
	}
	t.Cleanup(func() { _ = k8sClient.Delete(context.Background(), lvg) })

	llv := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "storage.deckhouse.io/v1alpha1",
		"kind":       "LVMLogicalVolume",
		"metadata":   map[string]any{"name": "envtest-numeric-size"},
		"spec": map[string]any{
			"actualLVNameOnTheNode": "lv-numeric",
			"type":                  "Thin",
			"size":                  int64(536870912),
			"lvmVolumeGroupName":    "envtest-numeric-size",
			"thin":                  map[string]any{"poolName": "tp"},
		},
	}}
	if err := k8sClient.Create(ctx, llv); err != nil {
		t.Fatalf("create LVMLogicalVolume with a numeric size: %v", err)
	}
	t.Cleanup(func() { _ = k8sClient.Delete(context.Background(), llv) })

	var gotLVG v1alpha1.LVMVolumeGroup
	if err := k8sClient.Get(ctx, client.ObjectKey{Name: "envtest-numeric-size"}, &gotLVG); err != nil {
		t.Fatalf("get LVMVolumeGroup: %v", err)
	}
	if gotLVG.Spec.ThinPools[0].Size != "1073741824" {
		t.Fatalf("thin pool size = %q", gotLVG.Spec.ThinPools[0].Size)
	}
	var lvgs v1alpha1.LVMVolumeGroupList
	if err := k8sClient.List(ctx, &lvgs); err != nil {
		t.Fatalf("list LVMVolumeGroups: %v", err)
	}

	var gotLLV v1alpha1.LVMLogicalVolume
	if err := k8sClient.Get(ctx, client.ObjectKey{Name: "envtest-numeric-size"}, &gotLLV); err != nil {
		t.Fatalf("get LVMLogicalVolume: %v", err)
	}
	if gotLLV.Spec.Size != "536870912" {
		t.Fatalf("logical volume size = %q", gotLLV.Spec.Size)
	}
	var llvs v1alpha1.LVMLogicalVolumeList
	if err := k8sClient.List(ctx, &llvs); err != nil {
		t.Fatalf("list LVMLogicalVolumes: %v", err)
	}
}
