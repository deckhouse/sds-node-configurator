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

// Tier A — pending upstream: storage-e2e

package framework

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

// virtualDiskGVR identifies the Deckhouse virtualization VirtualDisk resource.
var virtualDiskGVR = schema.GroupVersionResource{
	Group:    "virtualization.deckhouse.io",
	Version:  "v1alpha2",
	Resource: "virtualdisks",
}

// ResizeDisk locates a VirtualDisk by name across all namespaces and patches spec.persistentVolumeClaim.size.
// pending upstream: replace with cl.Disks().ResizeDisk after storage-e2e bump (feat/disk-manager-resize).
func ResizeDisk(ctx context.Context, dyn dynamic.Interface, diskName, newSize string) error {
	if _, err := resource.ParseQuantity(newSize); err != nil {
		return fmt.Errorf("parse disk size %q: %w", newSize, err)
	}

	list, err := dyn.Resource(virtualDiskGVR).Namespace(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list VirtualDisks: %w", err)
	}

	var target *unstructured.Unstructured
	for i := range list.Items {
		if list.Items[i].GetName() == diskName {
			target = &list.Items[i]
			break
		}
	}
	if target == nil {
		return fmt.Errorf("VirtualDisk %q not found in any namespace", diskName)
	}

	if err := unstructured.SetNestedField(target.Object, newSize, "spec", "persistentVolumeClaim", "size"); err != nil {
		return fmt.Errorf("set VirtualDisk %q size: %w", diskName, err)
	}

	if _, err := dyn.Resource(virtualDiskGVR).Namespace(target.GetNamespace()).Update(ctx, target, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("update VirtualDisk %q size to %s: %w", diskName, newSize, err)
	}
	return nil
}
