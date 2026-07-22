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

package tests

import (
	"context"
	"strings"
	"time"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// lvmVGNamePrefix matches all e2e-created LVMVolumeGroups (and LVMLogicalVolumes referencing them).
const lvmVGNamePrefix = "e2e-lvg-"

// forceDeleteAllNonConsumableBlockDevices removes finalizers and deletes every non-consumable BlockDevice CR,
// then waits until none remain.
func forceDeleteAllNonConsumableBlockDevices(ctx context.Context, cl client.Client, timeout time.Duration) {
	var bdList v1alpha1.BlockDeviceList
	if err := cl.List(ctx, &bdList, &client.ListOptions{}); err != nil {
		GinkgoWriter.Printf("Failed to list BlockDevices: %v\n", err)
		return
	}

	var toDelete []*v1alpha1.BlockDevice
	for i := range bdList.Items {
		bd := &bdList.Items[i]
		if !bd.Status.Consumable {
			toDelete = append(toDelete, bd)
		}
	}

	if len(toDelete) == 0 {
		GinkgoWriter.Println("No non-consumable BlockDevices found")
		return
	}

	GinkgoWriter.Printf("Force deleting %d non-consumable BlockDevices\n", len(toDelete))

	for _, bd := range toDelete {
		GinkgoWriter.Printf("  Removing finalizers and deleting BD %s (%s on %s, fsType=%s)\n",
			bd.Name, bd.Status.Path, bd.Status.NodeName, bd.Status.FsType)

		if len(bd.Finalizers) > 0 {
			bdCopy := bd.DeepCopy()
			bdCopy.Finalizers = nil
			if err := cl.Update(ctx, bdCopy); err != nil {
				GinkgoWriter.Printf("    Failed to remove finalizers: %v\n", err)
			}
		}

		if err := cl.Delete(ctx, bd); err != nil {
			GinkgoWriter.Printf("    Failed to delete: %v\n", err)
		}
	}

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if err := cl.List(ctx, &bdList, &client.ListOptions{}); err != nil {
			time.Sleep(5 * time.Second)
			continue
		}

		remaining := 0
		for i := range bdList.Items {
			if !bdList.Items[i].Status.Consumable {
				remaining++
			}
		}

		if remaining == 0 {
			GinkgoWriter.Println("All non-consumable BlockDevices deleted")
			return
		}

		GinkgoWriter.Printf("Waiting for %d non-consumable BlockDevices to be deleted...\n", remaining)
		time.Sleep(5 * time.Second)
	}
	GinkgoWriter.Println("Warning: timeout waiting for non-consumable BlockDevices deletion")
}

// forceDeleteAllBlockDevices removes finalizers and deletes every BlockDevice CR, then waits until none remain.
// Used after tests so the next Describe does not inherit orphan consumable BlockDevices.
func forceDeleteAllBlockDevices(ctx context.Context, cl client.Client, timeout time.Duration) {
	var bdList v1alpha1.BlockDeviceList
	if err := cl.List(ctx, &bdList, &client.ListOptions{}); err != nil {
		GinkgoWriter.Printf("forceDeleteAllBlockDevices: list failed: %v\n", err)
		return
	}
	if len(bdList.Items) == 0 {
		GinkgoWriter.Println("No BlockDevices to delete")
		return
	}
	GinkgoWriter.Printf("Force deleting %d BlockDevice(s)\n", len(bdList.Items))
	for i := range bdList.Items {
		bd := &bdList.Items[i]
		GinkgoWriter.Printf("  Removing finalizers and deleting BD %s (%s on %s)\n",
			bd.Name, bd.Status.Path, bd.Status.NodeName)
		if len(bd.Finalizers) > 0 {
			bdCopy := bd.DeepCopy()
			bdCopy.Finalizers = nil
			if err := cl.Update(ctx, bdCopy); err != nil {
				GinkgoWriter.Printf("    Failed to remove finalizers: %v\n", err)
			}
		}
		if err := cl.Delete(ctx, bd); err != nil {
			GinkgoWriter.Printf("    Failed to delete: %v\n", err)
		}
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if err := cl.List(ctx, &bdList, &client.ListOptions{}); err != nil {
			time.Sleep(5 * time.Second)
			continue
		}
		if len(bdList.Items) == 0 {
			GinkgoWriter.Println("All BlockDevices deleted")
			return
		}
		GinkgoWriter.Printf("Waiting for %d BlockDevice(s) to be gone...\n", len(bdList.Items))
		time.Sleep(5 * time.Second)
	}
	GinkgoWriter.Println("Warning: timeout waiting for all BlockDevices deletion")
}

// forceDeleteBlockDevicesByNames strips finalizers and deletes the named BlockDevice CRs (best effort).
func forceDeleteBlockDevicesByNames(ctx context.Context, cl client.Client, names []string) {
	if cl == nil || len(names) == 0 {
		return
	}
	for _, name := range names {
		bd := &v1alpha1.BlockDevice{}
		if err := cl.Get(ctx, client.ObjectKey{Name: name}, bd); err != nil {
			continue
		}
		GinkgoWriter.Printf("Deleting BlockDevice CR %s\n", name)
		if len(bd.Finalizers) > 0 {
			bd.Finalizers = nil
			if err := cl.Update(ctx, bd); err != nil {
				GinkgoWriter.Printf("  failed to strip finalizers on %s: %v\n", name, err)
			}
		}
		if err := cl.Delete(ctx, bd); err != nil {
			GinkgoWriter.Printf("  failed to delete BlockDevice %s: %v\n", name, err)
		}
	}
}

// cleanupLVMLogicalVolumes deletes LVMLogicalVolume CRs referencing e2e LVGs and waits for them to disappear.
// It does not strip finalizers: concurrent controller updates cause 409 conflicts; the agent completes removal
// once PVC/Pods are gone.
func cleanupLVMLogicalVolumes(ctx context.Context, cl client.Client) {
	var list v1alpha1.LVMLogicalVolumeList
	err := cl.List(ctx, &list, &client.ListOptions{})
	if err != nil {
		GinkgoWriter.Printf("List LVMLogicalVolumes failed (skip cleanup): %v\n", err)
		return
	}

	var toDelete []string
	for i := range list.Items {
		llv := &list.Items[i]
		if strings.HasPrefix(llv.Spec.LVMVolumeGroupName, lvmVGNamePrefix) {
			toDelete = append(toDelete, llv.Name)
		}
	}

	if len(toDelete) == 0 {
		return
	}

	GinkgoWriter.Printf("Deleting %d LVMLogicalVolume(s) referencing e2e LVGs\n", len(toDelete))
	for _, name := range toDelete {
		llv := &v1alpha1.LVMLogicalVolume{ObjectMeta: metav1.ObjectMeta{Name: name}}
		if err := cl.Delete(ctx, llv); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			GinkgoWriter.Printf("  Failed to delete LLV %s: %v\n", name, err)
		}
	}

	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		err := cl.List(ctx, &list, &client.ListOptions{})
		if err != nil {
			break
		}
		remaining := 0
		for i := range list.Items {
			if strings.HasPrefix(list.Items[i].Spec.LVMVolumeGroupName, lvmVGNamePrefix) {
				remaining++
			}
		}
		if remaining == 0 {
			GinkgoWriter.Println("All e2e LVMLogicalVolumes deleted")
			return
		}
		GinkgoWriter.Printf("Waiting for %d LVMLogicalVolumes to be deleted...\n", remaining)
		time.Sleep(5 * time.Second)
	}
	GinkgoWriter.Println("Warning: timeout waiting for LVMLogicalVolumes deletion")
}

// cleanupLVMVolumeGroups deletes e2e LVMVolumeGroup CRs, gives sds-node-configurator time to clean the VGs,
// then force-removes finalizers on any that remain stuck.
func cleanupLVMVolumeGroups(ctx context.Context, cl client.Client) {
	var list v1alpha1.LVMVolumeGroupList
	err := cl.List(ctx, &list, &client.ListOptions{})
	if err != nil {
		GinkgoWriter.Printf("List LVMVolumeGroups failed (skip cleanup): %v\n", err)
		return
	}
	var toDelete []string
	for i := range list.Items {
		if strings.HasPrefix(list.Items[i].Name, lvmVGNamePrefix) {
			toDelete = append(toDelete, list.Items[i].Name)
		}
	}
	if len(toDelete) == 0 {
		return
	}
	GinkgoWriter.Printf("Deleting %d LVMVolumeGroup(s): %v\n", len(toDelete), toDelete)
	for _, name := range toDelete {
		lvg := &v1alpha1.LVMVolumeGroup{}
		lvg.Name = name
		_ = cl.Delete(ctx, lvg)
	}

	GinkgoWriter.Println("Waiting for sds-node-configurator to cleanup VGs (up to 3 minutes)...")
	deadline := time.Now().Add(3 * time.Minute)
	forceRemoveAfter := time.Now().Add(2 * time.Minute)

	for time.Now().Before(deadline) {
		err := cl.List(ctx, &list, &client.ListOptions{})
		if err != nil {
			break
		}
		var remaining []string
		for i := range list.Items {
			if strings.HasPrefix(list.Items[i].Name, lvmVGNamePrefix) {
				remaining = append(remaining, list.Items[i].Name)
			}
		}
		if len(remaining) == 0 {
			GinkgoWriter.Println("All e2e LVMVolumeGroups removed (VGs cleaned by sds-node-configurator)")
			return
		}

		if time.Now().After(forceRemoveAfter) {
			GinkgoWriter.Printf("Force removing %d stuck LVMVolumeGroups\n", len(remaining))
			for _, name := range remaining {
				lvg := &v1alpha1.LVMVolumeGroup{}
				if err := cl.Get(ctx, client.ObjectKey{Name: name}, lvg); err != nil {
					continue
				}
				if len(lvg.Finalizers) > 0 {
					lvg.Finalizers = nil
					_ = cl.Update(ctx, lvg)
				}
			}
		}

		GinkgoWriter.Printf("Waiting for %d LVMVolumeGroups to be deleted...\n", len(remaining))
		time.Sleep(10 * time.Second)
	}
}
