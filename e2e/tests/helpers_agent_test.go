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
	"fmt"
	"time"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/e2e/tests/utils/consts"
	"github.com/deckhouse/storage-e2e/pkg/e2e"
	. "github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// lvmVolumeGroupReadyTimeout bounds convergence checks: LVMVolumeGroup Pending → Ready on busy CI
// can exceed 5m (agent + node LVM).
const lvmVolumeGroupReadyTimeout = 15 * time.Minute

// restartAgentOnNode deletes the sds-node-configurator-agent pod(s) scheduled on node and waits until a
// freshly created pod for that node is Running and Ready. It mirrors the "agent pod is restarted" pattern from
// block_device_stable_test.go but exposes it as a reusable, Gomega-free helper.
func restartAgentOnNode(ctx context.Context, cl *e2e.Cluster, node string) error {
	restartAt := time.Now()

	listOpts := metav1.ListOptions{
		LabelSelector: "app=" + consts.SdsNodeConfiguratorAgentName,
		FieldSelector: "spec.nodeName=" + node,
	}
	pods := cl.Clientset().CoreV1().Pods(consts.SdsNodeConfiguratorAgentNamespace)

	if err := pods.DeleteCollection(ctx, metav1.DeleteOptions{}, listOpts); err != nil {
		return fmt.Errorf("delete agent pods on node %s: %w", node, err)
	}

	deadline := time.Now().Add(5 * time.Minute)
	var lastErr error
	for {
		list, err := pods.List(ctx, listOpts)
		if err != nil {
			lastErr = err
		} else {
			ready := false
			for i := range list.Items {
				p := &list.Items[i]
				if p.DeletionTimestamp != nil {
					continue
				}
				if !p.CreationTimestamp.Time.After(restartAt) {
					continue
				}
				if p.Status.Phase == v1.PodRunning && isPodReady(p) {
					ready = true
					break
				}
			}
			if ready {
				return nil
			}
			lastErr = fmt.Errorf("no new ready agent pod on node %s yet", node)
		}

		if err := ctx.Err(); err != nil {
			return err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for agent pod restart on node %s: %w", node, lastErr)
		}
		time.Sleep(5 * time.Second)
	}
}

func isPodReady(pod *v1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == v1.PodReady && condition.Status == v1.ConditionTrue {
			return true
		}
	}
	return false
}

// managedStorageSnapshot captures BlockDevice/LVMVolumeGroup identity and status on a node
// before controller restart; used to assert no recreate/delete churn after the pod comes back.
type managedStorageSnapshot struct {
	NodeName               string
	BlockDeviceNamesOnNode map[string]types.UID
	BlockDeviceName        string
	BlockDeviceUID         types.UID
	BlockDeviceStatus      blockDeviceStatusSnapshot
	LVMVolumeGroupName     string
	LVMVolumeGroupUID      types.UID
	LVMVolumeGroupStatus   lvmVolumeGroupStatusSnapshot
}

type blockDeviceStatusSnapshot struct {
	Path                  string
	Size                  string
	Consumable            bool
	PVUuid                string
	LVMVolumeGroupName    string
	ActualVGNameOnTheNode string
}

type lvmVolumeGroupStatusSnapshot struct {
	Phase              string
	VGSize             string
	VGFree             string
	ThinPoolNamesReady map[string]bool
}

func blockDevicesOnNode(ctx context.Context, cl client.Client, nodeName string) (map[string]types.UID, error) {
	var list v1alpha1.BlockDeviceList
	if err := cl.List(ctx, &list, &client.ListOptions{}); err != nil {
		return nil, err
	}
	out := make(map[string]types.UID)
	for i := range list.Items {
		bd := &list.Items[i]
		if bd.Status.NodeName != nodeName || bd.DeletionTimestamp != nil {
			continue
		}
		out[bd.Name] = bd.UID
	}
	return out, nil
}

func takeManagedStorageSnapshot(ctx context.Context, cl client.Client, nodeName, blockDeviceName, lvgName string) (managedStorageSnapshot, error) {
	bdsOnNode, err := blockDevicesOnNode(ctx, cl, nodeName)
	if err != nil {
		return managedStorageSnapshot{}, err
	}

	var bd v1alpha1.BlockDevice
	if err := cl.Get(ctx, client.ObjectKey{Name: blockDeviceName}, &bd); err != nil {
		return managedStorageSnapshot{}, err
	}

	var lvg v1alpha1.LVMVolumeGroup
	if err := cl.Get(ctx, client.ObjectKey{Name: lvgName}, &lvg); err != nil {
		return managedStorageSnapshot{}, err
	}

	thinReady := make(map[string]bool, len(lvg.Status.ThinPools))
	for i := range lvg.Status.ThinPools {
		tp := lvg.Status.ThinPools[i]
		thinReady[tp.Name] = tp.Ready
	}

	return managedStorageSnapshot{
		NodeName:               nodeName,
		BlockDeviceNamesOnNode: bdsOnNode,
		BlockDeviceName:        bd.Name,
		BlockDeviceUID:         bd.UID,
		BlockDeviceStatus: blockDeviceStatusSnapshot{
			Path:                  bd.Status.Path,
			Size:                  bd.Status.Size.String(),
			Consumable:            bd.Status.Consumable,
			PVUuid:                bd.Status.PVUuid,
			LVMVolumeGroupName:    bd.Status.LVMVolumeGroupName,
			ActualVGNameOnTheNode: bd.Status.ActualVGNameOnTheNode,
		},
		LVMVolumeGroupName: lvg.Name,
		LVMVolumeGroupUID:  lvg.UID,
		LVMVolumeGroupStatus: lvmVolumeGroupStatusSnapshot{
			Phase:              lvg.Status.Phase,
			VGSize:             lvg.Status.VGSize.String(),
			VGFree:             lvg.Status.VGFree.String(),
			ThinPoolNamesReady: thinReady,
		},
	}, nil
}

func expectManagedStorageStableAfterRestart(ctx context.Context, cl client.Client, before managedStorageSnapshot) {
	Eventually(func(g Gomega) {
		afterBDs, err := blockDevicesOnNode(ctx, cl, before.NodeName)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(afterBDs).To(Equal(before.BlockDeviceNamesOnNode),
			"BlockDevice set on node %s must not change (no unexpected create/delete)", before.NodeName)

		var bd v1alpha1.BlockDevice
		g.Expect(cl.Get(ctx, client.ObjectKey{Name: before.BlockDeviceName}, &bd)).To(Succeed())
		g.Expect(bd.UID).To(Equal(before.BlockDeviceUID), "BlockDevice %s must not be recreated", before.BlockDeviceName)
		g.Expect(bd.DeletionTimestamp).To(BeNil())
		g.Expect(bd.Status.Path).To(Equal(before.BlockDeviceStatus.Path))
		g.Expect(bd.Status.Size.String()).To(Equal(before.BlockDeviceStatus.Size))
		g.Expect(bd.Status.Consumable).To(Equal(before.BlockDeviceStatus.Consumable))
		g.Expect(bd.Status.PVUuid).To(Equal(before.BlockDeviceStatus.PVUuid))
		g.Expect(bd.Status.LVMVolumeGroupName).To(Equal(before.BlockDeviceStatus.LVMVolumeGroupName))
		g.Expect(bd.Status.ActualVGNameOnTheNode).To(Equal(before.BlockDeviceStatus.ActualVGNameOnTheNode))

		var lvg v1alpha1.LVMVolumeGroup
		g.Expect(cl.Get(ctx, client.ObjectKey{Name: before.LVMVolumeGroupName}, &lvg)).To(Succeed())
		g.Expect(lvg.UID).To(Equal(before.LVMVolumeGroupUID), "LVMVolumeGroup %s must not be recreated", before.LVMVolumeGroupName)
		g.Expect(lvg.DeletionTimestamp).To(BeNil())
		g.Expect(lvg.Status.Phase).To(Equal(v1alpha1.PhaseReady), "LVMVolumeGroup phase should converge to Ready")
		g.Expect(lvg.Status.VGSize.String()).To(Equal(before.LVMVolumeGroupStatus.VGSize))
		g.Expect(lvg.Status.VGFree.String()).To(Equal(before.LVMVolumeGroupStatus.VGFree))

		g.Expect(len(lvg.Status.ThinPools)).To(Equal(len(before.LVMVolumeGroupStatus.ThinPoolNamesReady)),
			"thin-pool count must stay the same")
		for name, wasReady := range before.LVMVolumeGroupStatus.ThinPoolNamesReady {
			var found *v1alpha1.LVMVolumeGroupThinPoolStatus
			for i := range lvg.Status.ThinPools {
				if lvg.Status.ThinPools[i].Name == name {
					found = &lvg.Status.ThinPools[i]
					break
				}
			}
			g.Expect(found).NotTo(BeNil(), "thin-pool %q missing from status after restart", name)
			g.Expect(found.Ready).To(Equal(wasReady), "thin-pool %q Ready flag changed unexpectedly", name)
		}

		for _, c := range lvg.Status.Conditions {
			g.Expect(c.Status).NotTo(Equal(metav1.ConditionFalse),
				"condition %s has status False after restart: reason=%s message=%s", c.Type, c.Reason, c.Message)
		}
	}, lvmVolumeGroupReadyTimeout, 8*time.Second).Should(Succeed())
}
