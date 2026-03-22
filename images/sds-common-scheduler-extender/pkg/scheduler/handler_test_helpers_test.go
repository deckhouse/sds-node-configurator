/*
Copyright 2025 Flant JSC

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

package scheduler

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	snc "github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/cache"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/consts"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
	srv "github.com/deckhouse/sds-replicated-volume/api/v1alpha1"
)

func init() {
	_ = snc.AddToScheme(scheme.Scheme)
	_ = srv.AddToScheme(scheme.Scheme)
	_ = corev1.AddToScheme(scheme.Scheme)
	_ = storagev1.AddToScheme(scheme.Scheme)
}

func newTestScheduler(cl client.Client, c *cache.Cache) *scheduler {
	log, _ := logger.NewLogger("0")
	return &scheduler{
		client:         cl,
		ctx:            context.Background(),
		log:            log,
		cache:          c,
		defaultDivisor: 1.0,
	}
}

func readyLVG(name string, vgSize, vgFree int64) *snc.LVMVolumeGroup {
	return &snc.LVMVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: snc.LVMVolumeGroupStatus{
			Phase:  snc.PhaseReady,
			VGSize: *resource.NewQuantity(vgSize, resource.BinarySI),
			VGFree: *resource.NewQuantity(vgFree, resource.BinarySI),
		},
	}
}

func readyLVGWithThinPool(name string, tpAllocated, tpAvailable int64) *snc.LVMVolumeGroup {
	return &snc.LVMVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: snc.LVMVolumeGroupStatus{
			Phase:  snc.PhaseReady,
			VGSize: *resource.NewQuantity(hundredGiB, resource.BinarySI),
			VGFree: *resource.NewQuantity(0, resource.BinarySI),
			ThinPools: []snc.LVMVolumeGroupThinPoolStatus{
				{
					Name:           "tp0",
					AllocatedSize:  *resource.NewQuantity(tpAllocated, resource.BinarySI),
					AvailableSpace: *resource.NewQuantity(tpAvailable, resource.BinarySI),
				},
			},
		},
	}
}

func notReadyLVG(name string, phase string) *snc.LVMVolumeGroup {
	return &snc.LVMVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: snc.LVMVolumeGroupStatus{
			Phase:  phase,
			VGSize: *resource.NewQuantity(100*1024*1024*1024, resource.BinarySI),
			VGFree: *resource.NewQuantity(50*1024*1024*1024, resource.BinarySI),
		},
	}
}

func newTestCache() *cache.Cache {
	log, _ := logger.NewLogger("0")
	return cache.NewCache(log, time.Hour)
}

func thickLLV(name, lvgName, size, phase string) *snc.LVMLogicalVolume {
	llv := &snc.LVMLogicalVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: snc.LVMLogicalVolumeSpec{
			LVMVolumeGroupName: lvgName,
			Size:               size,
			Type:               "Thick",
		},
	}
	if phase != "" {
		llv.Status = &snc.LVMLogicalVolumeStatus{Phase: phase}
	}
	return llv
}

func thinLLV(name, thinPoolName, size, phase string) *snc.LVMLogicalVolume {
	llv := &snc.LVMLogicalVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: snc.LVMLogicalVolumeSpec{
			LVMVolumeGroupName: "lvg1",
			Size:               size,
			Type:               "Thin",
			Thin:               &snc.LVMLogicalVolumeThinSpec{PoolName: thinPoolName},
		},
	}
	if phase != "" {
		llv.Status = &snc.LVMLogicalVolumeStatus{Phase: phase}
	}
	return llv
}

func newFakeClient(objects ...client.Object) client.Client {
	return fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(objects...).
		WithIndex(&snc.LVMLogicalVolume{}, IndexFieldLLVLVGName, func(obj client.Object) []string {
			llv := obj.(*snc.LVMLogicalVolume)
			return []string{llv.Spec.LVMVolumeGroupName}
		}).
		WithIndex(&snc.LVMVolumeGroup{}, IndexFieldLVGNodeName, func(obj client.Object) []string {
			lvg := obj.(*snc.LVMVolumeGroup)
			var nodes []string
			for _, n := range lvg.Status.Nodes {
				nodes = append(nodes, n.Name)
			}
			return nodes
		}).
		Build()
}

func testRSC(name string, volumeAccess srv.ReplicatedStorageClassVolumeAccess, storagePoolName string) *srv.ReplicatedStorageClass { //nolint:unparam
	return &srv.ReplicatedStorageClass{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: srv.ReplicatedStorageClassSpec{
			VolumeAccess: volumeAccess,
		},
		Status: srv.ReplicatedStorageClassStatus{
			StoragePoolName: storagePoolName,
		},
	}
}

func testRSP(name string, rspType srv.ReplicatedStoragePoolType, lvgNames ...string) *srv.ReplicatedStoragePool { //nolint:unparam
	lvgs := make([]srv.ReplicatedStoragePoolLVMVolumeGroups, len(lvgNames))
	for i, n := range lvgNames {
		lvgs[i] = srv.ReplicatedStoragePoolLVMVolumeGroups{Name: n}
	}
	return &srv.ReplicatedStoragePool{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: srv.ReplicatedStoragePoolSpec{
			Type:            rspType,
			LVMVolumeGroups: lvgs,
		},
	}
}

func testRV(name string, members []srv.DatameshMember) *srv.ReplicatedVolume {
	return &srv.ReplicatedVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: srv.ReplicatedVolumeStatus{
			Datamesh: srv.ReplicatedVolumeDatamesh{
				Members: members,
			},
		},
	}
}

func diskfulMember(nodeName string) srv.DatameshMember {
	return srv.DatameshMember{
		Name:     "member-" + nodeName,
		Type:     srv.DatameshMemberTypeDiskful,
		NodeName: nodeName,
	}
}

func disklessMember(nodeName string) srv.DatameshMember {
	return srv.DatameshMember{
		Name:     "tiebreaker-" + nodeName,
		Type:     srv.DatameshMemberTypeTieBreaker,
		NodeName: nodeName,
	}
}

func testBoundPVC(name, namespace, scName, pvName string) *corev1.PersistentVolumeClaim { //nolint:unparam
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
			VolumeName:       pvName,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase: corev1.ClaimBound,
		},
	}
}

func testPV(name, csiVolumeHandle string) *corev1.PersistentVolume {
	return &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:       consts.SdsReplicatedVolumeProvisioner,
					VolumeHandle: csiVolumeHandle,
				},
			},
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("1Gi"),
			},
		},
	}
}

func testNode(name string, withReplicatedLabel bool) *corev1.Node {
	labels := map[string]string{}
	if withReplicatedLabel {
		labels[srv.AgentNodeLabelKey] = ""
	}
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: labels,
		},
	}
}

func testSC(name, provisioner string) *storagev1.StorageClass { //nolint:unparam
	return &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: name},
		Provisioner: provisioner,
	}
}

func readyLVGOnNode(name string, nodeName string, vgSize, vgFree int64) *snc.LVMVolumeGroup { //nolint:unparam
	return &snc.LVMVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: snc.LVMVolumeGroupStatus{
			Phase:  snc.PhaseReady,
			VGSize: *resource.NewQuantity(vgSize, resource.BinarySI),
			VGFree: *resource.NewQuantity(vgFree, resource.BinarySI),
			Nodes: []snc.LVMVolumeGroupNode{
				{Name: nodeName},
			},
		},
	}
}

func readyLVGOnNodes(name string, nodeNames []string, vgSize, vgFree int64) *snc.LVMVolumeGroup {
	nodes := make([]snc.LVMVolumeGroupNode, len(nodeNames))
	for i, n := range nodeNames {
		nodes[i] = snc.LVMVolumeGroupNode{Name: n}
	}
	return &snc.LVMVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: snc.LVMVolumeGroupStatus{
			Phase:  snc.PhaseReady,
			VGSize: *resource.NewQuantity(vgSize, resource.BinarySI),
			VGFree: *resource.NewQuantity(vgFree, resource.BinarySI),
			Nodes:  nodes,
		},
	}
}

func readyLVGOnNodesWithThinPool(name string, nodeNames []string, tpAllocated, tpAvailable int64) *snc.LVMVolumeGroup {
	nodes := make([]snc.LVMVolumeGroupNode, len(nodeNames))
	for i, n := range nodeNames {
		nodes[i] = snc.LVMVolumeGroupNode{Name: n}
	}
	return &snc.LVMVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: snc.LVMVolumeGroupStatus{
			Phase:  snc.PhaseReady,
			VGSize: *resource.NewQuantity(hundredGiB, resource.BinarySI),
			VGFree: *resource.NewQuantity(0, resource.BinarySI),
			Nodes:  nodes,
			ThinPools: []snc.LVMVolumeGroupThinPoolStatus{
				{
					Name:           "tp0",
					AllocatedSize:  *resource.NewQuantity(tpAllocated, resource.BinarySI),
					AvailableSpace: *resource.NewQuantity(tpAvailable, resource.BinarySI),
				},
			},
		},
	}
}

func readyLVGOnNodeWithThinPool(name, nodeName string, tpAllocated, tpAvailable int64) *snc.LVMVolumeGroup {
	return &snc.LVMVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: snc.LVMVolumeGroupStatus{
			Phase:  snc.PhaseReady,
			VGSize: *resource.NewQuantity(hundredGiB, resource.BinarySI),
			VGFree: *resource.NewQuantity(0, resource.BinarySI),
			Nodes: []snc.LVMVolumeGroupNode{
				{Name: nodeName},
			},
			ThinPools: []snc.LVMVolumeGroupThinPoolStatus{
				{
					Name:           "tp0",
					AllocatedSize:  *resource.NewQuantity(tpAllocated, resource.BinarySI),
					AvailableSpace: *resource.NewQuantity(tpAvailable, resource.BinarySI),
				},
			},
		},
	}
}

func testLocalSC(name, lvgName string) *storagev1.StorageClass {
	return &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: name},
		Provisioner: consts.SdsLocalVolumeProvisioner,
		Parameters: map[string]string{
			consts.LvmTypeParamKey:         consts.Thick,
			consts.LVMVolumeGroupsParamKey: "- name: " + lvgName + "\n",
		},
	}
}

func testLocalThinSC(name, lvgName, thinPoolName string) *storagev1.StorageClass {
	return &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: name},
		Provisioner: consts.SdsLocalVolumeProvisioner,
		Parameters: map[string]string{
			consts.LvmTypeParamKey:         consts.Thin,
			consts.LVMVolumeGroupsParamKey: "- name: " + lvgName + "\n  thin:\n    poolName: " + thinPoolName + "\n",
		},
	}
}

func testPendingPVC(name, namespace, scName string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase: corev1.ClaimPending,
		},
	}
}

func shadowDiskfulMember(nodeName string) srv.DatameshMember {
	return srv.DatameshMember{
		Name:     "shadow-" + nodeName,
		Type:     srv.DatameshMemberTypeShadowDiskful,
		NodeName: nodeName,
	}
}
