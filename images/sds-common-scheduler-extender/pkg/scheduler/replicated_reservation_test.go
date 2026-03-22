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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	srv "github.com/deckhouse/sds-replicated-volume/api/v1alpha1"

	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/cache"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/consts"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
)

func TestCreateReservations_ReplicatedPVC(t *testing.T) {
	log, _ := logger.NewLogger("0")

	scName := "replicated-sc"
	rspName := "test-rsp"

	sc := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: scName},
		Provisioner: consts.SdsReplicatedVolumeProvisioner,
	}
	rsc := testRSC(scName, srv.VolumeAccessPreferablyLocal, rspName)
	rsp := testRSP(rspName, srv.ReplicatedStoragePoolTypeLVM, "lvg-node1", "lvg-node2", "lvg-node3")

	lvg1 := readyLVGOnNode("lvg-node1", "node1", hundredGiB, hundredGiB)
	lvg2 := readyLVGOnNode("lvg-node2", "node2", hundredGiB, hundredGiB)
	lvg3 := readyLVGOnNode("lvg-node3", "node3", hundredGiB, hundredGiB)

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "rep-pvc", Namespace: "default"},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			},
		},
	}

	cl := newFakeClient(sc, rsc, rsp, lvg1, lvg2, lvg3, pvc)
	c := newTestCache()

	filteredNodes := []string{"node1", "node2", "node3"}
	managedPVCs := map[string]*corev1.PersistentVolumeClaim{"rep-pvc": pvc}
	scUsedByPVCs := map[string]*storagev1.StorageClass{scName: sc}
	pvcRequests := map[string]PVCRequest{
		"rep-pvc": {RequestedSize: 10 * oneGiB, DeviceType: consts.Thick},
	}

	err := createReservations(context.Background(), log, cl, c, &filteredNodes, managedPVCs, scUsedByPVCs, pvcRequests)
	require.NoError(t, err)

	pvcKey := "default/rep-pvc"
	assert.True(t, c.HasReservation(pvcKey))
	assert.True(t, c.IsReservationReplicated(pvcKey))

	size, pools, found := c.GetReservation(pvcKey)
	require.True(t, found)
	assert.Equal(t, 10*oneGiB, size)
	assert.Len(t, pools, 3)

	poolSet := make(map[cache.StoragePoolKey]struct{})
	for _, p := range pools {
		poolSet[p] = struct{}{}
	}
	assert.Contains(t, poolSet, cache.StoragePoolKey{LVGName: "lvg-node1"})
	assert.Contains(t, poolSet, cache.StoragePoolKey{LVGName: "lvg-node2"})
	assert.Contains(t, poolSet, cache.StoragePoolKey{LVGName: "lvg-node3"})
}

func TestCreateReservations_LocalPVC_NotReplicated(t *testing.T) {
	log, _ := logger.NewLogger("0")

	scName := "local-sc"
	sc := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: scName},
		Provisioner: consts.SdsLocalVolumeProvisioner,
		Parameters: map[string]string{
			consts.LvmTypeParamKey:         consts.Thick,
			consts.LVMVolumeGroupsParamKey: `[{"name":"lvg1"}]`,
		},
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "local-pvc", Namespace: "default"},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("5Gi")},
			},
		},
	}
	lvg := readyLVGOnNode("lvg1", "node1", hundredGiB, hundredGiB)

	cl := newFakeClient(sc, pvc, lvg)
	c := newTestCache()

	filteredNodes := []string{"node1"}
	managedPVCs := map[string]*corev1.PersistentVolumeClaim{"local-pvc": pvc}
	scUsedByPVCs := map[string]*storagev1.StorageClass{scName: sc}
	pvcRequests := map[string]PVCRequest{
		"local-pvc": {RequestedSize: 5 * oneGiB, DeviceType: consts.Thick},
	}

	err := createReservations(context.Background(), log, cl, c, &filteredNodes, managedPVCs, scUsedByPVCs, pvcRequests)
	require.NoError(t, err)

	pvcKey := "default/local-pvc"
	assert.True(t, c.HasReservation(pvcKey))
	assert.False(t, c.IsReservationReplicated(pvcKey))
}

func TestCreateReservations_MixedLocalAndReplicated(t *testing.T) {
	log, _ := logger.NewLogger("0")

	localSCName := "local-sc"
	repSCName := "rep-sc"
	rspName := "rsp1"

	localSC := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: localSCName},
		Provisioner: consts.SdsLocalVolumeProvisioner,
		Parameters: map[string]string{
			consts.LvmTypeParamKey:         consts.Thick,
			consts.LVMVolumeGroupsParamKey: `[{"name":"lvg-local"}]`,
		},
	}
	repSC := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: repSCName},
		Provisioner: consts.SdsReplicatedVolumeProvisioner,
	}
	rsc := testRSC(repSCName, srv.VolumeAccessPreferablyLocal, rspName)
	rsp := testRSP(rspName, srv.ReplicatedStoragePoolTypeLVM, "lvg-rep")

	localPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "local-pvc", Namespace: "ns"},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &localSCName,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("5Gi")},
			},
		},
	}
	repPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "rep-pvc", Namespace: "ns"},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &repSCName,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			},
		},
	}

	lvgLocal := readyLVGOnNode("lvg-local", "node1", hundredGiB, hundredGiB)
	lvgRep := readyLVGOnNode("lvg-rep", "node1", hundredGiB, hundredGiB)

	cl := newFakeClient(localSC, repSC, rsc, rsp, localPVC, repPVC, lvgLocal, lvgRep)
	c := newTestCache()

	filteredNodes := []string{"node1"}
	managedPVCs := map[string]*corev1.PersistentVolumeClaim{
		"local-pvc": localPVC,
		"rep-pvc":   repPVC,
	}
	scUsedByPVCs := map[string]*storagev1.StorageClass{
		localSCName: localSC,
		repSCName:   repSC,
	}
	pvcRequests := map[string]PVCRequest{
		"local-pvc": {RequestedSize: 5 * oneGiB, DeviceType: consts.Thick},
		"rep-pvc":   {RequestedSize: 10 * oneGiB, DeviceType: consts.Thick},
	}

	err := createReservations(context.Background(), log, cl, c, &filteredNodes, managedPVCs, scUsedByPVCs, pvcRequests)
	require.NoError(t, err)

	assert.True(t, c.HasReservation("ns/local-pvc"))
	assert.False(t, c.IsReservationReplicated("ns/local-pvc"))

	assert.True(t, c.HasReservation("ns/rep-pvc"))
	assert.True(t, c.IsReservationReplicated("ns/rep-pvc"))
}

func TestNarrowReservationsToFinalNodes_ReplicatedPVC(t *testing.T) {
	log, _ := logger.NewLogger("0")

	scName := "replicated-sc"
	rspName := "test-rsp"

	sc := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: scName},
		Provisioner: consts.SdsReplicatedVolumeProvisioner,
	}
	rsc := testRSC(scName, srv.VolumeAccessPreferablyLocal, rspName)
	rsp := testRSP(rspName, srv.ReplicatedStoragePoolTypeLVM, "lvg-node1", "lvg-node2", "lvg-node3")

	lvg1 := readyLVGOnNode("lvg-node1", "node1", hundredGiB, hundredGiB)
	lvg2 := readyLVGOnNode("lvg-node2", "node2", hundredGiB, hundredGiB)
	lvg3 := readyLVGOnNode("lvg-node3", "node3", hundredGiB, hundredGiB)

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "rep-pvc", Namespace: "default"},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			},
		},
	}

	cl := newFakeClient(sc, rsc, rsp, lvg1, lvg2, lvg3, pvc)
	c := newTestCache()

	pvcKey := "default/rep-pvc"
	c.AddReservation(pvcKey, defaultReservationTTL, 10*oneGiB, []cache.StoragePoolKey{
		{LVGName: "lvg-node1"},
		{LVGName: "lvg-node2"},
		{LVGName: "lvg-node3"},
	}, true)

	managedPVCs := map[string]*corev1.PersistentVolumeClaim{"rep-pvc": pvc}
	scUsedByPVCs := map[string]*storagev1.StorageClass{scName: sc}
	pvcRequests := map[string]PVCRequest{
		"rep-pvc": {RequestedSize: 10 * oneGiB, DeviceType: consts.Thick},
	}

	// Only node1 and node2 survive prioritize
	finalNodes := []string{"node1", "node2"}
	narrowReservationsToFinalNodes(context.Background(), log, cl, c, finalNodes, managedPVCs, scUsedByPVCs, pvcRequests)

	_, pools, found := c.GetReservation(pvcKey)
	require.True(t, found)
	assert.Len(t, pools, 2)

	poolSet := make(map[cache.StoragePoolKey]struct{})
	for _, p := range pools {
		poolSet[p] = struct{}{}
	}
	assert.Contains(t, poolSet, cache.StoragePoolKey{LVGName: "lvg-node1"})
	assert.Contains(t, poolSet, cache.StoragePoolKey{LVGName: "lvg-node2"})
	assert.NotContains(t, poolSet, cache.StoragePoolKey{LVGName: "lvg-node3"})
}

func TestPVCWatcher_SkipNarrowForReplicated(t *testing.T) {
	c := newTestCache()

	pool1 := cache.StoragePoolKey{LVGName: "lvg-node1"}
	pool2 := cache.StoragePoolKey{LVGName: "lvg-node2"}
	pool3 := cache.StoragePoolKey{LVGName: "lvg-node3"}

	c.AddReservation("default/rep-pvc", defaultReservationTTL, 10*oneGiB, []cache.StoragePoolKey{pool1, pool2, pool3}, true)

	assert.True(t, c.IsReservationReplicated("default/rep-pvc"))

	// The PVC watcher checks IsReservationReplicated and skips narrow.
	// Verify the reservation still has all 3 pools.
	_, pools, found := c.GetReservation("default/rep-pvc")
	require.True(t, found)
	assert.Len(t, pools, 3)
}

func TestPVCWatcher_NarrowLocalPVC(t *testing.T) {
	c := newTestCache()

	pool1 := cache.StoragePoolKey{LVGName: "lvg-node1"}
	pool2 := cache.StoragePoolKey{LVGName: "lvg-node2"}

	c.AddReservation("default/local-pvc", defaultReservationTTL, 5*oneGiB, []cache.StoragePoolKey{pool1, pool2}, false)

	assert.False(t, c.IsReservationReplicated("default/local-pvc"))

	// Simulating what PVC watcher does for local PVCs: narrow to single node
	c.NarrowReservation("default/local-pvc", []cache.StoragePoolKey{pool1}, defaultReservationTTL)

	_, pools, found := c.GetReservation("default/local-pvc")
	require.True(t, found)
	assert.Len(t, pools, 1)
	assert.Equal(t, pool1, pools[0])
}

func TestCreateReservations_ReplicatedPVC_NodeWithoutRSPLVG(t *testing.T) {
	log, _ := logger.NewLogger("0")

	scName := "replicated-sc"
	rspName := "test-rsp"

	sc := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: scName},
		Provisioner: consts.SdsReplicatedVolumeProvisioner,
	}
	rsc := testRSC(scName, srv.VolumeAccessPreferablyLocal, rspName)
	// RSP only has lvg-node1, not lvg for node2
	rsp := testRSP(rspName, srv.ReplicatedStoragePoolTypeLVM, "lvg-node1")

	lvg1 := readyLVGOnNode("lvg-node1", "node1", hundredGiB, hundredGiB)

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "rep-pvc", Namespace: "default"},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			},
		},
	}

	cl := newFakeClient(sc, rsc, rsp, lvg1, pvc)
	c := newTestCache()

	// node2 passed filter (e.g. volumeAccess=Any) but has no RSP LVG
	filteredNodes := []string{"node1", "node2"}
	managedPVCs := map[string]*corev1.PersistentVolumeClaim{"rep-pvc": pvc}
	scUsedByPVCs := map[string]*storagev1.StorageClass{scName: sc}
	pvcRequests := map[string]PVCRequest{
		"rep-pvc": {RequestedSize: 10 * oneGiB, DeviceType: consts.Thick},
	}

	err := createReservations(context.Background(), log, cl, c, &filteredNodes, managedPVCs, scUsedByPVCs, pvcRequests)
	require.NoError(t, err)

	pvcKey := "default/rep-pvc"
	_, pools, found := c.GetReservation(pvcKey)
	require.True(t, found)
	// Only node1 should have a reservation (node2 has no RSP LVG)
	assert.Len(t, pools, 1)
	assert.Equal(t, cache.StoragePoolKey{LVGName: "lvg-node1"}, pools[0])
}
