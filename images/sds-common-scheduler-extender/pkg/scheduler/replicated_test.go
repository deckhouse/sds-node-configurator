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

	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/consts"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
	srv "github.com/deckhouse/sds-replicated-volume/api/v1alpha1"
)

func TestGetReplicaNodesForBoundPVC(t *testing.T) {
	ctx := context.Background()

	t.Run("happy path: 2 diskful + 1 tiebreaker -> returns 2 node names", func(t *testing.T) {
		pvc := testBoundPVC("pvc1", "default", "sc1", "pv1")
		pv := testPV("pv1", "pv1")
		rv := testRV("pv1", []srv.DatameshMember{
			diskfulMember("node-1"),
			diskfulMember("node-2"),
			disklessMember("node-3"),
		})

		cl := newFakeClient(pvc, pv, rv)
		nodes, err := getReplicaNodesForBoundPVC(ctx, cl, pvc)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"node-1", "node-2"}, nodes)
	})

	t.Run("RV not found -> returns error", func(t *testing.T) {
		pvc := testBoundPVC("pvc1", "default", "sc1", "pv-missing")

		cl := newFakeClient(pvc)
		_, err := getReplicaNodesForBoundPVC(ctx, cl, pvc)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unable to get ReplicatedVolume")
	})

	t.Run("empty members -> returns empty list", func(t *testing.T) {
		pvc := testBoundPVC("pvc1", "default", "sc1", "pv1")
		pv := testPV("pv1", "pv1")
		rv := testRV("pv1", nil)

		cl := newFakeClient(pvc, pv, rv)
		nodes, err := getReplicaNodesForBoundPVC(ctx, cl, pvc)
		require.NoError(t, err)
		assert.Empty(t, nodes)
	})

	t.Run("only diskless members -> returns empty list", func(t *testing.T) {
		pvc := testBoundPVC("pvc1", "default", "sc1", "pv1")
		pv := testPV("pv1", "pv1")
		rv := testRV("pv1", []srv.DatameshMember{
			disklessMember("node-1"),
			disklessMember("node-2"),
		})

		cl := newFakeClient(pvc, pv, rv)
		nodes, err := getReplicaNodesForBoundPVC(ctx, cl, pvc)
		require.NoError(t, err)
		assert.Empty(t, nodes)
	})
}

func TestFilterNodeForReplicatedPVCs_LocalBound(t *testing.T) {
	ctx := context.Background()
	log, _ := logger.NewLogger("0")
	scName := "sc-local"

	rsc := testRSC(scName, srv.VolumeAccessLocal, "rsp1")
	rsp := testRSP("rsp1", srv.ReplicatedStoragePoolTypeLVM, "lvg1", "lvg2")
	lvg1 := readyLVGOnNode("lvg1", "node-1", hundredGiB, 50*oneGiB)
	lvg2 := readyLVGOnNode("lvg2", "node-2", hundredGiB, 50*oneGiB)
	pvc := testBoundPVC("pvc-local", "default", scName, "pv1")
	pv := testPV("pv1", "pv1")
	rv := testRV("pv1", []srv.DatameshMember{
		diskfulMember("node-1"),
		diskfulMember("node-2"),
	})
	sc := testSC(scName, consts.SdsReplicatedVolumeProvisioner)

	schedulerCache := newTestCache()

	replicatedPVCs := map[string]*corev1.PersistentVolumeClaim{
		pvc.Name: pvc,
	}
	scUsedByPVCs := map[string]*storagev1.StorageClass{
		scName: sc,
	}
	pvcRequests := map[string]PVCRequest{
		pvc.Name: {DeviceType: consts.Thick, RequestedSize: oneGiB},
	}

	t.Run("node with replica passes", func(t *testing.T) {
		node := testNode("node-1", true)
		cl := newFakeClient(rsc, rsp, lvg1, lvg2, pvc, pv, rv, sc, node)
		ok, reason := filterNodeForReplicatedPVCs(ctx, log, cl, schedulerCache, "node-1", node, replicatedPVCs, scUsedByPVCs, pvcRequests)
		assert.True(t, ok, "node-1 should pass (has replica): %s", reason)
	})

	t.Run("node without replica rejected", func(t *testing.T) {
		node := testNode("node-3", true)
		lvg3 := readyLVGOnNode("lvg3", "node-3", hundredGiB, 50*oneGiB)
		cl := newFakeClient(rsc, rsp, lvg1, lvg2, lvg3, pvc, pv, rv, sc, node)
		ok, reason := filterNodeForReplicatedPVCs(ctx, log, cl, schedulerCache, "node-3", node, replicatedPVCs, scUsedByPVCs, pvcRequests)
		assert.False(t, ok, "node-3 should be rejected (no replica)")
		assert.Contains(t, reason, "no diskful replica")
	})

	t.Run("node without replicated label rejected", func(t *testing.T) {
		node := testNode("node-1", false)
		cl := newFakeClient(rsc, rsp, lvg1, lvg2, pvc, pv, rv, sc, node)
		ok, reason := filterNodeForReplicatedPVCs(ctx, log, cl, schedulerCache, "node-1", node, replicatedPVCs, scUsedByPVCs, pvcRequests)
		assert.False(t, ok, "node without label should be rejected")
		assert.Contains(t, reason, srv.AgentNodeLabelKey)
	})
}

func TestFilterNodeForReplicatedPVCs_AnyBound(t *testing.T) {
	ctx := context.Background()
	log, _ := logger.NewLogger("0")
	scName := "sc-any"

	rsc := testRSC(scName, srv.VolumeAccessAny, "rsp1")
	rsp := testRSP("rsp1", srv.ReplicatedStoragePoolTypeLVM, "lvg1")
	lvg1 := readyLVGOnNode("lvg1", "node-1", hundredGiB, 50*oneGiB)
	pvc := testBoundPVC("pvc-any", "default", scName, "pv1")
	pv := testPV("pv1", "pv1")
	rv := testRV("pv1", []srv.DatameshMember{
		diskfulMember("node-1"),
	})
	sc := testSC(scName, consts.SdsReplicatedVolumeProvisioner)

	schedulerCache := newTestCache()

	replicatedPVCs := map[string]*corev1.PersistentVolumeClaim{
		pvc.Name: pvc,
	}
	scUsedByPVCs := map[string]*storagev1.StorageClass{
		scName: sc,
	}
	pvcRequests := map[string]PVCRequest{
		pvc.Name: {DeviceType: consts.Thick, RequestedSize: oneGiB},
	}

	t.Run("node without replica passes for VolumeAccessAny", func(t *testing.T) {
		node := testNode("node-2", true)
		cl := newFakeClient(rsc, rsp, lvg1, pvc, pv, rv, sc, node)
		ok, reason := filterNodeForReplicatedPVCs(ctx, log, cl, schedulerCache, "node-2", node, replicatedPVCs, scUsedByPVCs, pvcRequests)
		assert.True(t, ok, "node-2 should pass for Any access: %s", reason)
	})
}

func TestFilterNodeForReplicatedPVCs_EventuallyLocalBound(t *testing.T) {
	ctx := context.Background()
	log, _ := logger.NewLogger("0")
	scName := "sc-evlocal"

	rsc := testRSC(scName, srv.VolumeAccessEventuallyLocal, "rsp1")
	rsp := testRSP("rsp1", srv.ReplicatedStoragePoolTypeLVM, "lvg1")
	lvg1 := readyLVGOnNode("lvg1", "node-1", hundredGiB, 50*oneGiB)
	pvc := testBoundPVC("pvc-evlocal", "default", scName, "pv1")
	pv := testPV("pv1", "pv1")
	rv := testRV("pv1", []srv.DatameshMember{
		diskfulMember("node-1"),
	})
	sc := testSC(scName, consts.SdsReplicatedVolumeProvisioner)

	schedulerCache := newTestCache()

	replicatedPVCs := map[string]*corev1.PersistentVolumeClaim{
		pvc.Name: pvc,
	}
	scUsedByPVCs := map[string]*storagev1.StorageClass{
		scName: sc,
	}
	pvcRequests := map[string]PVCRequest{
		pvc.Name: {DeviceType: consts.Thick, RequestedSize: oneGiB},
	}

	t.Run("node with LVG passes", func(t *testing.T) {
		node := testNode("node-1", true)
		cl := newFakeClient(rsc, rsp, lvg1, pvc, pv, rv, sc, node)
		ok, reason := filterNodeForReplicatedPVCs(ctx, log, cl, schedulerCache, "node-1", node, replicatedPVCs, scUsedByPVCs, pvcRequests)
		assert.True(t, ok, "node-1 should pass (has LVG with space): %s", reason)
	})

	t.Run("node without LVG rejected", func(t *testing.T) {
		node := testNode("node-2", true)
		cl := newFakeClient(rsc, rsp, lvg1, pvc, pv, rv, sc, node)
		ok, reason := filterNodeForReplicatedPVCs(ctx, log, cl, schedulerCache, "node-2", node, replicatedPVCs, scUsedByPVCs, pvcRequests)
		assert.False(t, ok, "node-2 should be rejected (no LVG)")
		assert.Contains(t, reason, "no LVG from RSP")
	})
}

func TestScoreNodes_ReplicaLocality(t *testing.T) {
	ctx := context.Background()
	log, _ := logger.NewLogger("0")
	scName := "sc-preflocal"

	rsc := testRSC(scName, srv.VolumeAccessPreferablyLocal, "rsp1")
	rsp := testRSP("rsp1", srv.ReplicatedStoragePoolTypeLVM, "lvg1", "lvg2")
	lvg1 := readyLVGOnNode("lvg1", "node-1", hundredGiB, 50*oneGiB)
	lvg2 := readyLVGOnNode("lvg2", "node-2", hundredGiB, 50*oneGiB)
	pvc := testBoundPVC("pvc1", "default", scName, "pv1")
	pv := testPV("pv1", "pv1")
	rv := testRV("pv1", []srv.DatameshMember{
		diskfulMember("node-1"),
		disklessMember("node-2"),
	})
	sc := testSC(scName, consts.SdsReplicatedVolumeProvisioner)

	cl := newFakeClient(rsc, rsp, lvg1, lvg2, pvc, pv, rv, sc)
	schedulerCache := newTestCache()

	managedPVCs := map[string]*corev1.PersistentVolumeClaim{
		pvc.Name: pvc,
	}
	scUsedByPVCs := map[string]*storagev1.StorageClass{
		scName: sc,
	}
	pvcRequests := map[string]PVCRequest{
		pvc.Name: {DeviceType: consts.Thick, RequestedSize: oneGiB},
	}
	replicaLocations := map[string][]string{
		pvc.Name: {"node-1"},
	}

	nodeNames := []string{"node-1", "node-2"}
	scores, err := scoreNodes(ctx, log, cl, schedulerCache, &nodeNames, managedPVCs, scUsedByPVCs, pvcRequests, replicaLocations, 1.0)
	require.NoError(t, err)

	scoreMap := make(map[string]int, len(scores))
	for _, hp := range scores {
		scoreMap[hp.Host] = hp.Score
	}

	t.Run("node with replica gets max score", func(t *testing.T) {
		assert.Equal(t, 10, scoreMap["node-1"], "node-1 should get max score (1/1 replica hits)")
	})

	t.Run("node without replica gets space-based score", func(t *testing.T) {
		assert.Greater(t, scoreMap["node-1"], scoreMap["node-2"],
			"node-1 (replica) should score higher than node-2 (space-only)")
	})
}

func TestScoreNodes_VolumeAccessAny(t *testing.T) {
	ctx := context.Background()
	log, _ := logger.NewLogger("0")
	scName := "sc-any"

	rsc := testRSC(scName, srv.VolumeAccessAny, "rsp1")
	rsp := testRSP("rsp1", srv.ReplicatedStoragePoolTypeLVM, "lvg1", "lvg2")
	lvg1 := readyLVGOnNode("lvg1", "node-1", hundredGiB, 50*oneGiB)
	lvg2 := readyLVGOnNode("lvg2", "node-2", hundredGiB, 50*oneGiB)
	pvc := testBoundPVC("pvc1", "default", scName, "pv1")
	pv := testPV("pv1", "pv1")
	rv := testRV("pv1", []srv.DatameshMember{
		diskfulMember("node-1"),
		disklessMember("node-2"),
	})
	sc := testSC(scName, consts.SdsReplicatedVolumeProvisioner)

	cl := newFakeClient(rsc, rsp, lvg1, lvg2, pvc, pv, rv, sc)
	schedulerCache := newTestCache()

	managedPVCs := map[string]*corev1.PersistentVolumeClaim{
		pvc.Name: pvc,
	}
	scUsedByPVCs := map[string]*storagev1.StorageClass{
		scName: sc,
	}
	pvcRequests := map[string]PVCRequest{
		pvc.Name: {DeviceType: consts.Thick, RequestedSize: oneGiB},
	}
	replicaLocations := map[string][]string{
		pvc.Name: {"node-1"},
	}

	nodeNames := []string{"node-1", "node-2"}
	scores, err := scoreNodes(ctx, log, cl, schedulerCache, &nodeNames, managedPVCs, scUsedByPVCs, pvcRequests, replicaLocations, 1.0)
	require.NoError(t, err)

	scoreMap := make(map[string]int, len(scores))
	for _, hp := range scores {
		scoreMap[hp.Host] = hp.Score
	}

	t.Run("replica location ignored for Any access", func(t *testing.T) {
		assert.Equal(t, scoreMap["node-1"], scoreMap["node-2"],
			"nodes with identical space should get the same score when VolumeAccess=Any")
	})
}

// --- A. Multi-volume replica scoring ---

func TestScoreNodes_MultiVolume_ReplicaIntersection(t *testing.T) {
	ctx := context.Background()
	log, _ := logger.NewLogger("0")

	scName := "sc-pref"
	rsc := testRSC(scName, srv.VolumeAccessPreferablyLocal, "rsp1")
	rsp := testRSP("rsp1", srv.ReplicatedStoragePoolTypeLVM, "lvg1", "lvg2", "lvg3")
	lvg1 := readyLVGOnNode("lvg1", "node-1", hundredGiB, 50*oneGiB)
	lvg2 := readyLVGOnNode("lvg2", "node-2", hundredGiB, 50*oneGiB)
	lvg3 := readyLVGOnNode("lvg3", "node-3", hundredGiB, 50*oneGiB)

	// Vol A: replicas on node-1 and node-2
	pvcA := testBoundPVC("pvc-a", "default", scName, "pv-a")
	pvA := testPV("pv-a", "pv-a")
	rvA := testRV("pv-a", []srv.DatameshMember{diskfulMember("node-1"), diskfulMember("node-2")})

	// Vol B: replicas on node-2 and node-3
	pvcB := testBoundPVC("pvc-b", "default", scName, "pv-b")
	pvB := testPV("pv-b", "pv-b")
	rvB := testRV("pv-b", []srv.DatameshMember{diskfulMember("node-2"), diskfulMember("node-3")})

	sc := testSC(scName, consts.SdsReplicatedVolumeProvisioner)
	cl := newFakeClient(rsc, rsp, lvg1, lvg2, lvg3, pvcA, pvA, rvA, pvcB, pvB, rvB, sc)

	managedPVCs := map[string]*corev1.PersistentVolumeClaim{pvcA.Name: pvcA, pvcB.Name: pvcB}
	scUsedByPVCs := map[string]*storagev1.StorageClass{scName: sc}
	pvcRequests := map[string]PVCRequest{
		pvcA.Name: {DeviceType: consts.Thick, RequestedSize: oneGiB},
		pvcB.Name: {DeviceType: consts.Thick, RequestedSize: oneGiB},
	}
	replicaLocations := map[string][]string{
		pvcA.Name: {"node-1", "node-2"},
		pvcB.Name: {"node-2", "node-3"},
	}

	nodeNames := []string{"node-1", "node-2", "node-3"}
	scores, err := scoreNodes(ctx, log, cl, newTestCache(), &nodeNames, managedPVCs, scUsedByPVCs, pvcRequests, replicaLocations, 1.0)
	require.NoError(t, err)

	scoreMap := make(map[string]int, len(scores))
	for _, hp := range scores {
		scoreMap[hp.Host] = hp.Score
	}

	// node-2 has 2/2 hits -> 10, node-1 and node-3 have 1/2 -> 5
	assert.Equal(t, 10, scoreMap["node-2"], "node-2 should get 10 (2/2 hits)")
	assert.Equal(t, 5, scoreMap["node-1"], "node-1 should get 5 (1/2 hits)")
	assert.Equal(t, 5, scoreMap["node-3"], "node-3 should get 5 (1/2 hits)")
}

func TestScoreNodes_MultiVolume_AllReplicasOnAllNodes(t *testing.T) {
	ctx := context.Background()
	log, _ := logger.NewLogger("0")

	scName := "sc-pref"
	rsc := testRSC(scName, srv.VolumeAccessPreferablyLocal, "rsp1")
	rsp := testRSP("rsp1", srv.ReplicatedStoragePoolTypeLVM, "lvg1", "lvg2")
	lvg1 := readyLVGOnNode("lvg1", "node-1", hundredGiB, 50*oneGiB)
	lvg2 := readyLVGOnNode("lvg2", "node-2", hundredGiB, 50*oneGiB)

	pvcA := testBoundPVC("pvc-a", "default", scName, "pv-a")
	pvA := testPV("pv-a", "pv-a")
	rvA := testRV("pv-a", []srv.DatameshMember{diskfulMember("node-1"), diskfulMember("node-2")})

	pvcB := testBoundPVC("pvc-b", "default", scName, "pv-b")
	pvB := testPV("pv-b", "pv-b")
	rvB := testRV("pv-b", []srv.DatameshMember{diskfulMember("node-1"), diskfulMember("node-2")})

	sc := testSC(scName, consts.SdsReplicatedVolumeProvisioner)
	cl := newFakeClient(rsc, rsp, lvg1, lvg2, pvcA, pvA, rvA, pvcB, pvB, rvB, sc)

	managedPVCs := map[string]*corev1.PersistentVolumeClaim{pvcA.Name: pvcA, pvcB.Name: pvcB}
	scUsedByPVCs := map[string]*storagev1.StorageClass{scName: sc}
	pvcRequests := map[string]PVCRequest{
		pvcA.Name: {DeviceType: consts.Thick, RequestedSize: oneGiB},
		pvcB.Name: {DeviceType: consts.Thick, RequestedSize: oneGiB},
	}
	replicaLocations := map[string][]string{
		pvcA.Name: {"node-1", "node-2"},
		pvcB.Name: {"node-1", "node-2"},
	}

	nodeNames := []string{"node-1", "node-2"}
	scores, err := scoreNodes(ctx, log, cl, newTestCache(), &nodeNames, managedPVCs, scUsedByPVCs, pvcRequests, replicaLocations, 1.0)
	require.NoError(t, err)

	scoreMap := make(map[string]int, len(scores))
	for _, hp := range scores {
		scoreMap[hp.Host] = hp.Score
	}

	assert.Equal(t, 10, scoreMap["node-1"], "node-1 should get 10 (2/2)")
	assert.Equal(t, 10, scoreMap["node-2"], "node-2 should get 10 (2/2)")
}

func TestScoreNodes_ThreeVolumes_PartialOverlap(t *testing.T) {
	ctx := context.Background()
	log, _ := logger.NewLogger("0")

	scName := "sc-pref"
	rsc := testRSC(scName, srv.VolumeAccessPreferablyLocal, "rsp1")
	rsp := testRSP("rsp1", srv.ReplicatedStoragePoolTypeLVM, "lvg1", "lvg2", "lvg3")
	lvg1 := readyLVGOnNode("lvg1", "node-1", hundredGiB, 50*oneGiB)
	lvg2 := readyLVGOnNode("lvg2", "node-2", hundredGiB, 50*oneGiB)
	lvg3 := readyLVGOnNode("lvg3", "node-3", hundredGiB, 50*oneGiB)

	pvcA := testBoundPVC("pvc-a", "default", scName, "pv-a")
	pvA := testPV("pv-a", "pv-a")
	rvA := testRV("pv-a", []srv.DatameshMember{diskfulMember("node-1"), diskfulMember("node-2"), diskfulMember("node-3")})

	pvcB := testBoundPVC("pvc-b", "default", scName, "pv-b")
	pvB := testPV("pv-b", "pv-b")
	rvB := testRV("pv-b", []srv.DatameshMember{diskfulMember("node-1"), diskfulMember("node-2")})

	pvcC := testBoundPVC("pvc-c", "default", scName, "pv-c")
	pvC := testPV("pv-c", "pv-c")
	rvC := testRV("pv-c", []srv.DatameshMember{diskfulMember("node-1")})

	sc := testSC(scName, consts.SdsReplicatedVolumeProvisioner)
	cl := newFakeClient(rsc, rsp, lvg1, lvg2, lvg3, pvcA, pvA, rvA, pvcB, pvB, rvB, pvcC, pvC, rvC, sc)

	managedPVCs := map[string]*corev1.PersistentVolumeClaim{pvcA.Name: pvcA, pvcB.Name: pvcB, pvcC.Name: pvcC}
	scUsedByPVCs := map[string]*storagev1.StorageClass{scName: sc}
	pvcRequests := map[string]PVCRequest{
		pvcA.Name: {DeviceType: consts.Thick, RequestedSize: oneGiB},
		pvcB.Name: {DeviceType: consts.Thick, RequestedSize: oneGiB},
		pvcC.Name: {DeviceType: consts.Thick, RequestedSize: oneGiB},
	}
	replicaLocations := map[string][]string{
		pvcA.Name: {"node-1", "node-2", "node-3"},
		pvcB.Name: {"node-1", "node-2"},
		pvcC.Name: {"node-1"},
	}

	nodeNames := []string{"node-1", "node-2", "node-3"}
	scores, err := scoreNodes(ctx, log, cl, newTestCache(), &nodeNames, managedPVCs, scUsedByPVCs, pvcRequests, replicaLocations, 1.0)
	require.NoError(t, err)

	scoreMap := make(map[string]int, len(scores))
	for _, hp := range scores {
		scoreMap[hp.Host] = hp.Score
	}

	// node-1: 3/3 -> 10, node-2: 2/3 -> 6, node-3: 1/3 -> 3
	assert.Equal(t, 10, scoreMap["node-1"], "node-1 should get 10 (3/3)")
	assert.Equal(t, 6, scoreMap["node-2"], "node-2 should get 6 (2/3)")
	assert.Equal(t, 3, scoreMap["node-3"], "node-3 should get 3 (1/3)")
}

// --- B. Mixed local + replicated scoring ---

func TestScoreNodes_MixedLocalThickAndReplicated_SameLVG(t *testing.T) {
	ctx := context.Background()
	log, _ := logger.NewLogger("0")

	localSCName := "sc-local"
	replSCName := "sc-repl"

	localSC := testLocalSC(localSCName, "lvg1")
	replSC := testSC(replSCName, consts.SdsReplicatedVolumeProvisioner)

	rsc := testRSC(replSCName, srv.VolumeAccessPreferablyLocal, "rsp1")
	rsp := testRSP("rsp1", srv.ReplicatedStoragePoolTypeLVM, "lvg1")

	lvg1n1 := readyLVGOnNode("lvg1", "node-1", hundredGiB, 50*oneGiB)

	localPVC := testBoundPVC("pvc-local", "default", localSCName, "pv-local")
	pvLocal := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-local"},
		Spec: corev1.PersistentVolumeSpec{
			Capacity: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
		},
	}

	replPVC := testBoundPVC("pvc-repl", "default", replSCName, "pv-repl")
	pvRepl := testPV("pv-repl", "pv-repl")
	rv := testRV("pv-repl", []srv.DatameshMember{diskfulMember("node-1")})

	cl := newFakeClient(localSC, replSC, rsc, rsp, lvg1n1, localPVC, pvLocal, replPVC, pvRepl, rv)

	managedPVCs := map[string]*corev1.PersistentVolumeClaim{
		localPVC.Name: localPVC,
		replPVC.Name:  replPVC,
	}
	scUsedByPVCs := map[string]*storagev1.StorageClass{
		localSCName: localSC,
		replSCName:  replSC,
	}
	pvcRequests := map[string]PVCRequest{
		localPVC.Name: {DeviceType: consts.Thick, RequestedSize: oneGiB},
		replPVC.Name:  {DeviceType: consts.Thick, RequestedSize: oneGiB},
	}
	replicaLocations := map[string][]string{
		replPVC.Name: {"node-1"},
	}

	nodeNames := []string{"node-1"}
	scores, err := scoreNodes(ctx, log, cl, newTestCache(), &nodeNames, managedPVCs, scUsedByPVCs, pvcRequests, replicaLocations, 1.0)
	require.NoError(t, err)

	require.Len(t, scores, 1)
	assert.Equal(t, 10, scores[0].Score, "node-1 should get 10 (replica boost overrides space score)")
}

func TestScoreNodes_MixedLocalAndReplicated_DifferentLVGs(t *testing.T) {
	ctx := context.Background()
	log, _ := logger.NewLogger("0")

	localSCName := "sc-local"
	replSCName := "sc-repl"

	localSC := testLocalSC(localSCName, "lvg-local")
	replSC := testSC(replSCName, consts.SdsReplicatedVolumeProvisioner)

	rsc := testRSC(replSCName, srv.VolumeAccessPreferablyLocal, "rsp1")
	rsp := testRSP("rsp1", srv.ReplicatedStoragePoolTypeLVM, "lvg-repl-1", "lvg-repl-2")

	lvgLocal := readyLVGOnNodes("lvg-local", []string{"node-1", "node-2"}, hundredGiB, 50*oneGiB)
	lvgReplN1 := readyLVGOnNode("lvg-repl-1", "node-1", hundredGiB, 50*oneGiB)
	lvgReplN2 := readyLVGOnNode("lvg-repl-2", "node-2", hundredGiB, 50*oneGiB)

	localPVC := testBoundPVC("pvc-local", "default", localSCName, "pv-local")
	pvLocal := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-local"},
		Spec: corev1.PersistentVolumeSpec{
			Capacity: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
		},
	}

	replPVC := testBoundPVC("pvc-repl", "default", replSCName, "pv-repl")
	pvRepl := testPV("pv-repl", "pv-repl")
	rv := testRV("pv-repl", []srv.DatameshMember{diskfulMember("node-2")})

	cl := newFakeClient(localSC, replSC, rsc, rsp, lvgLocal, lvgReplN1, lvgReplN2, localPVC, pvLocal, replPVC, pvRepl, rv)

	managedPVCs := map[string]*corev1.PersistentVolumeClaim{
		localPVC.Name: localPVC,
		replPVC.Name:  replPVC,
	}
	scUsedByPVCs := map[string]*storagev1.StorageClass{
		localSCName: localSC,
		replSCName:  replSC,
	}
	pvcRequests := map[string]PVCRequest{
		localPVC.Name: {DeviceType: consts.Thick, RequestedSize: oneGiB},
		replPVC.Name:  {DeviceType: consts.Thick, RequestedSize: oneGiB},
	}
	replicaLocations := map[string][]string{
		replPVC.Name: {"node-2"},
	}

	nodeNames := []string{"node-1", "node-2"}
	scores, err := scoreNodes(ctx, log, cl, newTestCache(), &nodeNames, managedPVCs, scUsedByPVCs, pvcRequests, replicaLocations, 1.0)
	require.NoError(t, err)

	scoreMap := make(map[string]int, len(scores))
	for _, hp := range scores {
		scoreMap[hp.Host] = hp.Score
	}

	assert.Equal(t, 10, scoreMap["node-2"], "node-2 should get 10 (replica boost)")
	assert.Less(t, scoreMap["node-1"], scoreMap["node-2"], "node-1 should score lower than node-2")
}

func TestScoreNodes_MixedLocalThinAndReplicatedThick(t *testing.T) {
	ctx := context.Background()
	log, _ := logger.NewLogger("0")

	localSCName := "sc-local-thin"
	replSCName := "sc-repl-thick"

	localSC := testLocalThinSC(localSCName, "lvg-thin", "tp0")
	replSC := testSC(replSCName, consts.SdsReplicatedVolumeProvisioner)

	rsc := testRSC(replSCName, srv.VolumeAccessPreferablyLocal, "rsp1")
	rsp := testRSP("rsp1", srv.ReplicatedStoragePoolTypeLVM, "lvg-thick-1", "lvg-thick-2")

	lvgThin := readyLVGOnNodesWithThinPool("lvg-thin", []string{"node-1", "node-2"}, 10*oneGiB, 40*oneGiB)
	lvgThickN1 := readyLVGOnNode("lvg-thick-1", "node-1", hundredGiB, 50*oneGiB)
	lvgThickN2 := readyLVGOnNode("lvg-thick-2", "node-2", hundredGiB, 50*oneGiB)

	localPVC := testBoundPVC("pvc-thin", "default", localSCName, "pv-thin")
	pvLocal := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-thin"},
		Spec: corev1.PersistentVolumeSpec{
			Capacity: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
		},
	}

	replPVC := testBoundPVC("pvc-thick", "default", replSCName, "pv-thick")
	pvRepl := testPV("pv-thick", "pv-thick")
	rv := testRV("pv-thick", []srv.DatameshMember{diskfulMember("node-1")})

	cl := newFakeClient(localSC, replSC, rsc, rsp, lvgThin, lvgThickN1, lvgThickN2, localPVC, pvLocal, replPVC, pvRepl, rv)

	managedPVCs := map[string]*corev1.PersistentVolumeClaim{
		localPVC.Name: localPVC,
		replPVC.Name:  replPVC,
	}
	scUsedByPVCs := map[string]*storagev1.StorageClass{
		localSCName: localSC,
		replSCName:  replSC,
	}
	pvcRequests := map[string]PVCRequest{
		localPVC.Name: {DeviceType: consts.Thin, RequestedSize: oneGiB},
		replPVC.Name:  {DeviceType: consts.Thick, RequestedSize: oneGiB},
	}
	replicaLocations := map[string][]string{
		replPVC.Name: {"node-1"},
	}

	nodeNames := []string{"node-1", "node-2"}
	scores, err := scoreNodes(ctx, log, cl, newTestCache(), &nodeNames, managedPVCs, scUsedByPVCs, pvcRequests, replicaLocations, 1.0)
	require.NoError(t, err)

	scoreMap := make(map[string]int, len(scores))
	for _, hp := range scores {
		scoreMap[hp.Host] = hp.Score
	}

	assert.Equal(t, 10, scoreMap["node-1"], "node-1 should get 10 (replica boost)")
	assert.Greater(t, scoreMap["node-1"], scoreMap["node-2"], "node-1 should beat node-2")
}

func TestScoreNodes_MixedLocalAndReplicated_VolumeAccessAny(t *testing.T) {
	ctx := context.Background()
	log, _ := logger.NewLogger("0")

	localSCName := "sc-local"
	replSCName := "sc-repl-any"

	localSC := testLocalSC(localSCName, "lvg1")
	replSC := testSC(replSCName, consts.SdsReplicatedVolumeProvisioner)

	rsc := testRSC(replSCName, srv.VolumeAccessAny, "rsp1")
	rsp := testRSP("rsp1", srv.ReplicatedStoragePoolTypeLVM, "lvg1")
	lvg1 := readyLVGOnNodes("lvg1", []string{"node-1", "node-2"}, hundredGiB, 50*oneGiB)

	localPVC := testBoundPVC("pvc-local", "default", localSCName, "pv-local")
	pvLocal := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-local"},
		Spec: corev1.PersistentVolumeSpec{
			Capacity: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
		},
	}

	replPVC := testBoundPVC("pvc-repl", "default", replSCName, "pv-repl")
	pvRepl := testPV("pv-repl", "pv-repl")
	rv := testRV("pv-repl", []srv.DatameshMember{diskfulMember("node-1")})

	cl := newFakeClient(localSC, replSC, rsc, rsp, lvg1, localPVC, pvLocal, replPVC, pvRepl, rv)

	managedPVCs := map[string]*corev1.PersistentVolumeClaim{
		localPVC.Name: localPVC,
		replPVC.Name:  replPVC,
	}
	scUsedByPVCs := map[string]*storagev1.StorageClass{
		localSCName: localSC,
		replSCName:  replSC,
	}
	pvcRequests := map[string]PVCRequest{
		localPVC.Name: {DeviceType: consts.Thick, RequestedSize: oneGiB},
		replPVC.Name:  {DeviceType: consts.Thick, RequestedSize: oneGiB},
	}
	replicaLocations := map[string][]string{
		replPVC.Name: {"node-1"},
	}

	nodeNames := []string{"node-1", "node-2"}
	scores, err := scoreNodes(ctx, log, cl, newTestCache(), &nodeNames, managedPVCs, scUsedByPVCs, pvcRequests, replicaLocations, 1.0)
	require.NoError(t, err)

	scoreMap := make(map[string]int, len(scores))
	for _, hp := range scores {
		scoreMap[hp.Host] = hp.Score
	}

	assert.Equal(t, scoreMap["node-1"], scoreMap["node-2"],
		"VolumeAccessAny should not boost node-1 despite having replica")
}

// --- C. Edge cases ---

func TestScoreNodes_ReplicatedPending(t *testing.T) {
	ctx := context.Background()
	log, _ := logger.NewLogger("0")

	scName := "sc-pref"
	rsc := testRSC(scName, srv.VolumeAccessPreferablyLocal, "rsp1")
	rsp := testRSP("rsp1", srv.ReplicatedStoragePoolTypeLVM, "lvg1", "lvg2")
	lvg1 := readyLVGOnNode("lvg1", "node-1", hundredGiB, 50*oneGiB)
	lvg2 := readyLVGOnNode("lvg2", "node-2", hundredGiB, 50*oneGiB)
	sc := testSC(scName, consts.SdsReplicatedVolumeProvisioner)

	pendingPVC := testPendingPVC("pvc-pending", "default", scName)

	cl := newFakeClient(rsc, rsp, lvg1, lvg2, sc, pendingPVC)

	managedPVCs := map[string]*corev1.PersistentVolumeClaim{pendingPVC.Name: pendingPVC}
	scUsedByPVCs := map[string]*storagev1.StorageClass{scName: sc}
	pvcRequests := map[string]PVCRequest{
		pendingPVC.Name: {DeviceType: consts.Thick, RequestedSize: oneGiB},
	}
	replicaLocations := map[string][]string{}

	nodeNames := []string{"node-1", "node-2"}
	scores, err := scoreNodes(ctx, log, cl, newTestCache(), &nodeNames, managedPVCs, scUsedByPVCs, pvcRequests, replicaLocations, 1.0)
	require.NoError(t, err)

	scoreMap := make(map[string]int, len(scores))
	for _, hp := range scores {
		scoreMap[hp.Host] = hp.Score
	}

	assert.Equal(t, scoreMap["node-1"], scoreMap["node-2"],
		"pending PVC should give equal space-based scores on identical nodes")
	assert.Greater(t, scoreMap["node-1"], 0, "scores should be positive")
}

func TestScoreNodes_ShadowDiskfulCountsAsReplica(t *testing.T) {
	ctx := context.Background()
	log, _ := logger.NewLogger("0")

	scName := "sc-pref"
	rsc := testRSC(scName, srv.VolumeAccessPreferablyLocal, "rsp1")
	rsp := testRSP("rsp1", srv.ReplicatedStoragePoolTypeLVM, "lvg1", "lvg2")
	lvg1 := readyLVGOnNode("lvg1", "node-1", hundredGiB, 50*oneGiB)
	lvg2 := readyLVGOnNode("lvg2", "node-2", hundredGiB, 50*oneGiB)
	sc := testSC(scName, consts.SdsReplicatedVolumeProvisioner)

	pvc := testBoundPVC("pvc1", "default", scName, "pv1")
	pv := testPV("pv1", "pv1")
	rv := testRV("pv1", []srv.DatameshMember{
		shadowDiskfulMember("node-1"),
		disklessMember("node-2"),
	})

	cl := newFakeClient(rsc, rsp, lvg1, lvg2, sc, pvc, pv, rv)

	// Verify ShadowDiskful is resolved as a replica node
	nodes, err := getReplicaNodesForBoundPVC(ctx, cl, pvc)
	require.NoError(t, err)
	assert.Equal(t, []string{"node-1"}, nodes, "ShadowDiskful should count as replica")

	managedPVCs := map[string]*corev1.PersistentVolumeClaim{pvc.Name: pvc}
	scUsedByPVCs := map[string]*storagev1.StorageClass{scName: sc}
	pvcRequests := map[string]PVCRequest{pvc.Name: {DeviceType: consts.Thick, RequestedSize: oneGiB}}
	replicaLocations := map[string][]string{pvc.Name: {"node-1"}}

	nodeNames := []string{"node-1", "node-2"}
	scores, err := scoreNodes(ctx, log, cl, newTestCache(), &nodeNames, managedPVCs, scUsedByPVCs, pvcRequests, replicaLocations, 1.0)
	require.NoError(t, err)

	scoreMap := make(map[string]int, len(scores))
	for _, hp := range scores {
		scoreMap[hp.Host] = hp.Score
	}

	assert.Equal(t, 10, scoreMap["node-1"], "node-1 should get 10 (ShadowDiskful replica)")
	assert.Greater(t, scoreMap["node-1"], scoreMap["node-2"])
}

func TestScoreNodes_MixedVolumeAccess_LocalAndAny(t *testing.T) {
	ctx := context.Background()
	log, _ := logger.NewLogger("0")

	localSCName := "sc-local-va"
	anySCName := "sc-any-va"

	localSC := testSC(localSCName, consts.SdsReplicatedVolumeProvisioner)
	anySC := testSC(anySCName, consts.SdsReplicatedVolumeProvisioner)

	rscLocal := testRSC(localSCName, srv.VolumeAccessLocal, "rsp1")
	rscAny := testRSC(anySCName, srv.VolumeAccessAny, "rsp1")
	rsp := testRSP("rsp1", srv.ReplicatedStoragePoolTypeLVM, "lvg1", "lvg2")
	lvg1 := readyLVGOnNode("lvg1", "node-1", hundredGiB, 50*oneGiB)
	lvg2 := readyLVGOnNode("lvg2", "node-2", hundredGiB, 50*oneGiB)

	localPVC := testBoundPVC("pvc-local-va", "default", localSCName, "pv-local-va")
	pvLocal := testPV("pv-local-va", "pv-local-va")
	rvLocal := testRV("pv-local-va", []srv.DatameshMember{diskfulMember("node-1"), diskfulMember("node-2")})

	anyPVC := testBoundPVC("pvc-any-va", "default", anySCName, "pv-any-va")
	pvAny := testPV("pv-any-va", "pv-any-va")
	rvAny := testRV("pv-any-va", []srv.DatameshMember{diskfulMember("node-1")})

	cl := newFakeClient(localSC, anySC, rscLocal, rscAny, rsp, lvg1, lvg2, localPVC, pvLocal, rvLocal, anyPVC, pvAny, rvAny)

	managedPVCs := map[string]*corev1.PersistentVolumeClaim{
		localPVC.Name: localPVC,
		anyPVC.Name:   anyPVC,
	}
	scUsedByPVCs := map[string]*storagev1.StorageClass{
		localSCName: localSC,
		anySCName:   anySC,
	}
	pvcRequests := map[string]PVCRequest{
		localPVC.Name: {DeviceType: consts.Thick, RequestedSize: oneGiB},
		anyPVC.Name:   {DeviceType: consts.Thick, RequestedSize: oneGiB},
	}
	replicaLocations := map[string][]string{
		localPVC.Name: {"node-1", "node-2"},
		anyPVC.Name:   {"node-1"},
	}

	nodeNames := []string{"node-1", "node-2"}
	scores, err := scoreNodes(ctx, log, cl, newTestCache(), &nodeNames, managedPVCs, scUsedByPVCs, pvcRequests, replicaLocations, 1.0)
	require.NoError(t, err)

	scoreMap := make(map[string]int, len(scores))
	for _, hp := range scores {
		scoreMap[hp.Host] = hp.Score
	}

	// Only the Local PVC counts for replicaTotalPVCs (1). Both nodes have replica for it -> 1/1 = 10
	// Any PVC does NOT count
	assert.Equal(t, 10, scoreMap["node-1"], "node-1 should get 10 (1/1 Local hits)")
	assert.Equal(t, 10, scoreMap["node-2"], "node-2 should get 10 (1/1 Local hits)")
}
