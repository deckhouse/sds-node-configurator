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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	srv "github.com/deckhouse/sds-replicated-volume/api/v1alpha1"

	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/consts"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
)

func TestGetReplicaNodesForBoundPVC(t *testing.T) {
	ctx := context.Background()

	t.Run("happy path: 2 diskful + 1 tiebreaker -> returns 2 node names", func(t *testing.T) {
		pvc := testBoundPVC("pvc1", "default", "sc1", "pv1")
		pv := testPV("pv1", "rv1")
		rv := testRV("rv1", []srv.DatameshMember{
			diskfulMember("node-1"),
			diskfulMember("node-2"),
			disklessMember("node-3"),
		})

		cl := newFakeClient(pvc, pv, rv)
		nodes, err := getReplicaNodesForBoundPVC(ctx, cl, pvc)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"node-1", "node-2"}, nodes)
	})

	t.Run("no CSI spec -> returns error", func(t *testing.T) {
		pvc := testBoundPVC("pvc1", "default", "sc1", "pv-no-csi")
		pv := &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv-no-csi"},
			Spec:       corev1.PersistentVolumeSpec{},
		}

		cl := newFakeClient(pvc, pv)
		_, err := getReplicaNodesForBoundPVC(ctx, cl, pvc)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no CSI spec")
	})

	t.Run("RV not found -> returns error", func(t *testing.T) {
		pvc := testBoundPVC("pvc1", "default", "sc1", "pv1")
		pv := testPV("pv1", "rv-missing")

		cl := newFakeClient(pvc, pv)
		_, err := getReplicaNodesForBoundPVC(ctx, cl, pvc)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unable to get ReplicatedVolume")
	})

	t.Run("empty members -> returns empty list", func(t *testing.T) {
		pvc := testBoundPVC("pvc1", "default", "sc1", "pv1")
		pv := testPV("pv1", "rv1")
		rv := testRV("rv1", nil)

		cl := newFakeClient(pvc, pv, rv)
		nodes, err := getReplicaNodesForBoundPVC(ctx, cl, pvc)
		require.NoError(t, err)
		assert.Empty(t, nodes)
	})

	t.Run("only diskless members -> returns empty list", func(t *testing.T) {
		pvc := testBoundPVC("pvc1", "default", "sc1", "pv1")
		pv := testPV("pv1", "rv1")
		rv := testRV("rv1", []srv.DatameshMember{
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
	pv := testPV("pv1", "rv1")
	rv := testRV("rv1", []srv.DatameshMember{
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
	pv := testPV("pv1", "rv1")
	rv := testRV("rv1", []srv.DatameshMember{
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
	pv := testPV("pv1", "rv1")
	rv := testRV("rv1", []srv.DatameshMember{
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
	pv := testPV("pv1", "rv1")
	rv := testRV("rv1", []srv.DatameshMember{
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

	t.Run("node with replica gets boosted score", func(t *testing.T) {
		// Raw score 100 -> getNodeScore(100, 1.0) = round(log2(100)) = 7
		assert.Equal(t, 7, scoreMap["node-1"], "node-1 should get replica-boosted score")
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
	pv := testPV("pv1", "rv1")
	rv := testRV("rv1", []srv.DatameshMember{
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
