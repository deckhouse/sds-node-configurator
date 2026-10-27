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

package scheduler

// The space score of a node must not depend on the kind of the PVC: a local and a
// replicated PVC on pools with the same free space give the same score, and in a pod
// with both kinds each pool counts in the same unit.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/consts"
	srv "github.com/deckhouse/sds-replicated-volume/api/v1alpha1"
)

const thousandGiB = 1000 * oneGiB

// The free space left after a 1Gi request on a 1000Gi pool is 99, 50, 10 and 2 percent.
var scaleNodes = []struct {
	node   string
	vgFree int64
}{
	{"n99", thousandGiB},
	{"n50", 501 * oneGiB},
	{"n10", 101 * oneGiB},
	{"n2", 21 * oneGiB},
}

// One conversion of the free percent: round(log2(99|50|10|2)) = 7, 6, 3, 1.
var expectedSpaceScores = map[string]int{"n99": 7, "n50": 6, "n10": 3, "n2": 1}

func localSCWithLVGs(name string, lvgNames ...string) *storagev1.StorageClass {
	param := ""
	for _, n := range lvgNames {
		param += "- name: " + n + "\n"
	}
	return &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: name},
		Provisioner: consts.SdsLocalVolumeProvisioner,
		Parameters: map[string]string{
			consts.LvmTypeParamKey:         consts.Thick,
			consts.LVMVolumeGroupsParamKey: param,
		},
	}
}

func scoreMapOf(t *testing.T, cl client.Client, nodeNames []string,
	managedPVCs map[string]*corev1.PersistentVolumeClaim, scs map[string]*storagev1.StorageClass,
	reqs map[string]PVCRequest) map[string]int {
	t.Helper()
	log, _ := logger.NewLogger("0")
	scores, err := scoreNodes(context.Background(), log, cl, newTestCache(), &nodeNames, managedPVCs, scs, reqs, map[string][]string{}, 1.0)
	require.NoError(t, err)
	m := make(map[string]int, len(scores))
	for _, hp := range scores {
		m[hp.Host] = hp.Score
	}
	return m
}

func TestScoreScale_ReplicatedMatchesLocal(t *testing.T) {
	lvgNames := []string{}
	nodeNames := []string{}
	lvgs := []client.Object{}
	for _, n := range scaleNodes {
		lvgName := "lvg-" + n.node
		lvgs = append(lvgs, readyLVGOnNode(lvgName, n.node, thousandGiB, n.vgFree))
		lvgNames = append(lvgNames, lvgName)
		nodeNames = append(nodeNames, n.node)
	}
	reqs := map[string]PVCRequest{"pvc": {DeviceType: consts.Thick, RequestedSize: oneGiB}}

	t.Run("local", func(t *testing.T) {
		sc := localSCWithLVGs("sc-local", lvgNames...)
		pvc := testPendingPVC("pvc", "default", sc.Name)
		cl := newFakeClient(append(lvgs, sc, pvc)...)
		got := scoreMapOf(t, cl, nodeNames, map[string]*corev1.PersistentVolumeClaim{pvc.Name: pvc},
			map[string]*storagev1.StorageClass{sc.Name: sc}, reqs)
		require.Equal(t, expectedSpaceScores, got)
	})

	// A pending PreferablyLocal PVC has no replicas yet, so every node is scored by space.
	t.Run("replicated", func(t *testing.T) {
		sc := testSC("sc-repl", consts.SdsReplicatedVolumeProvisioner)
		rsc := testRSC(sc.Name, srv.VolumeAccessPreferablyLocal, "rsp1")
		rsp := testRSP("rsp1", srv.ReplicatedStoragePoolTypeLVM, lvgNames...)
		pvc := testPendingPVC("pvc", "default", sc.Name)
		cl := newFakeClient(append(lvgs, sc, rsc, rsp, pvc)...)
		got := scoreMapOf(t, cl, nodeNames, map[string]*corev1.PersistentVolumeClaim{pvc.Name: pvc},
			map[string]*storagev1.StorageClass{sc.Name: sc}, reqs)
		require.Equal(t, expectedSpaceScores, got)
	})
}

// A local PVC with 50% free on both nodes and a replicated PVC with 99% free on node-A
// and 10% on node-B: (50+99)/2 = 74% -> 6 against (50+10)/2 = 30% -> 5.
func TestScoreScale_MixedPodSeesReplicatedSpace(t *testing.T) {
	localSC := localSCWithLVGs("sc-local", "lvg-loc-A", "lvg-loc-B")
	replSC := testSC("sc-repl", consts.SdsReplicatedVolumeProvisioner)
	rsc := testRSC(replSC.Name, srv.VolumeAccessPreferablyLocal, "rsp1")
	rsp := testRSP("rsp1", srv.ReplicatedStoragePoolTypeLVM, "lvg-rep-A", "lvg-rep-B")

	localPVC := testPendingPVC("pvc-local", "default", localSC.Name)
	replPVC := testPendingPVC("pvc-repl", "default", replSC.Name)

	cl := newFakeClient(localSC, replSC, rsc, rsp, localPVC, replPVC,
		readyLVGOnNode("lvg-loc-A", "node-A", thousandGiB, 501*oneGiB),
		readyLVGOnNode("lvg-loc-B", "node-B", thousandGiB, 501*oneGiB),
		readyLVGOnNode("lvg-rep-A", "node-A", thousandGiB, thousandGiB),
		readyLVGOnNode("lvg-rep-B", "node-B", thousandGiB, 101*oneGiB),
	)

	got := scoreMapOf(t, cl, []string{"node-A", "node-B"},
		map[string]*corev1.PersistentVolumeClaim{localPVC.Name: localPVC, replPVC.Name: replPVC},
		map[string]*storagev1.StorageClass{localSC.Name: localSC, replSC.Name: replSC},
		map[string]PVCRequest{
			localPVC.Name: {DeviceType: consts.Thick, RequestedSize: oneGiB},
			replPVC.Name:  {DeviceType: consts.Thick, RequestedSize: oneGiB},
		})
	require.Equal(t, map[string]int{"node-A": 6, "node-B": 5}, got)
}

// A pool smaller than the request adds 0, the same as a node without a pool of the RSP.
// Pod: a local PVC with 50% free on both nodes and a pending PreferablyLocal PVC of 500Gi.
// node-A has a 100Gi pool of the RSP with 1Gi free, node-C has no pool of the RSP.
// Both: (50+0)/2 = 25% -> 5. A negative percent would give node-A (50-499)/2 -> 1.
func TestScoreScale_PoolSmallerThanRequest(t *testing.T) {
	localSC := localSCWithLVGs("sc-local", "lvg-loc-A", "lvg-loc-C")
	replSC := testSC("sc-repl", consts.SdsReplicatedVolumeProvisioner)
	rsc := testRSC(replSC.Name, srv.VolumeAccessPreferablyLocal, "rsp1")
	rsp := testRSP("rsp1", srv.ReplicatedStoragePoolTypeLVM, "lvg-rep-A")

	localPVC := testPendingPVC("pvc-local", "default", localSC.Name)
	replPVC := testPendingPVC("pvc-repl", "default", replSC.Name)

	cl := newFakeClient(localSC, replSC, rsc, rsp, localPVC, replPVC,
		readyLVGOnNode("lvg-loc-A", "node-A", thousandGiB, 501*oneGiB),
		readyLVGOnNode("lvg-loc-C", "node-C", thousandGiB, 501*oneGiB),
		readyLVGOnNode("lvg-rep-A", "node-A", hundredGiB, oneGiB),
	)

	got := scoreMapOf(t, cl, []string{"node-A", "node-C"},
		map[string]*corev1.PersistentVolumeClaim{localPVC.Name: localPVC, replPVC.Name: replPVC},
		map[string]*storagev1.StorageClass{localSC.Name: localSC, replSC.Name: replSC},
		map[string]PVCRequest{
			localPVC.Name: {DeviceType: consts.Thick, RequestedSize: oneGiB},
			replPVC.Name:  {DeviceType: consts.Thick, RequestedSize: 500 * oneGiB},
		})
	require.Equal(t, map[string]int{"node-A": 5, "node-C": 5}, got)
}

func TestGetFreeSpaceLeftPercent_Bounds(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		free, requested, total int64
		want                   int64
	}{
		{"half left", 51 * oneGiB, oneGiB, hundredGiB, 50},
		{"request equals free", oneGiB, oneGiB, hundredGiB, 0},
		{"request exceeds pool", oneGiB, 500 * oneGiB, hundredGiB, 0},
		// Without the guard 0/0 is NaN, and int64 of NaN differs between amd64 and arm64.
		{"zero size, zero request", 0, 0, 0, 0},
		{"zero size", 0, oneGiB, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, getFreeSpaceLeftPercent(tc.free, tc.requested, tc.total))
		})
	}
}

// A node with a replica of one of three PVCs must not lose to a node without replicas when
// the pools are equally free. Three bound PreferablyLocal PVCs, all pools 50% free;
// replicas: pvc-a on n3, n2, n1; pvc-b on n3, n2; pvc-c on n3.
// n1: the two PVCs without a replica average 50% -> 6, above the replica score 10*1/3 = 3.
// n0: 50% -> 6. Counting the replica PVC as 0% in the average gave n1 (0+50+50)/3 -> 5.
func TestScoreScale_PartialReplicaNotBelowNoReplica(t *testing.T) {
	scName := "sc-pref"
	sc := testSC(scName, consts.SdsReplicatedVolumeProvisioner)
	objs := []client.Object{
		sc,
		testRSC(scName, srv.VolumeAccessPreferablyLocal, "rsp1"),
		testRSP("rsp1", srv.ReplicatedStoragePoolTypeLVM, "lvg-n3", "lvg-n2", "lvg-n1", "lvg-n0"),
	}
	nodeNames := []string{"n3", "n2", "n1", "n0"}
	for _, n := range nodeNames {
		objs = append(objs, readyLVGOnNode("lvg-"+n, n, hundredGiB, 50*oneGiB))
	}

	replicaLocations := map[string][]string{
		"pvc-a": {"n3", "n2", "n1"},
		"pvc-b": {"n3", "n2"},
		"pvc-c": {"n3"},
	}
	pvcs := map[string]*corev1.PersistentVolumeClaim{}
	reqs := map[string]PVCRequest{}
	for name, nodes := range replicaLocations {
		members := make([]srv.DatameshMember, 0, len(nodes))
		for _, n := range nodes {
			members = append(members, diskfulMember(n))
		}
		pvName := "pv-" + name
		pvc := testBoundPVC(name, "default", scName, pvName)
		objs = append(objs, pvc, testPV(pvName, pvName), testRV(pvName, members))
		pvcs[name] = pvc
		// A bound PVC only needs the growth delta, here none.
		reqs[name] = PVCRequest{DeviceType: consts.Thick}
	}

	log, _ := logger.NewLogger("0")
	scores, err := scoreNodes(context.Background(), log, newFakeClient(objs...), newTestCache(), &nodeNames, pvcs,
		map[string]*storagev1.StorageClass{scName: sc}, reqs, replicaLocations, 1.0)
	require.NoError(t, err)
	got := make(map[string]int, len(scores))
	for _, hp := range scores {
		got[hp.Host] = hp.Score
	}
	require.Equal(t, map[string]int{"n3": 10, "n2": 6, "n1": 6, "n0": 6}, got)
}
