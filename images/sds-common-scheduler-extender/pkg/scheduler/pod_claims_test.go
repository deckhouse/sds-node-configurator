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

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/consts"
	srv "github.com/deckhouse/sds-replicated-volume/api/v1alpha1"
)

// podCycleScore runs filter then prioritize for the Pod on node-a, as a real cycle or a dry run,
// on a fresh cache, and returns the score of node-a.
func podCycleScore(t *testing.T, pvcNames []string, dryRun bool, objects ...client.Object) int {
	t.Helper()
	nodeNames := []string{"node-a"}
	pod := podWithPVCs("", pvcNames...)

	s := newTestScheduler(newFakeClient(objects...), newTestCache())
	s.targetProvisioners = []string{consts.SdsLocalVolumeProvisioner, consts.SdsReplicatedVolumeProvisioner}
	filtered := callFilter(t, s, ExtenderArgs{Pod: pod, NodeNames: &nodeNames, DryRun: dryRun})
	require.NotNil(t, filtered.NodeNames)
	require.Equal(t, nodeNames, *filtered.NodeNames, "failed nodes: %v", filtered.FailedNodes)
	scores := callPrioritize(t, s, ExtenderArgs{Pod: pod, NodeNames: filtered.NodeNames, DryRun: dryRun})
	require.Len(t, scores, 1)
	return scores[0].Score
}

// lvg-a (100 GiB, 60 GiB free) serves a local StorageClass and an RSP at once. The Pod has a
// pending local PVC and a pending replicated PVC, 20 GiB each. A real filter reserves the local
// PVC before prioritize, a dry run reserves nothing. Both PVCs see the pool after the whole Pod
// is placed: (60-20-20)/100 = 20% -> 4, in both cycles.
func TestPrioritize_LocalAndReplicatedPVCsOnSharedLVG(t *testing.T) {
	objects := func() []client.Object {
		return []client.Object{
			localSCWithLVGs("local-sc", "lvg-a"),
			testSC("repl-sc", consts.SdsReplicatedVolumeProvisioner),
			testRSC("repl-sc", srv.VolumeAccessPreferablyLocal, "repl-pool"),
			testRSP("repl-pool", srv.ReplicatedStoragePoolTypeLVM, "lvg-a"),
			readyLVGOnNode("lvg-a", "node-a", hundredGiB, 60*oneGiB),
			testNode("node-a", true),
			pendingPVCWithSize("pvc-local", "default", "local-sc", 20*oneGiB),
			pendingPVCWithSize("pvc-repl", "default", "repl-sc", 20*oneGiB),
			testNewControlPlaneMC(),
		}
	}
	pvcs := []string{"pvc-local", "pvc-repl"}

	assert.Equal(t, 4, podCycleScore(t, pvcs, true, objects()...), "dry run")
	assert.Equal(t, 4, podCycleScore(t, pvcs, false, objects()...), "real cycle")
}

// Two pending replicated 20 GiB PVCs of one Pod in one RSP, pool 100 GiB with 60 GiB free:
// each sees the pool after both are placed, (60-20-20)/100 = 20% -> 4, like two local PVCs.
func TestPrioritize_TwoPendingReplicatedPVCsInOnePool(t *testing.T) {
	objects := func() []client.Object {
		return []client.Object{
			testSC("repl-sc", consts.SdsReplicatedVolumeProvisioner),
			testRSC("repl-sc", srv.VolumeAccessPreferablyLocal, "repl-pool"),
			testRSP("repl-pool", srv.ReplicatedStoragePoolTypeLVM, "lvg-a"),
			readyLVGOnNode("lvg-a", "node-a", hundredGiB, 60*oneGiB),
			testNode("node-a", true),
			pendingPVCWithSize("pvc-1", "default", "repl-sc", 20*oneGiB),
			pendingPVCWithSize("pvc-2", "default", "repl-sc", 20*oneGiB),
			testNewControlPlaneMC(),
		}
	}
	pvcs := []string{"pvc-1", "pvc-2"}

	assert.Equal(t, 4, podCycleScore(t, pvcs, true, objects()...), "dry run")
	assert.Equal(t, 4, podCycleScore(t, pvcs, false, objects()...), "real cycle")
}

// Two pending local 20 GiB PVCs of one Pod in one pool: the pool must hold both. With 30 GiB
// free each fits alone and the Pod does not, so the node is rejected; with 40 GiB it passes.
// A dry run answers the same.
func TestFilter_PoolMustHoldAllPodPVCs(t *testing.T) {
	sc := localSCWithLVGs("local-sc", "lvg-a")
	nodeNames := []string{"node-a"}
	pod := podWithPVCs("", "pvc-1", "pvc-2")

	for _, tc := range []struct {
		free   int64
		passes bool
	}{{30 * oneGiB, false}, {40 * oneGiB, true}} {
		for _, dryRun := range []bool{true, false} {
			cl := newFakeClient(sc,
				pendingPVCWithSize("pvc-1", "default", sc.Name, 20*oneGiB),
				pendingPVCWithSize("pvc-2", "default", sc.Name, 20*oneGiB),
				readyLVGOnNode("lvg-a", "node-a", hundredGiB, tc.free))
			s := newTestScheduler(cl, newTestCache())
			s.targetProvisioners = []string{consts.SdsLocalVolumeProvisioner}

			res := callFilter(t, s, ExtenderArgs{Pod: pod, NodeNames: &nodeNames, DryRun: dryRun})
			require.NotNil(t, res.NodeNames)
			if tc.passes {
				assert.Equal(t, nodeNames, *res.NodeNames, "free %d, dry run %v", tc.free, dryRun)
				continue
			}
			assert.Empty(t, *res.NodeNames, "free %d, dry run %v", tc.free, dryRun)
			assert.Contains(t, res.FailedNodes["node-a"], "and the Pod's other PVCs in it")
		}
	}
}

// filterNodeA runs filter for the Pod on node-a, as a real call or a dry run, and returns the
// failure reason of node-a, "" when it passes.
func filterNodeA(t *testing.T, pvcNames []string, dryRun bool, objects ...client.Object) string {
	t.Helper()
	nodeNames := []string{"node-a"}
	s := newTestScheduler(newFakeClient(objects...), newTestCache())
	s.targetProvisioners = []string{consts.SdsLocalVolumeProvisioner, consts.SdsReplicatedVolumeProvisioner}
	res := callFilter(t, s, ExtenderArgs{Pod: podWithPVCs("", pvcNames...), NodeNames: &nodeNames, DryRun: dryRun})
	require.NotNil(t, res.NodeNames)
	if len(*res.NodeNames) == 1 {
		return ""
	}
	return res.FailedNodes["node-a"]
}

// The pool must hold all the Pod's PVCs placed in it, replicated ones and local ones on an LVG
// shared with an RSP too. The reason names the first PVC of the pool by name, so it is stable.
func TestFilter_PoolMustHoldReplicatedAndSharedClaims(t *testing.T) {
	shared := func(free int64) []client.Object {
		return []client.Object{
			localSCWithLVGs("local-sc", "lvg-a"),
			testSC("repl-sc", consts.SdsReplicatedVolumeProvisioner),
			testRSC("repl-sc", srv.VolumeAccessEventuallyLocal, "repl-pool"),
			testRSP("repl-pool", srv.ReplicatedStoragePoolTypeLVM, "lvg-a"),
			readyLVGOnNode("lvg-a", "node-a", hundredGiB, free),
			testNode("node-a", true),
			pendingPVCWithSize("pvc-b-local", "default", "local-sc", 20*oneGiB),
			pendingPVCWithSize("pvc-a-repl", "default", "repl-sc", 50*oneGiB),
			testNewControlPlaneMC(),
		}
	}
	replicated := func(free int64) []client.Object {
		return []client.Object{
			testSC("repl-sc", consts.SdsReplicatedVolumeProvisioner),
			testRSC("repl-sc", srv.VolumeAccessEventuallyLocal, "repl-pool"),
			testRSP("repl-pool", srv.ReplicatedStoragePoolTypeLVM, "lvg-a"),
			readyLVGOnNode("lvg-a", "node-a", hundredGiB, free),
			testNode("node-a", true),
			pendingPVCWithSize("pvc-2", "default", "repl-sc", 20*oneGiB),
			pendingPVCWithSize("pvc-1", "default", "repl-sc", 20*oneGiB),
			testNewControlPlaneMC(),
		}
	}

	for _, tc := range []struct {
		name    string
		pvcs    []string
		objects func(free int64) []client.Object
		tight   int64
		roomy   int64
		reason  string
	}{
		{"local and replicated on a shared LVG", []string{"pvc-b-local", "pvc-a-repl"}, shared, 60 * oneGiB, 70 * oneGiB,
			"LVMVolumeGroup lvg-a does not have enough space for PVC pvc-a-repl and the Pod's other PVCs in it"},
		{"two replicated in one pool", []string{"pvc-2", "pvc-1"}, replicated, 30 * oneGiB, 40 * oneGiB,
			"LVMVolumeGroup lvg-a does not have enough space for PVC pvc-1 and the Pod's other PVCs in it"},
	} {
		for _, dryRun := range []bool{true, false} {
			assert.Contains(t, filterNodeA(t, tc.pvcs, dryRun, tc.objects(tc.tight)...), tc.reason, "%s, dry run %v", tc.name, dryRun)
			assert.Empty(t, filterNodeA(t, tc.pvcs, dryRun, tc.objects(tc.roomy)...), "%s, dry run %v", tc.name, dryRun)
		}
	}
}
