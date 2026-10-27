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
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/cache"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/consts"
	srv "github.com/deckhouse/sds-replicated-volume/api/v1alpha1"
)

// The real Pod's reservation is added with a TTL shorter than the one filter and
// prioritize set, so any write to it shows up as a changed ExpiresAt.
const realPodReservationTTL = 30 * time.Second

func TestExtenderArgs_DryRunWireName(t *testing.T) {
	var args ExtenderArgs
	require.NoError(t, json.Unmarshal([]byte(`{"pod":{},"dryRun":true}`), &args))
	assert.True(t, args.DryRun)

	body, err := json.Marshal(ExtenderArgs{Pod: &corev1.Pod{}})
	require.NoError(t, err)
	assert.NotContains(t, string(body), "dryRun", "a real cycle must not carry the field")
}

// dryRunScheduler builds one extender state: a VM Pod whose PVC already holds a
// reservation from its real scheduling cycle, plus another Pod's reservation.
func dryRunScheduler(t *testing.T, pvcSize int64, lvgFree map[string]int64, realPodPools []string) (*scheduler, *cache.Cache) {
	t.Helper()
	const scName = "local-sc"

	sc := localSCWithLVGs(scName, "lvg-a", "lvg-b", "lvg-c")
	cl := newFakeClient(
		sc,
		pendingPVCWithSize("pvc-vm", "default", scName, pvcSize),
		readyLVGOnNode("lvg-a", "node-a", hundredGiB, lvgFree["lvg-a"]),
		readyLVGOnNode("lvg-b", "node-b", hundredGiB, lvgFree["lvg-b"]),
		readyLVGOnNode("lvg-c", "node-c", hundredGiB, lvgFree["lvg-c"]),
	)

	c := newTestCache()
	pools := make([]cache.StoragePoolKey, 0, len(realPodPools))
	for _, name := range realPodPools {
		pools = append(pools, cache.StoragePoolKey{LVGName: name})
	}
	if len(pools) > 0 {
		c.AddReservation("default/pvc-vm", realPodReservationTTL, pvcSize, pools)
	}
	c.AddReservation("default/pvc-other", time.Minute, tenGiB, []cache.StoragePoolKey{{LVGName: "lvg-c"}})

	s := newTestScheduler(cl, c)
	s.targetProvisioners = []string{consts.SdsLocalVolumeProvisioner}
	return s, c
}

func vmPod() *corev1.Pod {
	return podWithPVCs("", "pvc-vm")
}

// reservationsSnapshot is GetAllReservations with pools sorted, so two snapshots
// compare with assert.Equal.
func reservationsSnapshot(c *cache.Cache) map[string]cache.ReservationInfo {
	all := c.GetAllReservations()
	for id, r := range all {
		sort.Slice(r.Pools, func(i, j int) bool { return r.Pools[i].String() < r.Pools[j].String() })
		all[id] = r
	}
	return all
}

func reservedPools(t *testing.T, c *cache.Cache, id string) []string {
	t.Helper()
	_, pools, ok := c.GetReservation(id)
	require.True(t, ok, "reservation %s must exist", id)
	names := make([]string, 0, len(pools))
	for _, p := range pools {
		names = append(names, p.String())
	}
	sort.Strings(names)
	return names
}

func TestFilter_DryRun_AnswersLikeRealFilterWithoutWrites(t *testing.T) {
	// node-a: 15 GiB free, 10 of them held by the real Pod's own reservation.
	// node-b: plenty of space. node-c: 15 GiB free, 10 held by another Pod.
	// Both calls leave the own reservation out, so node-a passes; the other
	// Pod's reservation still counts, so node-c is rejected.
	lvgFree := map[string]int64{"lvg-a": 15 * oneGiB, "lvg-b": hundredGiB, "lvg-c": 15 * oneGiB}
	nodeNames := []string{"node-a", "node-b", "node-c"}

	dryS, dryC := dryRunScheduler(t, tenGiB, lvgFree, []string{"lvg-a"})
	before := reservationsSnapshot(dryC)
	dry := callFilter(t, dryS, ExtenderArgs{Pod: vmPod(), NodeNames: &nodeNames, DryRun: true})

	realS, realC := dryRunScheduler(t, tenGiB, lvgFree, []string{"lvg-a"})
	realBefore := reservationsSnapshot(realC)
	realRes := callFilter(t, realS, ExtenderArgs{Pod: vmPod(), NodeNames: &nodeNames})

	require.NotNil(t, dry.NodeNames)
	require.NotNil(t, realRes.NodeNames)
	assert.ElementsMatch(t, *realRes.NodeNames, *dry.NodeNames, "dry run must keep the nodes a real filter keeps")
	assert.Equal(t, realRes.FailedNodes, dry.FailedNodes, "dry run must reject nodes with the same reasons")
	assert.ElementsMatch(t, []string{"node-a", "node-b"}, *dry.NodeNames)

	assert.Equal(t, before, reservationsSnapshot(dryC),
		"dry run must not add a reservation, replace the real Pod's one or reset its TTL")

	// The real call is unchanged: it replaces the reservation with the surviving
	// nodes' pools and restarts the 60s TTL.
	assert.Equal(t, []string{"lvg-a", "lvg-b"}, reservedPools(t, realC, "default/pvc-vm"))
	assert.Greater(t, realC.GetAllReservations()["default/pvc-vm"].ExpiresAt, realBefore["default/pvc-vm"].ExpiresAt)
}

func TestFilter_DryRun_PodWithoutReservationCreatesNone(t *testing.T) {
	lvgFree := map[string]int64{"lvg-a": hundredGiB, "lvg-b": hundredGiB, "lvg-c": hundredGiB}
	nodeNames := []string{"node-a", "node-b"}

	s, c := dryRunScheduler(t, tenGiB, lvgFree, nil)

	result := callFilter(t, s, ExtenderArgs{Pod: vmPod(), NodeNames: &nodeNames, DryRun: true})

	require.NotNil(t, result.NodeNames)
	assert.ElementsMatch(t, nodeNames, *result.NodeNames)
	assert.False(t, c.HasReservation("default/pvc-vm"), "a simulated Pod must not hold space on candidate nodes")
}

func TestPrioritize_DryRun_ScoresLikeRealPrioritizeWithoutNarrowing(t *testing.T) {
	lvgFree := map[string]int64{"lvg-a": hundredGiB, "lvg-b": 40 * oneGiB, "lvg-c": hundredGiB}
	// The real Pod's reservation spans three nodes; the final list has two, so a
	// real prioritize narrows it.
	realPools := []string{"lvg-a", "lvg-b", "lvg-c"}
	nodeNames := []string{"node-a", "node-b"}

	dryS, dryC := dryRunScheduler(t, tenGiB, lvgFree, realPools)
	before := reservationsSnapshot(dryC)
	dry := callPrioritize(t, dryS, ExtenderArgs{Pod: vmPod(), NodeNames: &nodeNames, DryRun: true})

	realS, realC := dryRunScheduler(t, tenGiB, lvgFree, realPools)
	realRes := callPrioritize(t, realS, ExtenderArgs{Pod: vmPod(), NodeNames: &nodeNames})

	assert.ElementsMatch(t, realRes, dry, "dry run must score nodes like a real prioritize")
	require.Len(t, dry, 2)

	assert.Equal(t, before, reservationsSnapshot(dryC),
		"dry run must not narrow the real Pod's reservation or reset its TTL")

	assert.Equal(t, []string{"lvg-a", "lvg-b"}, reservedPools(t, realC, "default/pvc-vm"),
		"a real prioritize still narrows the reservation to the final node list")
}

// TestPrioritize_DryRun_PendingPVCScoresLikeRealCycle covers a full filter then
// prioritize cycle. A real filter reserves the PVC on every surviving node before
// prioritize runs, a dry-run filter reserves nothing. Prioritize leaves the PVC's
// own reservation out, so both cycles count the request once. Pool 100 GiB,
// 60 GiB free, PVC 30 GiB: 60-30 = 30% left -> 5 (getFreeSpaceLeftPercent,
// getNodeScore).
func TestPrioritize_DryRun_PendingPVCScoresLikeRealCycle(t *testing.T) {
	lvgFree := map[string]int64{"lvg-a": 60 * oneGiB, "lvg-b": hundredGiB, "lvg-c": hundredGiB}
	nodeNames := []string{"node-a"}

	cycle := func(dryRun bool) int {
		s, _ := dryRunScheduler(t, 30*oneGiB, lvgFree, nil)
		filtered := callFilter(t, s, ExtenderArgs{Pod: vmPod(), NodeNames: &nodeNames, DryRun: dryRun})
		require.NotNil(t, filtered.NodeNames)
		require.Equal(t, nodeNames, *filtered.NodeNames)
		scores := callPrioritize(t, s, ExtenderArgs{Pod: vmPod(), NodeNames: filtered.NodeNames, DryRun: dryRun})
		require.Len(t, scores, 1)
		return scores[0].Score
	}

	dryScore, realScore := cycle(true), cycle(false)
	assert.Equal(t, realScore, dryScore, "dry run must score a pending PVC like a real cycle")
	assert.Equal(t, 5, dryScore)
}

func TestFilter_DryRun_ReplicatedPVCAnswersLikeRealFilter(t *testing.T) {
	const (
		scName  = "repl-sc"
		rspName = "repl-pool"
	)
	nodeNames := []string{"node-a", "node-b"}

	run := func(dryRun bool) (ExtenderFilterResult, *cache.Cache) {
		cl := newFakeClient(
			testSC(scName, consts.SdsReplicatedVolumeProvisioner),
			testRSC(scName, srv.VolumeAccessLocal, rspName),
			testRSP(rspName, srv.ReplicatedStoragePoolTypeLVM, "lvg-a"),
			readyLVGOnNode("lvg-a", "node-a", hundredGiB, hundredGiB),
			testNode("node-a", true),
			testNode("node-b", true),
			pendingPVCWithSize("pvc-vm", "default", scName, tenGiB),
			testNewControlPlaneMC(),
		)
		c := newTestCache()
		s := newTestScheduler(cl, c)
		s.targetProvisioners = []string{consts.SdsReplicatedVolumeProvisioner}
		// callFilter requires HTTP 200.
		return callFilter(t, s, ExtenderArgs{Pod: vmPod(), NodeNames: &nodeNames, DryRun: dryRun}), c
	}

	dry, dryC := run(true)
	realRes, _ := run(false)

	require.NotNil(t, dry.NodeNames)
	require.NotNil(t, realRes.NodeNames)
	assert.ElementsMatch(t, *realRes.NodeNames, *dry.NodeNames)
	assert.Equal(t, realRes.FailedNodes, dry.FailedNodes)
	assert.Equal(t, []string{"node-a"}, *dry.NodeNames)
	assert.Empty(t, dryC.GetAllReservations())
}

// /stat counts dry-run requests apart. Requests run in parallel, as net/http serves
// them, so `go test -race` catches a counter that is not atomic. The Pod has no PVCs:
// both handlers answer it with 200.
func TestStat_CountsDryRunRequests(t *testing.T) {
	s := newTestScheduler(newFakeClient(), newTestCache())
	nodeNames := []string{"node-a"}

	var wg sync.WaitGroup
	for i := range 20 {
		for _, path := range []string{"/scheduler/filter", "/scheduler/prioritize"} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				body, err := json.Marshal(ExtenderArgs{Pod: podWithPVCs(""), NodeNames: &nodeNames, DryRun: i%4 == 0})
				assert.NoError(t, err)
				w := httptest.NewRecorder()
				s.ServeHTTP(w, httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body)))
				assert.Equal(t, http.StatusOK, w.Code, path)
			}()
		}
	}
	wg.Wait()

	w := httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/stat", nil))
	assert.Contains(t, w.Body.String(),
		"Filter request count: 20 (real: 15, dry-run: 5), Prioritize request count: 20 (real: 15, dry-run: 5)")
}

// Two pending 20 GiB PVCs of one Pod in a 100 GiB pool with 60 GiB free. A real filter
// reserves both before prioritize, a dry run reserves nothing; prioritize accounts the Pod's
// own PVCs from the request, so both cycles see (60-20-20)/100 = 20% left -> 4.
func TestPrioritize_DryRun_TwoPendingPVCsScoreLikeRealCycle(t *testing.T) {
	sc := localSCWithLVGs("local-sc", "lvg-a")
	nodeNames := []string{"node-a"}
	pod := podWithPVCs("", "pvc-1", "pvc-2")

	cycle := func(dryRun bool) int {
		cl := newFakeClient(sc,
			pendingPVCWithSize("pvc-1", "default", sc.Name, 20*oneGiB),
			pendingPVCWithSize("pvc-2", "default", sc.Name, 20*oneGiB),
			readyLVGOnNode("lvg-a", "node-a", hundredGiB, 60*oneGiB))
		s := newTestScheduler(cl, newTestCache())
		s.targetProvisioners = []string{consts.SdsLocalVolumeProvisioner}
		filtered := callFilter(t, s, ExtenderArgs{Pod: pod, NodeNames: &nodeNames, DryRun: dryRun})
		require.NotNil(t, filtered.NodeNames)
		require.Equal(t, nodeNames, *filtered.NodeNames)
		scores := callPrioritize(t, s, ExtenderArgs{Pod: pod, NodeNames: filtered.NodeNames, DryRun: dryRun})
		require.Len(t, scores, 1)
		return scores[0].Score
	}

	assert.Equal(t, 4, cycle(true), "dry run")
	assert.Equal(t, 4, cycle(false), "real cycle")
}
