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
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	d8commonapi "github.com/deckhouse/sds-common-lib/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/consts"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
	srv "github.com/deckhouse/sds-replicated-volume/api/v1alpha1"
)

func TestPodExtraPVCsAnnotationKey(t *testing.T) {
	// The key is a cross-module contract (producer lives in the virtualization
	// module). Pin the exact string so a rename cannot happen silently.
	assert.Equal(t, "scheduler.deckhouse.io/extra-pvcs", consts.PodExtraPVCsAnnotation)
}

func TestParseExtraPVCNames(t *testing.T) {
	tt := []struct {
		name  string
		value string
		want  []string
	}{
		{name: "empty value", value: "", want: nil},
		{name: "single name", value: "pvc-a", want: []string{"pvc-a"}},
		{name: "two names", value: "pvc-a,pvc-b", want: []string{"pvc-a", "pvc-b"}},
		{name: "surrounding spaces are trimmed", value: " pvc-a , pvc-b ", want: []string{"pvc-a", "pvc-b"}},
		{name: "empty entries are ignored", value: ",,pvc-a,,pvc-b,", want: []string{"pvc-a", "pvc-b"}},
		{name: "only separators and spaces", value: " , , ", want: nil},
		{name: "order is preserved", value: "b,a,c", want: []string{"b", "a", "c"}},
		{name: "duplicates are kept (dedup happens later)", value: "pvc-a,pvc-a", want: []string{"pvc-a", "pvc-a"}},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, parseExtraPVCNames(tc.value))
		})
	}
}

// podWithPVCs builds a Pod whose spec.volumes reference specPVCs and whose
// extra-pvcs annotation is set to annotation (omitted entirely when empty).
func podWithPVCs(annotation string, specPVCs ...string) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: "default"},
	}
	for i, name := range specPVCs {
		pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
			Name: fmt.Sprintf("v%d", i),
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: name},
			},
		})
	}
	if annotation != "" {
		pod.Annotations = map[string]string{consts.PodExtraPVCsAnnotation: annotation}
	}
	return pod
}

func TestPodPVCNames(t *testing.T) {
	t.Run("no annotation returns only spec volumes", func(t *testing.T) {
		names, fromSpec := podPVCNames(podWithPVCs("", "pvc-a", "pvc-b"))
		assert.Equal(t, []string{"pvc-a", "pvc-b"}, names)
		assert.Equal(t, map[string]bool{"pvc-a": true, "pvc-b": true}, fromSpec)
	})

	t.Run("annotation names are appended after spec names", func(t *testing.T) {
		names, fromSpec := podPVCNames(podWithPVCs("pvc-x,pvc-y", "pvc-a"))
		assert.Equal(t, []string{"pvc-a", "pvc-x", "pvc-y"}, names)
		assert.True(t, fromSpec["pvc-a"])
		assert.False(t, fromSpec["pvc-x"])
	})

	t.Run("a name in both sources appears once and counts as spec-sourced", func(t *testing.T) {
		names, fromSpec := podPVCNames(podWithPVCs("pvc-a", "pvc-a"))
		assert.Equal(t, []string{"pvc-a"}, names)
		assert.True(t, fromSpec["pvc-a"], "spec origin must win: a missing spec PVC still has to fail the request")
	})

	t.Run("non-PVC volumes are ignored", func(t *testing.T) {
		pod := podWithPVCs("", "pvc-a")
		pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
			Name:         "scratch",
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		})
		names, _ := podPVCNames(pod)
		assert.Equal(t, []string{"pvc-a"}, names)
	})
}

func TestGetManagedPVCsFromPod_ExtraPVCsAnnotation(t *testing.T) {
	log, err := logger.NewLogger("0")
	require.NoError(t, err)
	ctx := context.Background()

	const (
		localSCName   = "e2e-local-sc"
		foreignSCName = "foreign-sc"
	)

	// Objects shared by every case: a local SC our extender manages, a foreign SC
	// it must ignore, and one PVC for each.
	objects := func() []client.Object {
		return []client.Object{
			testLocalSC(localSCName, "lvg1"),
			testSC(foreignSCName, "foreign.csi.example.com"),
			testPendingPVC("pvc-spec", "default", localSCName),
			testPendingPVC("pvc-hint", "default", localSCName),
			testPendingPVC("pvc-hint-2", "default", localSCName),
			testPendingPVC("pvc-foreign", "default", foreignSCName),
		}
	}

	tt := []struct {
		name         string
		pod          *corev1.Pod
		want         []string
		wantHintOnly []string
		wantError    bool
	}{
		{
			name: "no annotation: only spec volumes (backward compatibility)",
			pod:  podWithPVCs("", "pvc-spec"),
			want: []string{"pvc-spec"},
		},
		{
			name: "empty annotation value: only spec volumes",
			pod:  podWithPVCs("   ", "pvc-spec"),
			want: []string{"pvc-spec"},
		},
		{
			name:         "one annotation PVC, no spec volumes (the virt-launcher case)",
			pod:          podWithPVCs("pvc-hint"),
			want:         []string{"pvc-hint"},
			wantHintOnly: []string{"pvc-hint"},
		},
		{
			name:         "several annotation PVCs",
			pod:          podWithPVCs("pvc-hint,pvc-hint-2"),
			want:         []string{"pvc-hint", "pvc-hint-2"},
			wantHintOnly: []string{"pvc-hint", "pvc-hint-2"},
		},
		{
			name:         "spec and annotation combined",
			pod:          podWithPVCs("pvc-hint", "pvc-spec"),
			want:         []string{"pvc-spec", "pvc-hint"},
			wantHintOnly: []string{"pvc-hint"},
		},
		{
			name: "duplicate between spec and annotation is deduplicated and counts as spec-sourced",
			pod:  podWithPVCs("pvc-spec", "pvc-spec"),
			want: []string{"pvc-spec"},
		},
		{
			name:         "annotation PVC that does not exist is skipped without error",
			pod:          podWithPVCs("pvc-hint,pvc-does-not-exist"),
			want:         []string{"pvc-hint"},
			wantHintOnly: []string{"pvc-hint"},
		},
		{
			name: "annotation PVC of a foreign provisioner is filtered out",
			pod:  podWithPVCs("pvc-foreign"),
			want: nil,
		},
		{
			name: "malformed annotation value does not panic and adds nothing",
			pod:  podWithPVCs(",, ,", "pvc-spec"),
			want: []string{"pvc-spec"},
		},
		{
			name:      "missing spec PVC still fails the request (unchanged behavior)",
			pod:       podWithPVCs("", "pvc-does-not-exist"),
			wantError: true,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			cl := newFakeClient(objects()...)
			managed, hintOnly, err := getManagedPVCsFromPod(ctx, cl, log, tc.pod,
				[]string{consts.SdsLocalVolumeProvisioner})

			if tc.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			got := make([]string, 0, len(managed))
			for name := range managed {
				got = append(got, name)
			}
			sort.Strings(got)

			gotHintOnly := make([]string, 0, len(hintOnly))
			for name := range hintOnly {
				gotHintOnly = append(gotHintOnly, name)
			}
			sort.Strings(gotHintOnly)

			if len(tc.wantHintOnly) == 0 {
				assert.Empty(t, gotHintOnly, "no PVC should be marked annotation-only")
			} else {
				wantHintOnly := append([]string(nil), tc.wantHintOnly...)
				sort.Strings(wantHintOnly)
				assert.Equal(t, wantHintOnly, gotHintOnly)
			}

			// got is always non-nil (make with len 0), so compare emptiness
			// explicitly instead of letting assert.Equal see []string{} vs nil.
			if len(tc.want) == 0 {
				assert.Empty(t, got)
				return
			}
			want := append([]string(nil), tc.want...)
			sort.Strings(want)
			assert.Equal(t, want, got)
		})
	}
}

// callFilter posts args to the /filter handler and returns the decoded result.
func callFilter(t *testing.T, s *scheduler, args ExtenderArgs) ExtenderFilterResult {
	t.Helper()
	body, err := json.Marshal(args)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/scheduler/filter", bytes.NewReader(body))
	w := httptest.NewRecorder()
	s.filter(w, req)

	require.Equal(t, http.StatusOK, w.Code, "filter must answer 200; body: %s", w.Body.String())
	var result ExtenderFilterResult
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
	return result
}

// TestFilter_AnnotationPVC_LocalRejectsNodeWithoutSpace is the core proof of the
// feature: a Pod with an EMPTY spec.volumes (the virt-launcher shape) and only the
// extra-pvcs annotation must still be filtered by free space, and must get a
// reservation on the surviving node.
func TestFilter_AnnotationPVC_LocalRejectsNodeWithoutSpace(t *testing.T) {
	const scName = "local-sc"

	sc := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: scName},
		Provisioner: consts.SdsLocalVolumeProvisioner,
		Parameters: map[string]string{
			consts.LvmTypeParamKey:         consts.Thick,
			consts.LVMVolumeGroupsParamKey: "- name: lvg-a\n- name: lvg-b\n",
		},
	}

	cl := newFakeClient(
		sc,
		pendingPVCWithSize("pvc-hotplug", "default", scName, tenGiB),
		readyLVGOnNode("lvg-a", "node-a", hundredGiB, hundredGiB),
		readyLVGOnNode("lvg-b", "node-b", hundredGiB, oneGiB),
	)
	c := newTestCache()
	s := newTestScheduler(cl, c)
	s.targetProvisioners = []string{consts.SdsLocalVolumeProvisioner}

	nodeNames := []string{"node-a", "node-b"}
	result := callFilter(t, s, ExtenderArgs{
		Pod: &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "virt-launcher",
				Namespace:   "default",
				Annotations: map[string]string{consts.PodExtraPVCsAnnotation: "pvc-hotplug"},
			},
			// No Volumes on purpose: KubeVirt keeps hotplug disks out of the launcher spec.
		},
		NodeNames: &nodeNames,
	})

	require.NotNil(t, result.NodeNames)
	assert.Equal(t, []string{"node-a"}, *result.NodeNames, "only the node with free space must survive")
	assert.Contains(t, result.FailedNodes["node-b"], "does not have enough space for PVC",
		"the rejection reason must name the space problem so it surfaces in the Pod's FailedScheduling event")

	assert.True(t, c.HasReservation("default/pvc-hotplug"),
		"the annotation PVC must get the same 60s reservation a spec PVC gets: reservation is built "+
			"into filter (createReservations), so it follows automatically once the PVC is managed")
}

// TestFilter_NoAnnotation_PodWithoutPVCsIsNoOp pins backward compatibility at the
// handler level: without the annotation, a Pod with no PVCs gets every node back
// and no reservation is created.
func TestFilter_NoAnnotation_PodWithoutPVCsIsNoOp(t *testing.T) {
	const scName = "local-sc"

	cl := newFakeClient(
		testLocalSC(scName, "lvg-a"),
		pendingPVCWithSize("pvc-hotplug", "default", scName, tenGiB),
		readyLVGOnNode("lvg-a", "node-a", hundredGiB, hundredGiB),
		readyLVGOnNode("lvg-b", "node-b", hundredGiB, oneGiB),
	)
	c := newTestCache()
	s := newTestScheduler(cl, c)
	s.targetProvisioners = []string{consts.SdsLocalVolumeProvisioner}

	nodeNames := []string{"node-a", "node-b"}
	result := callFilter(t, s, ExtenderArgs{
		Pod: &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "virt-launcher", Namespace: "default"},
		},
		NodeNames: &nodeNames,
	})

	require.NotNil(t, result.NodeNames)
	assert.ElementsMatch(t, nodeNames, *result.NodeNames, "without the annotation the extender must stay a no-op")
	assert.Empty(t, result.FailedNodes)
	assert.False(t, c.HasReservation("default/pvc-hotplug"))
}

// TestFilter_AnnotationPVC_ReplicatedLocalAccess covers the replicated side of the
// feature: an unbound replicated PVC with volumeAccess=Local must space-reject nodes
// that carry no LVG from the storage pool. The e2e stand installs only
// sds-local-volume, so this handler-level test is the only coverage of that path.
func TestFilter_AnnotationPVC_ReplicatedLocalAccess(t *testing.T) {
	const (
		scName  = "repl-sc"
		rspName = "repl-pool"
	)

	cl := newFakeClient(
		testSC(scName, consts.SdsReplicatedVolumeProvisioner),
		testRSC(scName, srv.VolumeAccessLocal, rspName),
		testRSP(rspName, srv.ReplicatedStoragePoolTypeLVM, "lvg-a"),
		readyLVGOnNode("lvg-a", "node-a", hundredGiB, hundredGiB),
		testNode("node-a", true),
		testNode("node-b", true),
		pendingPVCWithSize("pvc-hotplug", "default", scName, tenGiB),
		&d8commonapi.ModuleConfig{
			ObjectMeta: metav1.ObjectMeta{Name: "sds-replicated-volume"},
			Spec: d8commonapi.ModuleConfigSpec{
				Settings: d8commonapi.SettingsValues{"newControlPlane": true},
			},
		},
	)
	c := newTestCache()
	s := newTestScheduler(cl, c)
	s.targetProvisioners = []string{consts.SdsReplicatedVolumeProvisioner}

	nodeNames := []string{"node-a", "node-b"}
	result := callFilter(t, s, ExtenderArgs{
		Pod: &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "virt-launcher",
				Namespace:   "default",
				Annotations: map[string]string{consts.PodExtraPVCsAnnotation: "pvc-hotplug"},
			},
		},
		NodeNames: &nodeNames,
	})

	require.NotNil(t, result.NodeNames)
	assert.Equal(t, []string{"node-a"}, *result.NodeNames,
		"only the node carrying an LVG of the replicated storage pool must survive")
	assert.Contains(t, result.FailedNodes["node-b"], "no LVG from RSP repl-pool found on node node-b")
}

// TestFilter_AnnotationPVC_UnresolvableIsIgnored is the counterpart of the core
// test: the annotation is a hint, so a PVC the extender cannot resolve must never
// cost the Pod its nodes. Here the SC lacks the lvm-type parameter, which makes
// extractRequestedSize fail; before the hint origin was propagated that error
// reached writeFailAllNodesResponse and left the launcher unschedulable forever.
func TestFilter_AnnotationPVC_UnresolvableIsIgnored(t *testing.T) {
	const scName = "local-sc-no-lvm-type"

	sc := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: scName},
		Provisioner: consts.SdsLocalVolumeProvisioner,
		Parameters: map[string]string{
			// No LvmTypeParamKey on purpose.
			consts.LVMVolumeGroupsParamKey: "- name: lvg-a\n",
		},
	}

	cl := newFakeClient(
		sc,
		pendingPVCWithSize("pvc-hotplug", "default", scName, tenGiB),
		readyLVGOnNode("lvg-a", "node-a", hundredGiB, hundredGiB),
		readyLVGOnNode("lvg-b", "node-b", hundredGiB, hundredGiB),
	)
	c := newTestCache()
	s := newTestScheduler(cl, c)
	s.targetProvisioners = []string{consts.SdsLocalVolumeProvisioner}

	nodeNames := []string{"node-a", "node-b"}
	result := callFilter(t, s, ExtenderArgs{
		Pod: &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "virt-launcher",
				Namespace:   "default",
				Annotations: map[string]string{consts.PodExtraPVCsAnnotation: "pvc-hotplug"},
			},
		},
		NodeNames: &nodeNames,
	})

	require.NotNil(t, result.NodeNames)
	assert.ElementsMatch(t, nodeNames, *result.NodeNames,
		"an unresolvable hint PVC must degrade to the pre-annotation behavior, not reject every node")
	assert.Empty(t, result.FailedNodes)
	assert.False(t, c.HasReservation("default/pvc-hotplug"),
		"a dropped hint PVC must not hold space in the cache")
}

// callPrioritize posts args to the /prioritize handler and returns the decoded result.
func callPrioritize(t *testing.T, s *scheduler, args ExtenderArgs) HostPriorityList {
	t.Helper()
	body, err := json.Marshal(args)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/scheduler/prioritize", bytes.NewReader(body))
	w := httptest.NewRecorder()
	s.prioritize(w, req)

	require.Equal(t, http.StatusOK, w.Code, "prioritize must answer 200; body: %s", w.Body.String())
	var result HostPriorityList
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
	return result
}

// TestPrioritize_AnnotationPVC_ScoresNodes covers the second handler: the PR claims
// prioritize inherits the annotation "for free", and unlike filter it has its own
// error branch and its own cache step (narrowReservationsToFinalNodes).
func TestPrioritize_AnnotationPVC_ScoresNodes(t *testing.T) {
	const scName = "local-sc"

	sc := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: scName},
		Provisioner: consts.SdsLocalVolumeProvisioner,
		Parameters: map[string]string{
			consts.LvmTypeParamKey:         consts.Thick,
			consts.LVMVolumeGroupsParamKey: "- name: lvg-a\n- name: lvg-b\n",
		},
	}

	cl := newFakeClient(
		sc,
		pendingPVCWithSize("pvc-hotplug", "default", scName, tenGiB),
		readyLVGOnNode("lvg-a", "node-a", hundredGiB, hundredGiB),
		readyLVGOnNode("lvg-b", "node-b", hundredGiB, 20*oneGiB),
	)
	c := newTestCache()
	s := newTestScheduler(cl, c)
	s.targetProvisioners = []string{consts.SdsLocalVolumeProvisioner}

	nodeNames := []string{"node-a", "node-b"}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "virt-launcher",
			Namespace:   "default",
			Annotations: map[string]string{consts.PodExtraPVCsAnnotation: "pvc-hotplug"},
		},
	}

	// filter runs first in a real scheduling cycle and is what creates the
	// reservation; prioritize only scores and narrows it.
	filtered := callFilter(t, s, ExtenderArgs{Pod: pod, NodeNames: &nodeNames})
	require.NotNil(t, filtered.NodeNames)
	require.ElementsMatch(t, nodeNames, *filtered.NodeNames)
	_, pools, ok := c.GetReservation("default/pvc-hotplug")
	require.True(t, ok, "filter must reserve for the annotation PVC")
	require.Len(t, pools, 2, "the reservation starts spread over every surviving node")

	scores := callPrioritize(t, s, ExtenderArgs{Pod: pod, NodeNames: &nodeNames})

	require.Len(t, scores, 2, "both nodes must be scored")
	byNode := make(map[string]int, len(scores))
	for _, s := range scores {
		byNode[s.Host] = s.Score
	}
	require.Contains(t, byNode, "node-a")
	require.Contains(t, byNode, "node-b")
	assert.GreaterOrEqual(t, byNode["node-a"], byNode["node-b"],
		"the node with more free space must not score lower")

	// narrowReservationsToFinalNodes must work for an annotation PVC too. It only
	// ever narrows down to the final node list — narrowing to the single chosen
	// node needs selected-node, which a Pod that does not mount the PVC never gets.
	oneNode := []string{"node-a"}
	callPrioritize(t, s, ExtenderArgs{Pod: pod, NodeNames: &oneNode})
	_, pools, ok = c.GetReservation("default/pvc-hotplug")
	require.True(t, ok, "narrowing must not drop the reservation")
	assert.Len(t, pools, 1, "the reservation must follow the final node list")
}
