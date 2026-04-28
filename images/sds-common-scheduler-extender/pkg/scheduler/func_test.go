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
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/consts"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
)

func TestShouldProcessPod(t *testing.T) {
	log := logger.Logger{}
	ctx := context.Background()

	tt := []struct {
		name                  string
		pod                   *corev1.Pod
		objects               []runtime.Object
		targetProvisioner     string
		expectedShouldProcess bool
		expectedError         bool
	}{
		{
			name: "Provisioner in PVC annotations",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pod1",
					Namespace: "default",
				},
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{
						{
							Name: "volume1",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "pvc1",
								},
							},
						},
					},
				},
			},
			objects: []runtime.Object{
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pvc1",
						Namespace: "default",
						Annotations: map[string]string{
							"volume.beta.kubernetes.io/storage-provisioner": "my-provisioner",
						},
					},
				},
			},
			targetProvisioner:     "my-provisioner",
			expectedShouldProcess: true,
		},
		{
			name: "Provisioner in StorageClass",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pod2",
					Namespace: "default",
				},
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{
						{
							Name: "volume2",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "pvc2",
								},
							},
						},
					},
				},
			},
			objects: []runtime.Object{
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pvc2",
						Namespace: "default",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						StorageClassName: stringPtr("sc2"),
					},
				},
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "sc2",
					},
					Provisioner: "my-provisioner",
				},
			},
			targetProvisioner:     "my-provisioner",
			expectedShouldProcess: true,
		},
		{
			name: "Provisioner in PV",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pod3",
					Namespace: "default",
				},
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{
						{
							Name: "volume3",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "pvc3",
								},
							},
						},
					},
				},
			},
			objects: []runtime.Object{
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pvc3",
						Namespace: "default",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						VolumeName: "pv3",
					},
				},
				&corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv3",
					},
					Spec: corev1.PersistentVolumeSpec{
						PersistentVolumeSource: corev1.PersistentVolumeSource{
							CSI: &corev1.CSIPersistentVolumeSource{
								Driver: "my-provisioner",
							},
						},
					},
				},
			},
			targetProvisioner:     "my-provisioner",
			expectedShouldProcess: true,
		},
		{
			name: "No matching Provisioner",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pod4",
					Namespace: "default",
				},
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{
						{
							Name: "volume4",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "pvc4",
								},
							},
						},
					},
				},
			},
			objects: []runtime.Object{
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pvc4",
						Namespace: "default",
					},
				},
			},
			targetProvisioner:     "my-provisioner",
			expectedShouldProcess: false,
		},
		{
			name: "Error getting PVC",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pod5",
					Namespace: "default",
				},
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{
						{
							Name: "volume5",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "nonexistent-pvc",
								},
							},
						},
					},
				},
			},
			objects:               []runtime.Object{},
			targetProvisioner:     "my-provisioner",
			expectedShouldProcess: false,
			expectedError:         true,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			s := scheme.Scheme
			_ = corev1.AddToScheme(s)
			_ = storagev1.AddToScheme(s)

			cl := fake.NewFakeClient(tc.objects...)
			targetProvisioners := []string{tc.targetProvisioner}
			managedPVCs, err := getManagedPVCsFromPod(ctx, cl, log, tc.pod, targetProvisioners)
			if (err != nil) != tc.expectedError {
				t.Fatalf("Unexpected error: %v", err)
			}

			shouldProcess := len(managedPVCs) > 0
			if shouldProcess != tc.expectedShouldProcess {
				t.Errorf("Expected shouldProcess to be %v, but got %v", tc.expectedShouldProcess, shouldProcess)
			}
		})
	}
}

func TestTwoPVCsSameStorageClass_Filter(t *testing.T) {
	scName := "shared-sc"
	provisioner := consts.SdsLocalVolumeProvisioner

	sc := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: scName},
		Provisioner: provisioner,
		Parameters: map[string]string{
			consts.LvmTypeParamKey:         consts.Thick,
			consts.LVMVolumeGroupsParamKey: `[{"name":"lvg1"}]`,
		},
	}
	pvc1 := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc1", Namespace: "default"},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}
	pvc2 := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc2", Namespace: "default"},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}

	hundredGiB := int64(100 * 1024 * 1024 * 1024)
	cl := newFakeClient(sc, pvc1, pvc2, readyLVG("lvg1", hundredGiB, hundredGiB))
	c := newTestCache()
	s := newTestScheduler(cl, c)
	s.targetProvisioners = []string{provisioner}

	nodeNames := []string{"node1"}
	args := ExtenderArgs{
		Pod: &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: "default"},
			Spec: corev1.PodSpec{
				Volumes: []corev1.Volume{
					{Name: "v1", VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "pvc1"},
					}},
					{Name: "v2", VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "pvc2"},
					}},
				},
			},
		},
		NodeNames: &nodeNames,
	}

	body, err := json.Marshal(args)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/filter", bytes.NewReader(body))
	w := httptest.NewRecorder()
	s.filter(w, req)

	if w.Code == http.StatusInternalServerError {
		t.Fatalf("filter returned 500 for two PVCs sharing the same StorageClass; body: %s", w.Body.String())
	}
}

func TestTwoPVCsSameStorageClass_Prioritize(t *testing.T) {
	scName := "shared-sc"
	provisioner := consts.SdsLocalVolumeProvisioner

	sc := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: scName},
		Provisioner: provisioner,
		Parameters: map[string]string{
			consts.LvmTypeParamKey:         consts.Thick,
			consts.LVMVolumeGroupsParamKey: `[{"name":"lvg1"}]`,
		},
	}
	pvc1 := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc1", Namespace: "default"},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}
	pvc2 := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc2", Namespace: "default"},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}

	hundredGiB := int64(100 * 1024 * 1024 * 1024)
	cl := newFakeClient(sc, pvc1, pvc2, readyLVG("lvg1", hundredGiB, hundredGiB))
	c := newTestCache()
	s := newTestScheduler(cl, c)
	s.targetProvisioners = []string{provisioner}

	nodeNames := []string{"node1"}
	args := ExtenderArgs{
		Pod: &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: "default"},
			Spec: corev1.PodSpec{
				Volumes: []corev1.Volume{
					{Name: "v1", VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "pvc1"},
					}},
					{Name: "v2", VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "pvc2"},
					}},
				},
			},
		},
		NodeNames: &nodeNames,
	}

	body, err := json.Marshal(args)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/prioritize", bytes.NewReader(body))
	w := httptest.NewRecorder()
	s.prioritize(w, req)

	if w.Code == http.StatusInternalServerError {
		t.Fatalf("prioritize returned 500 for two PVCs sharing the same StorageClass; body: %s", w.Body.String())
	}
}

func TestFilter_MissingSC_RejectsAllNodes(t *testing.T) {
	scName := "non-existent-sc"
	provisioner := consts.SdsLocalVolumeProvisioner

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pvc1",
			Namespace: "default",
			Annotations: map[string]string{
				"volume.beta.kubernetes.io/storage-provisioner": provisioner,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
		},
	}

	cl := newFakeClient(pvc)
	c := newTestCache()
	s := newTestScheduler(cl, c)
	s.targetProvisioners = []string{provisioner}

	nodeNames := []string{"node1", "node2"}
	args := ExtenderArgs{
		Pod: &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: "default"},
			Spec: corev1.PodSpec{
				Volumes: []corev1.Volume{
					{Name: "v1", VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "pvc1"},
					}},
				},
			},
		},
		NodeNames: &nodeNames,
	}

	body, err := json.Marshal(args)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/filter", bytes.NewReader(body))
	w := httptest.NewRecorder()
	s.filter(w, req)

	assert.Equal(t, http.StatusOK, w.Code, "filter must return 200 so the scheduler does not ignore the response")

	var result ExtenderFilterResult
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
	assert.Empty(t, *result.NodeNames, "filtered node list must be empty")
	assert.Len(t, result.FailedNodes, 2, "all input nodes must be in FailedNodes")
	for _, nodeName := range nodeNames {
		reason, ok := result.FailedNodes[nodeName]
		assert.True(t, ok, "node %s must be in FailedNodes", nodeName)
		assert.Contains(t, reason, "StorageClass", "reason must mention StorageClass")
	}
}

func TestFilter_FailedExtractSize_RejectsAllNodes(t *testing.T) {
	scName := "bad-sc"
	provisioner := consts.SdsLocalVolumeProvisioner

	sc := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: scName},
		Provisioner: provisioner,
		Parameters:  map[string]string{},
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc1", Namespace: "default"},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}

	cl := newFakeClient(sc, pvc)
	c := newTestCache()
	s := newTestScheduler(cl, c)
	s.targetProvisioners = []string{provisioner}

	nodeNames := []string{"node1"}
	args := ExtenderArgs{
		Pod: &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: "default"},
			Spec: corev1.PodSpec{
				Volumes: []corev1.Volume{
					{Name: "v1", VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "pvc1"},
					}},
				},
			},
		},
		NodeNames: &nodeNames,
	}

	body, err := json.Marshal(args)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/filter", bytes.NewReader(body))
	w := httptest.NewRecorder()
	s.filter(w, req)

	assert.Equal(t, http.StatusOK, w.Code, "filter must return 200 so the scheduler does not ignore the response")

	var result ExtenderFilterResult
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
	assert.Empty(t, *result.NodeNames, "filtered node list must be empty")
	assert.NotEmpty(t, result.FailedNodes, "FailedNodes must contain a reason")
}

func stringPtr(s string) *string {
	return &s
}

// TestFilter_RawFileLocalSC_PassesAllNodes is a regression test for the bug
// where any PVC bound to a StorageClass with provisioner `local.csi.storage.deckhouse.io`
// but without `lvm-type` parameter caused the filter to fail with
// "unable to extract request size: [...] unable to determine device type for PVC ...",
// rejecting all candidate nodes.
//
// LocalStorageClass resources with `spec.rawFile` produce exactly such
// StorageClasses (LVM parameters are absent on purpose). For those PVCs the
// extender has nothing useful to compute -- placement is enforced by
// `allowedTopologies` on the StorageClass. The filter MUST return all input
// nodes unchanged.
func TestFilter_RawFileLocalSC_PassesAllNodes(t *testing.T) {
	scName := "rawfile-sc"
	sc := testLocalRawFileSC(scName)
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc1", Namespace: "default"},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}

	cl := newFakeClient(sc, pvc)
	c := newTestCache()
	s := newTestScheduler(cl, c)
	s.targetProvisioners = []string{consts.SdsLocalVolumeProvisioner}

	nodeNames := []string{"node1", "node2", "node3"}
	args := ExtenderArgs{
		Pod: &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: "default"},
			Spec: corev1.PodSpec{
				Volumes: []corev1.Volume{
					{Name: "v1", VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "pvc1"},
					}},
				},
			},
		},
		NodeNames: &nodeNames,
	}

	body, err := json.Marshal(args)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/filter", bytes.NewReader(body))
	w := httptest.NewRecorder()
	s.filter(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var result ExtenderFilterResult
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
	require.NotNil(t, result.NodeNames)
	assert.ElementsMatch(t, nodeNames, *result.NodeNames, "all input nodes must be returned unchanged")
	assert.Empty(t, result.FailedNodes, "no nodes must be marked as failed for a rawfile-only Pod")
}

// TestFilter_MixedLocalLVMAndRawFile_LVMRulesApply verifies that mixing an
// LVM-backed local PVC and a rawfile-backed local PVC on the same Pod does not
// regress the LVM behavior: LVG matching still happens for the LVM PVC, and
// the rawfile PVC is silently ignored.
func TestFilter_MixedLocalLVMAndRawFile_LVMRulesApply(t *testing.T) {
	rawFileSCName := "rawfile-sc"
	rawFileSC := testLocalRawFileSC(rawFileSCName)
	rawFilePVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "raw-pvc", Namespace: "default"},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &rawFileSCName,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending},
	}

	lvmSCName := "lvm-sc"
	lvmSC := testLocalSC(lvmSCName, "lvg1")
	lvmPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "lvm-pvc", Namespace: "default"},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &lvmSCName,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending},
	}

	// node1 has the matching LVG, node2 does not -- so the LVM PVC must
	// keep node1 and reject node2.
	const hundredGiB = int64(100 * 1024 * 1024 * 1024)
	cl := newFakeClient(rawFileSC, rawFilePVC, lvmSC, lvmPVC, readyLVGOnNode("lvg1", "node1", hundredGiB, hundredGiB))
	c := newTestCache()
	s := newTestScheduler(cl, c)
	s.targetProvisioners = []string{consts.SdsLocalVolumeProvisioner}

	nodeNames := []string{"node1", "node2"}
	args := ExtenderArgs{
		Pod: &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: "default"},
			Spec: corev1.PodSpec{
				Volumes: []corev1.Volume{
					{Name: "v-lvm", VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "lvm-pvc"},
					}},
					{Name: "v-raw", VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "raw-pvc"},
					}},
				},
			},
		},
		NodeNames: &nodeNames,
	}

	body, err := json.Marshal(args)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/filter", bytes.NewReader(body))
	w := httptest.NewRecorder()
	s.filter(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var result ExtenderFilterResult
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
	require.NotNil(t, result.NodeNames)
	assert.ElementsMatch(t, []string{"node1"}, *result.NodeNames, "only the node with a matching LVG must pass the LVM filter")
	if reason, ok := result.FailedNodes["node2"]; ok {
		assert.Contains(t, reason, "[local]", "node2 must be rejected by the LVM-aware filter, not by rawfile handling")
	}
}

// TestPrioritize_RawFileLocalSC_DoesNotError ensures the prioritize endpoint
// also tolerates rawfile-backed local PVCs (used to return 500 with the same
// "unable to extract request size" failure).
func TestPrioritize_RawFileLocalSC_DoesNotError(t *testing.T) {
	scName := "rawfile-sc"
	sc := testLocalRawFileSC(scName)
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc1", Namespace: "default"},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}

	cl := newFakeClient(sc, pvc)
	c := newTestCache()
	s := newTestScheduler(cl, c)
	s.targetProvisioners = []string{consts.SdsLocalVolumeProvisioner}

	nodeNames := []string{"node1", "node2"}
	args := ExtenderArgs{
		Pod: &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: "default"},
			Spec: corev1.PodSpec{
				Volumes: []corev1.Volume{
					{Name: "v1", VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "pvc1"},
					}},
				},
			},
		},
		NodeNames: &nodeNames,
	}

	body, err := json.Marshal(args)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/prioritize", bytes.NewReader(body))
	w := httptest.NewRecorder()
	s.prioritize(w, req)

	assert.NotEqual(t, http.StatusInternalServerError, w.Code, "prioritize must not return 500 for a rawfile-backed local SC")
}
