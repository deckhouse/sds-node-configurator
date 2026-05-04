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

// TestFilter_MissingSC_PassesAllNodes asserts the regression fix: when the only
// managed PVC references a StorageClass that does not exist in the cluster
// (typical for statically provisioned PVs, including hostPath-backed PVs and
// PVs that survived their StorageClass being deleted), the extender must NOT
// fail-all-nodes. Instead, it must drop the PVC from its scheduling decision
// and return the input node list unchanged so that the upstream kube-scheduler
// (which does not require the SC object for bound PVCs either) can schedule
// the pod normally.
//
// See: https://kubernetes.io/docs/concepts/storage/persistent-volumes/#class
// "A PV of a particular class can only be bound to PVCs requesting that class."
// (storageClassName is a matching label, not a hard reference to a SC object).
func TestFilter_MissingSC_PassesAllNodes(t *testing.T) {
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

	assert.Equal(t, http.StatusOK, w.Code, "filter must return 200")

	var result ExtenderFilterResult
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
	require.NotNil(t, result.NodeNames, "node list must be present in the response")
	assert.ElementsMatch(t, nodeNames, *result.NodeNames, "all input nodes must be returned unchanged")
	assert.Empty(t, result.FailedNodes, "no nodes must be reported as failed")
}

// TestPrioritize_MissingSC_PassesAllNodes mirrors TestFilter_MissingSC_PassesAllNodes
// for the prioritize endpoint: a missing StorageClass must not cause the
// extender to fail the request; instead, it should return all input nodes with
// a neutral score so kube-scheduler can pick one.
func TestPrioritize_MissingSC_PassesAllNodes(t *testing.T) {
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

	req := httptest.NewRequest(http.MethodPost, "/prioritize", bytes.NewReader(body))
	w := httptest.NewRecorder()
	s.prioritize(w, req)

	assert.Equal(t, http.StatusOK, w.Code, "prioritize must return 200")

	var scores []HostPriority
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &scores))
	gotNodes := make([]string, 0, len(scores))
	for _, hp := range scores {
		gotNodes = append(gotNodes, hp.Host)
		assert.Equal(t, 0, hp.Score, "score for node %s must be 0 when there are no managed PVCs to score", hp.Host)
	}
	assert.ElementsMatch(t, nodeNames, gotNodes, "all input nodes must be scored")
}

// TestFilter_BoundStaticPV_NoSC verifies the realistic regression scenario:
// the pod uses a managed PVC that is already bound to a statically provisioned
// PV (via PVC annotation -> PV.Spec.CSI.Driver discovery); the storageClassName
// references a StorageClass object that no longer exists in the cluster
// (deleted, never created, or used as a static-binding marker).
//
// Expected: the extender returns the input nodes unchanged and lets upstream
// kube-scheduler pick one based on the PV's nodeAffinity.
func TestFilter_BoundStaticPV_NoSC(t *testing.T) {
	scName := "manual"
	provisioner := consts.SdsLocalVolumeProvisioner
	pvName := "pv-static-1"

	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: pvName},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:       provisioner,
					VolumeHandle: "vh-1",
				},
			},
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("1Gi"),
			},
			StorageClassName: scName,
		},
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc1", Namespace: "default"},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
			VolumeName:       pvName,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}

	cl := newFakeClient(pv, pvc)
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
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/filter", bytes.NewReader(body))
	w := httptest.NewRecorder()
	s.filter(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var result ExtenderFilterResult
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
	require.NotNil(t, result.NodeNames)
	assert.ElementsMatch(t, nodeNames, *result.NodeNames, "all input nodes must be returned for a statically bound PVC with no SC")
	assert.Empty(t, result.FailedNodes)
}

// TestFilter_MixedManagedPVCs_OneMissingSC verifies that a missing SC for one
// managed PVC must NOT prevent processing of other managed PVCs in the same
// pod. The pod has two managed PVCs: pvc-bad (SC missing) is dropped, and
// pvc-good (SC present, with a usable LVG) is processed normally.
func TestFilter_MixedManagedPVCs_OneMissingSC(t *testing.T) {
	scGood := "sc-good"
	scBad := "sc-missing"
	provisioner := consts.SdsLocalVolumeProvisioner

	scGoodObj := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: scGood},
		Provisioner: provisioner,
		Parameters: map[string]string{
			consts.LvmTypeParamKey:         consts.Thick,
			consts.LVMVolumeGroupsParamKey: "- name: lvg1\n",
		},
	}
	pvcGood := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-good", Namespace: "default"},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &scGood,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending},
	}
	pvcBad := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pvc-bad",
			Namespace: "default",
			Annotations: map[string]string{
				"volume.beta.kubernetes.io/storage-provisioner": provisioner,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &scBad,
		},
	}
	hundredGiB := int64(100 * 1024 * 1024 * 1024)
	lvg := readyLVGOnNode("lvg1", "node1", hundredGiB, hundredGiB)

	cl := newFakeClient(scGoodObj, pvcGood, pvcBad, lvg)
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
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "pvc-good"},
					}},
					{Name: "v2", VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "pvc-bad"},
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

	assert.Equal(t, http.StatusOK, w.Code, "filter must return 200")
	var result ExtenderFilterResult
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
	require.NotNil(t, result.NodeNames)
	// node1 hosts a usable LVG matching pvc-good's SC; node2 has no LVG and
	// must be filtered out by the local-PVC checks. The missing-SC PVC must
	// not cause node1 to be rejected.
	assert.Contains(t, *result.NodeNames, "node1", "node1 with a matching LVG must remain")
	assert.NotContains(t, *result.NodeNames, "node2", "node2 without any LVG must be filtered out by the local PVC")
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
