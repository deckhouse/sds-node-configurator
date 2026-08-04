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
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/consts"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
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
		name      string
		pod       *corev1.Pod
		want      []string
		wantError bool
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
			name: "one annotation PVC, no spec volumes (the virt-launcher case)",
			pod:  podWithPVCs("pvc-hint"),
			want: []string{"pvc-hint"},
		},
		{
			name: "several annotation PVCs",
			pod:  podWithPVCs("pvc-hint,pvc-hint-2"),
			want: []string{"pvc-hint", "pvc-hint-2"},
		},
		{
			name: "spec and annotation combined",
			pod:  podWithPVCs("pvc-hint", "pvc-spec"),
			want: []string{"pvc-spec", "pvc-hint"},
		},
		{
			name: "duplicate between spec and annotation is deduplicated",
			pod:  podWithPVCs("pvc-spec", "pvc-spec"),
			want: []string{"pvc-spec"},
		},
		{
			name: "annotation PVC that does not exist is skipped without error",
			pod:  podWithPVCs("pvc-hint,pvc-does-not-exist"),
			want: []string{"pvc-hint"},
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
			managed, err := getManagedPVCsFromPod(ctx, cl, log, tc.pod,
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
