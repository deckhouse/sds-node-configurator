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

package lvg

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-common-lib/conditions"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/mock_utils"
)

func TestServingLVGName(t *testing.T) {
	base := time.Date(2026, 11, 3, 0, 0, 0, 0, time.UTC)
	mk := func(name, node, vg string, age time.Duration) v1alpha1.LVMVolumeGroup {
		return v1alpha1.LVMVolumeGroup{
			ObjectMeta: metav1.ObjectMeta{Name: name, CreationTimestamp: metav1.NewTime(base.Add(-age))},
			Spec: v1alpha1.LVMVolumeGroupSpec{
				ActualVGNameOnTheNode: vg,
				Local:                 v1alpha1.LVMVolumeGroupLocalSpec{NodeName: node},
			},
		}
	}

	original := mk("vg-data-worker-1", "worker-1", "vg-data", time.Hour)
	copyOf := mk("vg-data-copy", "worker-1", "vg-data", time.Minute)
	otherNode := mk("vg-data-worker-2", "worker-2", "vg-data", 2*time.Hour)
	otherVG := mk("vg-logs-worker-1", "worker-1", "vg-logs", 2*time.Hour)
	all := []v1alpha1.LVMVolumeGroup{original, copyOf, otherNode, otherVG}

	tests := []struct {
		name     string
		lvg      v1alpha1.LVMVolumeGroup
		all      []v1alpha1.LVMVolumeGroup
		tagOwner string
		want     string
	}{
		{
			name: "the only resource of its Volume Group is served",
			lvg:  original,
			all:  []v1alpha1.LVMVolumeGroup{original, otherNode, otherVG},
			want: "",
		},
		{
			name:     "the copy yields to the resource the Volume Group is tagged for",
			lvg:      copyOf,
			all:      all,
			tagOwner: "vg-data-worker-1",
			want:     "vg-data-worker-1",
		},
		{
			name:     "the resource the Volume Group is tagged for is served",
			lvg:      original,
			all:      all,
			tagOwner: "vg-data-worker-1",
			want:     "",
		},
		{
			name:     "the tag wins over the age",
			lvg:      original,
			all:      all,
			tagOwner: "vg-data-copy",
			want:     "vg-data-copy",
		},
		{
			name: "with no tag the newer resource yields to the older one",
			lvg:  copyOf,
			all:  all,
			want: "vg-data-worker-1",
		},
		{
			name: "with no tag the older resource is served",
			lvg:  original,
			all:  all,
			want: "",
		},
		{
			name:     "a tag naming no resource of the Volume Group is ignored",
			lvg:      copyOf,
			all:      all,
			tagOwner: "vg-data-gone",
			want:     "vg-data-worker-1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, servingLVGName(&tc.lvg, tc.all, tc.tagOwner))
		})
	}

	t.Run("resources created in the same second are ordered by name", func(t *testing.T) {
		a := mk("vg-data-a", "worker-1", "vg-data", time.Minute)
		b := mk("vg-data-b", "worker-1", "vg-data", time.Minute)
		both := []v1alpha1.LVMVolumeGroup{a, b}
		assert.Equal(t, "", servingLVGName(&a, both, ""))
		assert.Equal(t, "vg-data-a", servingLVGName(&b, both, ""))
	})
}

// A second resource for a Volume Group the node already serves under another
// resource is refused before anything runs on the node, so the tag of the
// Volume Group is never moved to it.
func TestReconcileRefusesDuplicateLVG(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	// No host command is expected: the mock fails the test on any of them.
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	spec := v1alpha1.LVMVolumeGroupSpec{
		ActualVGNameOnTheNode: "data-vg",
		Type:                  internal.Local,
		Local:                 v1alpha1.LVMVolumeGroupLocalSpec{NodeName: "test_node"},
	}
	original := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "vg-original", Finalizers: []string{internal.SdsNodeConfiguratorFinalizer}},
		Spec:       spec,
	}
	duplicate := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "vg-duplicate", Finalizers: []string{internal.SdsNodeConfiguratorFinalizer}},
		Spec:       spec,
	}
	require.NoError(t, r.cl.Create(ctx, original))
	require.NoError(t, r.cl.Create(ctx, duplicate))

	r.sdsCache.StoreVGs([]internal.VGData{{
		VGName: "data-vg",
		VGUUID: "vg-uuid",
		VGTags: fmt.Sprintf("%s,%s=%s", internal.LVMTags[0], internal.LVMVolumeGroupTag, original.Name),
	}}, bytes.Buffer{})

	res, err := r.Reconcile(ctx, controller.ReconcileRequest[*v1alpha1.LVMVolumeGroup]{Object: duplicate})
	require.NoError(t, err)
	assert.Positive(t, res.RequeueAfter, "the duplicate is checked again in case the other resource goes away")

	got := &v1alpha1.LVMVolumeGroup{}
	require.NoError(t, r.cl.Get(ctx, client.ObjectKeyFromObject(duplicate), got))
	applied := conditions.Get(got.Status.Conditions, internal.TypeVGConfigurationApplied)
	require.NotNil(t, applied)
	assert.Equal(t, metav1.ConditionFalse, applied.Status)
	assert.Equal(t, internal.ReasonValidationFailed, applied.Reason)
	assert.Contains(t, applied.Message, "already described by the LVMVolumeGroup vg-original")
}
