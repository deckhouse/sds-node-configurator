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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/mock_utils"
)

const thinPoolTestVG = "data-vg"

func thinPoolLVG(pools ...string) *v1alpha1.LVMVolumeGroup {
	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: "vg-a"},
		Spec: v1alpha1.LVMVolumeGroupSpec{
			ActualVGNameOnTheNode: thinPoolTestVG,
			Local:                 v1alpha1.LVMVolumeGroupLocalSpec{NodeName: "test_node"},
		},
		Status: v1alpha1.LVMVolumeGroupStatus{VGSize: resource.MustParse("10Gi")},
	}
	for _, p := range pools {
		lvg.Spec.ThinPools = append(lvg.Spec.ThinPools, v1alpha1.LVMVolumeGroupThinPoolSpec{Name: p, Size: "1Gi"})
	}
	return lvg
}

func thinPoolLV(vg, name, tags string) internal.LVData {
	return internal.LVData{VGName: vg, LVName: name, LVAttr: "twi-aotz--", LVSize: resource.MustParse("1Gi"), LvTags: tags}
}

func thinLV(vg, name, pool string) internal.LVData {
	return internal.LVData{VGName: vg, LVName: name, LVAttr: "Vwi-aotz--", PoolName: pool, LVSize: resource.MustParse("100Mi")}
}

// Only a pool the spec declared and no longer does, and that holds nothing, is
// removed. Everything else on the node is left as it is.
func TestRemoveThinPoolsDroppedFromSpec(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	lvg := thinPoolLVG("kept")
	tag := declaredThinPoolTag(lvg.Name)
	lvs := []internal.LVData{
		thinPoolLV(thinPoolTestVG, "kept", tag),
		thinPoolLV(thinPoolTestVG, "dropped-empty", tag),
		thinPoolLV(thinPoolTestVG, "dropped-busy", tag),
		thinLV(thinPoolTestVG, "pvc-1", "dropped-busy"),
		thinLV(thinPoolTestVG, "pvc-2", "dropped-busy"),
		thinPoolLV(thinPoolTestVG, "never-declared", ""),
		thinPoolLV("other-vg", "dropped-empty", tag),
	}

	mc.EXPECT().RemoveEmptyThinPool(gomock.Any(), thinPoolTestVG, "dropped-empty").Return(true, "lvremove", nil)

	kept, err := r.removeThinPoolsDroppedFromSpec(ctx, lvg, thinPoolTestVG, lvs)
	require.NoError(t, err)
	assert.Contains(t, kept, "dropped-busy")
	assert.Contains(t, kept, "pvc-1, pvc-2")
	assert.NotContains(t, kept, "never-declared")
	assert.NotContains(t, kept, "dropped-empty")
}

// A volume created after the cache was filled keeps the pool: LVM refuses, and
// the pool is reported as kept rather than taken for removed.
func TestRemoveThinPoolsDroppedFromSpecReportsAPoolLVMKept(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	lvg := thinPoolLVG()
	lvs := []internal.LVData{thinPoolLV(thinPoolTestVG, "dropped", declaredThinPoolTag(lvg.Name))}
	mc.EXPECT().RemoveEmptyThinPool(gomock.Any(), thinPoolTestVG, "dropped").Return(false, "lvremove", nil)

	kept, err := r.removeThinPoolsDroppedFromSpec(ctx, lvg, thinPoolTestVG, lvs)
	require.NoError(t, err)
	assert.Contains(t, kept, "thin pool dropped")
}

// Every pool the spec declares gets the mark, the ones created now and the ones
// that were there already; a pool of the same name in another Volume Group is
// not taken for this one's.
func TestReconcileThinPoolsTagsTheDeclaredPools(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	lvg := thinPoolLVG("existing", "tagged", "new")
	require.NoError(t, r.cl.Create(ctx, lvg))
	tag := declaredThinPoolTag(lvg.Name)
	vg := internal.VGData{VGName: thinPoolTestVG, VGSize: resource.MustParse("10Gi")}
	lvs := []internal.LVData{
		thinPoolLV(thinPoolTestVG, "existing", ""),
		thinPoolLV(thinPoolTestVG, "tagged", tag),
		thinPoolLV("other-vg", "new", tag),
	}

	mc.EXPECT().LVChangeAddTag(gomock.Any(), thinPoolTestVG, "existing", tag).Return("lvchange", nil)
	mc.EXPECT().CreateThinPool("new", thinPoolTestVG, gomock.Any()).Return("lvcreate", nil)
	mc.EXPECT().LVChangeAddTag(gomock.Any(), thinPoolTestVG, "new", tag).Return("lvchange", nil)

	require.NoError(t, r.reconcileThinPoolsIfNeeded(ctx, lvg, vg, lvs))
}
