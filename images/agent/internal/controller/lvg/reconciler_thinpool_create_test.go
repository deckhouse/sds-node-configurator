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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/mock_utils"
)

func createThinPoolsLVG(pools ...v1alpha1.LVMVolumeGroupThinPoolSpec) *v1alpha1.LVMVolumeGroup {
	lvg := &v1alpha1.LVMVolumeGroup{}
	lvg.Name = "lvg-create"
	lvg.Spec.ActualVGNameOnTheNode = thinPoolTestVG
	lvg.Spec.ThinPools = pools
	return lvg
}

// One thin pool that does not fit used to stop the loop, so the thin pools
// after it were never created and the whole LVMVolumeGroup went NotReady.
func TestCreateThinPoolsGoesOnAfterAFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	lvg := createThinPoolsLVG(
		v1alpha1.LVMVolumeGroupThinPoolSpec{Name: "too-big", Size: "8Gi"},
		v1alpha1.LVMVolumeGroupThinPoolSpec{Name: "fits", Size: "1Gi"},
	)

	gomock.InOrder(
		mc.EXPECT().CreateThinPool("too-big", thinPoolTestVG, gomock.Any()).Return("lvcreate", errors.New("insufficient free space")),
		mc.EXPECT().CreateThinPool("fits", thinPoolTestVG, gomock.Any()).Return("lvcreate", nil),
	)

	failed, err := r.createThinPools(lvg, resource.MustParse("10Gi"), resource.MustParse("4Mi"))
	require.NoError(t, err)
	assert.Contains(t, failed, "too-big")
	assert.Contains(t, failed, "insufficient free space")
	assert.NotContains(t, failed, "fits")
}

func TestCreateThinPoolsReportsNothingWhenAllAreCreated(t *testing.T) {
	ctrl := gomock.NewController(t)
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	lvg := createThinPoolsLVG(
		v1alpha1.LVMVolumeGroupThinPoolSpec{Name: "a", Size: "1Gi"},
		v1alpha1.LVMVolumeGroupThinPoolSpec{Name: "b", Size: "100%"},
	)

	mc.EXPECT().CreateThinPool("a", thinPoolTestVG, gomock.Any()).Return("lvcreate", nil)
	mc.EXPECT().CreateThinPoolFullVGSpace("b", thinPoolTestVG).Return("lvcreate", nil)

	failed, err := r.createThinPools(lvg, resource.MustParse("10Gi"), resource.MustParse("4Mi"))
	require.NoError(t, err)
	assert.Empty(t, failed)
}

// A size that cannot be worked out is a mistake in the spec, not a pool that
// did not fit, and no lvcreate is worth running after it.
func TestCreateThinPoolsStopsOnASizeItCannotRead(t *testing.T) {
	ctrl := gomock.NewController(t)
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	lvg := createThinPoolsLVG(v1alpha1.LVMVolumeGroupThinPoolSpec{Name: "bad", Size: "lots"})

	_, err := r.createThinPools(lvg, resource.MustParse("10Gi"), resource.MustParse("4Mi"))
	assert.Error(t, err)
}
