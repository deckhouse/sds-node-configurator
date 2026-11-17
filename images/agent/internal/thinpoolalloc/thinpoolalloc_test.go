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

package thinpoolalloc

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
)

type listerFunc func(vgName string) ([]internal.LVData, string, bytes.Buffer, error)

func (f listerFunc) GetVGLVs(vgName string) ([]internal.LVData, string, bytes.Buffer, error) {
	return f(vgName)
}

func testLVG() *v1alpha1.LVMVolumeGroup {
	return &v1alpha1.LVMVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "lvg"},
		Spec:       v1alpha1.LVMVolumeGroupSpec{ActualVGNameOnTheNode: "vg"},
		Status: v1alpha1.LVMVolumeGroupStatus{
			ThinPools: []v1alpha1.LVMVolumeGroupThinPoolStatus{{Name: "pool", ActualSize: resource.MustParse("1Gi"), AllocationLimit: "100%"}},
		},
	}
}

func listing(allocated string) listerFunc {
	return func(string) ([]internal.LVData, string, bytes.Buffer, error) {
		return []internal.LVData{
			{LVName: "pool", VGName: "vg", LVAttr: "twi-aotz--", LVSize: resource.MustParse("1Gi")},
			{LVName: "lv", VGName: "vg", PoolName: "pool", LVAttr: "Vwi-a-tz--", LVSize: resource.MustParse(allocated)},
			{LVName: "lv", VGName: "other-vg", PoolName: "pool", LVAttr: "Vwi-a-tz--", LVSize: resource.MustParse("1Gi")},
		}, "lvs", bytes.Buffer{}, nil
	}
}

func TestCheckSpaceRefusalIsErrNotEnoughSpace(t *testing.T) {
	err := CheckSpace(logger.NewNop(), listing("800Mi"), testLVG(), "pool", "new", "", resource.MustParse("400Mi"))
	require.ErrorIs(t, err, ErrNotEnoughSpace)
	assert.Equal(t,
		"not enough space: thin pool pool of VG vg has 800Mi allocated out of 1Gi allowed by allocationLimit 100% (pool size 1Gi), 224Mi left, 400Mi requested",
		err.Error())

	assert.NoError(t, CheckSpace(logger.NewNop(), listing("600Mi"), testLVG(), "pool", "new", "", resource.MustParse("400Mi")))
}

func TestCheckSpaceFailuresOtherThanSpaceAreNotErrNotEnoughSpace(t *testing.T) {
	failing := listerFunc(func(string) ([]internal.LVData, string, bytes.Buffer, error) {
		return nil, "lvs", bytes.Buffer{}, errors.New("exit status 5")
	})
	err := CheckSpace(logger.NewNop(), failing, testLVG(), "pool", "new", "", resource.MustParse("400Mi"))
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrNotEnoughSpace)

	unlisted := listerFunc(func(string) ([]internal.LVData, string, bytes.Buffer, error) {
		t.Fatal("lvs ran for a pool the LVMVolumeGroup does not have")
		return nil, "", bytes.Buffer{}, nil
	})
	err = CheckSpace(logger.NewNop(), unlisted, testLVG(), "missing", "new", "", resource.MustParse("400Mi"))
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrNotEnoughSpace)
}

func TestLocksSerializeOnePoolOnly(t *testing.T) {
	ctx := context.Background()
	var locks Locks

	unlock, err := locks.Lock(ctx, "vg", "pool")
	require.NoError(t, err)

	other, err := locks.Lock(ctx, "vg", "other-pool")
	require.NoError(t, err)
	other()

	acquired := make(chan func(), 1)
	go func() {
		next, err := locks.Lock(ctx, "vg", "pool")
		assert.NoError(t, err)
		acquired <- next
	}()
	require.Eventually(t, func() bool { return locks.Refs("vg", "pool") == 2 }, 5*time.Second, time.Millisecond)
	select {
	case <-acquired:
		t.Fatal("the pool was locked twice")
	default:
	}

	unlock()
	unlock()
	select {
	case next := <-acquired:
		next()
	case <-time.After(5 * time.Second):
		t.Fatal("the waiter did not get the released pool")
	}
	assert.Zero(t, locks.Len())
}
