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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-common-lib/conditions"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/mock_utils"
)

type deleteThinPoolsFixture struct {
	r   *Reconciler
	mc  *mock_utils.MockCommands
	rec *events.FakeRecorder
	lvg *v1alpha1.LVMVolumeGroup
}

func newDeleteThinPoolsFixture(t *testing.T, pools ...string) *deleteThinPoolsFixture {
	t.Helper()
	ctrl := gomock.NewController(t)
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)
	rec := events.NewFakeRecorder(10)
	r.recorder = internal.WithoutProgressEvents(rec)

	lvg := thinPoolLVG(pools...)
	lvg.Finalizers = []string{internal.SdsNodeConfiguratorFinalizer}
	lvg.Spec.Type = internal.Local
	ctx := context.Background()
	require.NoError(t, r.cl.Create(ctx, lvg))
	require.NoError(t, r.cl.Delete(ctx, lvg))
	require.NoError(t, r.cl.Get(ctx, client.ObjectKey{Name: lvg.Name}, lvg))
	lvg.Status.Conditions = []v1.Condition{{
		Type:               internal.TypeVGConfigurationApplied,
		Status:             v1.ConditionFalse,
		Reason:             internal.ReasonTerminating,
		Message:            "trying to delete VG",
		LastTransitionTime: v1.Now(),
	}}
	require.NoError(t, r.cl.Status().Update(ctx, lvg))

	r.sdsCache.StoreVGs([]internal.VGData{{VGName: thinPoolTestVG, VGUUID: "vg-uuid"}}, bytes.Buffer{})
	r.sdsCache.StorePVs([]internal.PVData{{PVName: "/dev/sdb", VGName: thinPoolTestVG}}, bytes.Buffer{})

	return &deleteThinPoolsFixture{r: r, mc: mc, rec: rec, lvg: lvg}
}

func (f *deleteThinPoolsFixture) storeLVs(lvs ...internal.LVData) {
	f.r.sdsCache.StoreLVs(lvs, bytes.Buffer{})
}

func (f *deleteThinPoolsFixture) condition(t *testing.T) *v1.Condition {
	t.Helper()
	got := &v1alpha1.LVMVolumeGroup{}
	require.NoError(t, f.r.cl.Get(context.Background(), client.ObjectKey{Name: f.lvg.Name}, got))
	return conditions.Get(got.Status.Conditions, internal.TypeVGConfigurationApplied)
}

func drainEvents(rec *events.FakeRecorder) []string {
	var got []string
	for {
		select {
		case e := <-rec.Events:
			got = append(got, e)
		default:
			return got
		}
	}
}

func hiddenLV(name, attr string) internal.LVData {
	return internal.LVData{VGName: thinPoolTestVG, LVName: name, LVAttr: attr, LVSize: resource.MustParse("4Mi")}
}

func thinVolume(name, pool string) internal.LVData {
	return internal.LVData{VGName: thinPoolTestVG, LVName: name, LVAttr: "Vwi-a-tz--", PoolName: pool, LVSize: resource.MustParse("100Mi")}
}

func thickLV(name string) internal.LVData {
	return internal.LVData{VGName: thinPoolTestVG, LVName: name, LVAttr: "-wi-a-----", LVSize: resource.MustParse("1Gi")}
}

func TestDeleteRemovesTheOwnEmptyThinPoolBeforeTheVG(t *testing.T) {
	ctx := context.Background()
	f := newDeleteThinPoolsFixture(t, "tp1")
	f.storeLVs(
		thinPoolLV(thinPoolTestVG, "tp1", ""),
		hiddenLV("[tp1_tdata]", "Twi-ao----"),
		hiddenLV("[tp1_tmeta]", "ewi-ao----"),
		hiddenLV("[lvol0_pmspare]", "ewi-------"),
	)

	f.mc.EXPECT().RemoveEmptyThinPool(gomock.Any(), thinPoolTestVG, "tp1").Return(true, "lvremove", nil)

	requeue, err := f.r.reconcileLVGDeleteFunc(ctx, f.lvg)
	require.NoError(t, err)
	assert.True(t, requeue)
	assert.Empty(t, drainEvents(f.rec))

	f.storeLVs()
	f.mc.EXPECT().RemoveVG(thinPoolTestVG).Return("vgremove", nil)
	f.mc.EXPECT().RemovePV([]string{"/dev/sdb"}).Return("pvremove", nil)

	_, _ = f.r.reconcileLVGDeleteFunc(ctx, f.lvg)
	err = f.r.cl.Get(ctx, client.ObjectKey{Name: f.lvg.Name}, &v1alpha1.LVMVolumeGroup{})
	assert.True(t, apierrors.IsNotFound(err), "got %v", err)
}

func TestDeleteRemovesAPoolTaggedForTheLVGButDroppedFromTheSpec(t *testing.T) {
	f := newDeleteThinPoolsFixture(t)
	f.storeLVs(thinPoolLV(thinPoolTestVG, "dropped", declaredThinPoolTag(f.lvg.Name)))

	f.mc.EXPECT().RemoveEmptyThinPool(gomock.Any(), thinPoolTestVG, "dropped").Return(true, "lvremove", nil)

	requeue, err := f.r.reconcileLVGDeleteFunc(context.Background(), f.lvg)
	require.NoError(t, err)
	assert.True(t, requeue)
}

func TestDeleteKeepsWhatHoldsDataOrIsNotOwned(t *testing.T) {
	ctx := context.Background()
	f := newDeleteThinPoolsFixture(t, "tp-empty", "tp-busy")
	tag := declaredThinPoolTag(f.lvg.Name)
	busy := []internal.LVData{
		thinPoolLV(thinPoolTestVG, "tp-busy", tag),
		thinVolume("pvc-1", "tp-busy"),
		thinVolume("snap-1", "tp-busy"),
		thinPoolLV(thinPoolTestVG, "foreign", ""),
		thickLV("thick-1"),
		hiddenLV("[tp-busy_tdata]", "Twi-ao----"),
	}
	f.storeLVs(append([]internal.LVData{thinPoolLV(thinPoolTestVG, "tp-empty", "")}, busy...)...)

	f.mc.EXPECT().RemoveEmptyThinPool(gomock.Any(), thinPoolTestVG, "tp-empty").Return(true, "lvremove", nil)

	requeue, err := f.r.reconcileLVGDeleteFunc(ctx, f.lvg)
	require.NoError(t, err)
	assert.True(t, requeue)

	f.storeLVs(busy...)
	for range 2 {
		requeue, err = f.r.reconcileLVGDeleteFunc(ctx, f.lvg)
		require.NoError(t, err)
		assert.True(t, requeue)
	}

	c := f.condition(t)
	require.NotNil(t, c)
	assert.Equal(t, v1.ConditionFalse, c.Status)
	assert.Equal(t, internal.ReasonTerminating, c.Reason)
	assert.Contains(t, c.Message, "thin pool tp-busy holds pvc-1, snap-1")
	assert.Contains(t, c.Message, "thin pool foreign is not declared by this LVMVolumeGroup")
	assert.Contains(t, c.Message, "logical volume thick-1")
	assert.NotContains(t, c.Message, "tp-empty")
	assert.NotContains(t, c.Message, "tdata")

	got := drainEvents(f.rec)
	require.Len(t, got, 1)
	assert.Contains(t, got[0], "Warning "+internal.ReasonVGHasLogicalVolumes)
	assert.Contains(t, got[0], "thin pool tp-busy holds pvc-1, snap-1")

	assert.NoError(t, f.r.cl.Get(ctx, client.ObjectKey{Name: f.lvg.Name}, &v1alpha1.LVMVolumeGroup{}))
}

func TestDeleteWaitsWhenLVMKeepsAPoolTheCacheShowsEmpty(t *testing.T) {
	ctx := context.Background()
	f := newDeleteThinPoolsFixture(t, "tp1")
	f.storeLVs(thinPoolLV(thinPoolTestVG, "tp1", ""))

	f.mc.EXPECT().RemoveEmptyThinPool(gomock.Any(), thinPoolTestVG, "tp1").Return(false, "lvremove", nil).Times(2)

	for range 2 {
		requeue, err := f.r.reconcileLVGDeleteFunc(ctx, f.lvg)
		require.NoError(t, err)
		assert.True(t, requeue)
	}
	assert.Equal(t, "trying to delete VG", f.condition(t).Message)
	assert.Empty(t, drainEvents(f.rec))
}

func TestDeleteReportsAFailedPoolRemoval(t *testing.T) {
	f := newDeleteThinPoolsFixture(t, "tp1")
	f.storeLVs(thinPoolLV(thinPoolTestVG, "tp1", ""))

	f.mc.EXPECT().RemoveEmptyThinPool(gomock.Any(), thinPoolTestVG, "tp1").Return(false, "lvremove", errors.New("device busy"))

	requeue, err := f.r.reconcileLVGDeleteFunc(context.Background(), f.lvg)
	require.Error(t, err)
	assert.True(t, requeue)
	c := f.condition(t)
	assert.Equal(t, internal.ReasonTerminating, c.Reason)
	assert.Contains(t, c.Message, "unable to remove the empty thin pool tp1")
	assert.Contains(t, c.Message, "device busy")
}

func TestDeleteRemovesAPoolOnlyUnderTheSharedThinPoolLock(t *testing.T) {
	f := newDeleteThinPoolsFixture(t, "tp1")
	f.storeLVs(thinPoolLV(thinPoolTestVG, "tp1", ""))

	unlock, err := f.r.thinPools.Lock(context.Background(), thinPoolTestVG, "tp1")
	require.NoError(t, err)
	defer unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	requeue, err := f.r.reconcileLVGDeleteFunc(ctx, f.lvg)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.True(t, requeue)
}
