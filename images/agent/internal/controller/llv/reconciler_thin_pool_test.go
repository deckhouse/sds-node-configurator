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

package llv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/cache"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/mock_utils"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/monitoring"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/test_utils"
)

const (
	racePoolVG   = "race-vg"
	racePoolName = "race-pool"
	racePoolLVG  = "race-lvg"
)

type fakeThinPool struct {
	mu       sync.Mutex
	lvs      map[string]int64
	poolSize int64
	listErr  error
}

func (p *fakeThinPool) allocated() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	var sum int64
	for _, size := range p.lvs {
		sum += size
	}
	return sum
}

func (p *fakeThinPool) lv(name string) internal.LVData {
	p.mu.Lock()
	defer p.mu.Unlock()

	return internal.LVData{
		LVName:   name,
		VGName:   racePoolVG,
		PoolName: racePoolName,
		LVAttr:   "Vwi-a-tz--",
		LVSize:   *resource.NewQuantity(p.lvs[name], resource.BinarySI),
	}
}

func (p *fakeThinPool) listing() []internal.LVData {
	p.mu.Lock()
	defer p.mu.Unlock()

	lvs := []internal.LVData{
		{LVName: racePoolName, VGName: racePoolVG, LVAttr: "twi-aotz--", LVSize: *resource.NewQuantity(p.poolSize, resource.BinarySI)},
		{LVName: "other-pool-lv", VGName: racePoolVG, PoolName: "other-pool", LVAttr: "Vwi-a-tz--", LVSize: resource.MustParse("10Gi")},
		{LVName: "thick-lv", VGName: racePoolVG, LVAttr: "-wi-a-----", LVSize: resource.MustParse("10Gi")},
	}
	for name, size := range p.lvs {
		lvs = append(lvs, internal.LVData{
			LVName:   name,
			VGName:   racePoolVG,
			PoolName: racePoolName,
			LVAttr:   "Vwi-a-tz--",
			LVSize:   *resource.NewQuantity(size, resource.BinarySI),
		})
	}
	return lvs
}

type thinPoolRaceEnv struct {
	t           *testing.T
	r           *Reconciler
	cl          client.Client
	mockCmds    *mock_utils.MockCommands
	lvg         *v1alpha1.LVMVolumeGroup
	pool        *fakeThinPool
	queued      int
	consumeOnce sync.Once
}

func newThinPoolRaceEnv(t *testing.T, allocationLimit string, existing map[string]int64) *thinPoolRaceEnv {
	t.Helper()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	mockCmds := mock_utils.NewMockCommands(ctrl)
	cl := test_utils.NewFakeClient(&v1alpha1.LVMLogicalVolume{})
	r := NewReconciler(cl, logger.NewNop(), monitoring.GetMetrics(""), cache.New(), mockCmds, nil, ReconcilerConfig{})

	actualSize := resource.MustParse("1Gi")
	pool := &fakeThinPool{lvs: map[string]int64{}, poolSize: actualSize.Value()}
	for name, size := range existing {
		pool.lvs[name] = size
	}

	limitSize := limitedPoolSize(t, actualSize, allocationLimit)
	available := resource.NewQuantity(limitSize-pool.allocated(), resource.BinarySI)

	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{Name: racePoolLVG},
		Spec: v1alpha1.LVMVolumeGroupSpec{
			ActualVGNameOnTheNode: racePoolVG,
			Local:                 v1alpha1.LVMVolumeGroupLocalSpec{NodeName: "node-1"},
		},
		Status: v1alpha1.LVMVolumeGroupStatus{
			ExtentSize: resource.MustParse("4Mi"),
			VGSize:     resource.MustParse("100Gi"),
			VGFree:     resource.MustParse("100Gi"),
			ThinPools: []v1alpha1.LVMVolumeGroupThinPoolStatus{
				{
					Name:            racePoolName,
					ActualSize:      actualSize,
					AllocatedSize:   *resource.NewQuantity(pool.allocated(), resource.BinarySI),
					AvailableSpace:  *available,
					AllocationLimit: allocationLimit,
				},
			},
		},
	}

	env := &thinPoolRaceEnv{t: t, r: r, cl: cl, mockCmds: mockCmds, lvg: lvg, pool: pool}
	env.wireFakePool()

	return env
}

func limitedPoolSize(t *testing.T, actualSize resource.Quantity, allocationLimit string) int64 {
	t.Helper()

	var percent int64
	_, err := fmt.Sscanf(allocationLimit, "%d%%", &percent)
	require.NoError(t, err)

	return actualSize.Value() * percent / 100
}

func (e *thinPoolRaceEnv) waitForQueuedReconciles() {
	if e.queued == 0 {
		return
	}

	e.consumeOnce.Do(func() {
		assert.Eventually(e.t, func() bool {
			return e.r.thinPools.refs(racePoolVG, racePoolName) == e.queued
		}, 5*time.Second, time.Millisecond, "the other reconciles of the pool did not wait for the one consuming its space")
	})
}

func (e *thinPoolRaceEnv) wireFakePool() {
	e.mockCmds.EXPECT().
		CreateThinLogicalVolume(racePoolVG, racePoolName, gomock.Any(), gomock.Any()).
		DoAndReturn(func(_, _, lvName string, size int64) (string, error) {
			e.waitForQueuedReconciles()
			e.pool.mu.Lock()
			defer e.pool.mu.Unlock()
			e.pool.lvs[lvName] = size
			return "lvcreate --thin", nil
		}).
		AnyTimes()

	e.mockCmds.EXPECT().
		CreateThinLogicalVolumeFromSource(gomock.Any(), racePoolVG, gomock.Any()).
		DoAndReturn(func(lvName, _, sourceName string) (string, error) {
			e.waitForQueuedReconciles()
			e.pool.mu.Lock()
			defer e.pool.mu.Unlock()
			e.pool.lvs[lvName] = e.pool.lvs[sourceName]
			return "lvcreate -s", nil
		}).
		AnyTimes()

	e.mockCmds.EXPECT().
		ExtendLV(gomock.Any(), racePoolVG, gomock.Any()).
		DoAndReturn(func(size int64, _, lvName string) (string, error) {
			e.waitForQueuedReconciles()
			e.pool.mu.Lock()
			defer e.pool.mu.Unlock()
			e.pool.lvs[lvName] = size
			return "lvextend", nil
		}).
		AnyTimes()

	e.mockCmds.EXPECT().
		GetVGLVs(racePoolVG).
		DoAndReturn(func(string) ([]internal.LVData, string, bytes.Buffer, error) {
			if e.pool.listErr != nil {
				return nil, "lvs", bytes.Buffer{}, e.pool.listErr
			}
			return e.pool.listing(), "lvs", bytes.Buffer{}, nil
		}).
		AnyTimes()

	e.mockCmds.EXPECT().
		GetLV(racePoolVG, gomock.Any()).
		DoAndReturn(func(_, lvName string) (internal.LVData, string, bytes.Buffer, error) {
			return e.pool.lv(lvName), "lvs", bytes.Buffer{}, nil
		}).
		AnyTimes()
}

func (e *thinPoolRaceEnv) newLLV(ctx context.Context, t *testing.T, name, size string, source *v1alpha1.LVMLogicalVolumeSource) *v1alpha1.LVMLogicalVolume {
	t.Helper()

	llv := &v1alpha1.LVMLogicalVolume{
		ObjectMeta: v1.ObjectMeta{Name: name},
		Spec: v1alpha1.LVMLogicalVolumeSpec{
			ActualLVNameOnTheNode: name,
			Type:                  internal.Thin,
			LVMVolumeGroupName:    racePoolLVG,
			Thin:                  &v1alpha1.LVMLogicalVolumeThinSpec{PoolName: racePoolName},
			Size:                  size,
			Source:                source,
		},
		Status: &v1alpha1.LVMLogicalVolumeStatus{Phase: v1alpha1.PhasePending},
	}
	require.NoError(t, e.cl.Create(ctx, llv))

	return llv
}

type reconcileOutcome struct {
	name          string
	shouldRequeue bool
	err           error
}

func reconcileConcurrently(llvs []*v1alpha1.LVMLogicalVolume, lvg *v1alpha1.LVMVolumeGroup, reconcile func(*v1alpha1.LVMLogicalVolume, *v1alpha1.LVMVolumeGroup) (bool, error)) []reconcileOutcome {
	outcomes := make([]reconcileOutcome, len(llvs))
	start := make(chan struct{})

	var wg sync.WaitGroup
	for i, llv := range llvs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			shouldRequeue, err := reconcile(llv, lvg.DeepCopy())
			outcomes[i] = reconcileOutcome{name: llv.Name, shouldRequeue: shouldRequeue, err: err}
		}()
	}
	close(start)
	wg.Wait()

	return outcomes
}

func splitOutcomes(t *testing.T, outcomes []reconcileOutcome) (succeeded, refused int) {
	t.Helper()

	for _, o := range outcomes {
		if o.err == nil {
			assert.False(t, o.shouldRequeue, o.name)
			succeeded++
			continue
		}
		assert.Contains(t, o.err.Error(), "not enough space", o.name)
		assert.True(t, o.shouldRequeue, o.name)
		refused++
	}
	return succeeded, refused
}

func TestReconcileLLVCreateFunc_ConcurrentThinVolumesStayWithinAllocationLimit(t *testing.T) {
	ctx := context.Background()
	env := newThinPoolRaceEnv(t, "100%", nil)

	llvs := []*v1alpha1.LVMLogicalVolume{
		env.newLLV(ctx, t, "race-lv-0", "400Mi", nil),
		env.newLLV(ctx, t, "race-lv-1", "400Mi", nil),
		env.newLLV(ctx, t, "race-lv-2", "400Mi", nil),
	}
	env.queued = len(llvs)

	outcomes := reconcileConcurrently(llvs, env.lvg, func(llv *v1alpha1.LVMLogicalVolume, lvg *v1alpha1.LVMVolumeGroup) (bool, error) {
		return env.r.reconcileLLVCreateFunc(ctx, llv, lvg)
	})

	succeeded, refused := splitOutcomes(t, outcomes)
	assert.Equal(t, 2, succeeded)
	assert.Equal(t, 1, refused)
	assert.LessOrEqual(t, env.pool.allocated(), bytesOf("1Gi"))
	assert.Zero(t, env.r.thinPools.len())
}

func TestReconcileLLVUpdateFunc_ConcurrentThinExtensionsStayWithinAllocationLimit(t *testing.T) {
	ctx := context.Background()
	existing := map[string]int64{
		"race-lv-0": bytesOf("100Mi"),
		"race-lv-1": bytesOf("100Mi"),
		"race-lv-2": bytesOf("100Mi"),
	}
	env := newThinPoolRaceEnv(t, "100%", existing)

	cached := make([]internal.LVData, 0, len(existing))
	llvs := make([]*v1alpha1.LVMLogicalVolume, 0, len(existing))
	for _, name := range []string{"race-lv-0", "race-lv-1", "race-lv-2"} {
		cached = append(cached, env.pool.lv(name))
		llvs = append(llvs, env.newLLV(ctx, t, name, "348Mi", nil))
	}
	env.r.sdsCache.StoreLVs(cached, bytes.Buffer{})
	env.queued = len(llvs)

	outcomes := reconcileConcurrently(llvs, env.lvg, func(llv *v1alpha1.LVMLogicalVolume, lvg *v1alpha1.LVMVolumeGroup) (bool, error) {
		return env.r.reconcileLLVUpdateFunc(ctx, llv, lvg)
	})

	succeeded, refused := splitOutcomes(t, outcomes)
	assert.Equal(t, 2, succeeded)
	assert.Equal(t, 1, refused)
	assert.LessOrEqual(t, env.pool.allocated(), bytesOf("1Gi"))
}

func TestReconcileLLVCreateFunc_ConcurrentThinClonesStayWithinAllocationLimit(t *testing.T) {
	ctx := context.Background()
	existing := map[string]int64{"race-source": bytesOf("400Mi")}
	env := newThinPoolRaceEnv(t, "120%", existing)

	source := env.newLLV(ctx, t, "race-source", "400Mi", nil)
	sourceRef := &v1alpha1.LVMLogicalVolumeSource{Kind: "LVMLogicalVolume", Name: source.Name}

	llvs := []*v1alpha1.LVMLogicalVolume{
		env.newLLV(ctx, t, "race-clone-0", "400Mi", sourceRef),
		env.newLLV(ctx, t, "race-clone-1", "400Mi", sourceRef),
		env.newLLV(ctx, t, "race-clone-2", "400Mi", sourceRef),
	}
	env.queued = len(llvs)

	outcomes := reconcileConcurrently(llvs, env.lvg, func(llv *v1alpha1.LVMLogicalVolume, lvg *v1alpha1.LVMVolumeGroup) (bool, error) {
		return env.r.reconcileLLVCreateFunc(ctx, llv, lvg)
	})

	succeeded, refused := splitOutcomes(t, outcomes)
	assert.Equal(t, 2, succeeded)
	assert.Equal(t, 1, refused)
	assert.LessOrEqual(t, env.pool.allocated(), limitedPoolSize(t, resource.MustParse("1Gi"), "120%"))
}

func bytesOf(size string) int64 {
	q := resource.MustParse(size)
	return q.Value()
}

func (l *thinPoolLocks) len() int {
	l.mu.Lock()
	defer l.mu.Unlock()

	return len(l.locks)
}

func (l *thinPoolLocks) refs(vgName, poolName string) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	entry, ok := l.locks[thinPoolKey{vgName: vgName, poolName: poolName}]
	if !ok {
		return 0
	}
	return entry.refs
}

func TestReconcileLLVCreateFunc_RefusalNamesTheLimitAndTheAllocatedSpace(t *testing.T) {
	ctx := context.Background()
	existing := map[string]int64{"race-lv-0": bytesOf("800Mi")}
	env := newThinPoolRaceEnv(t, "100%", existing)
	env.lvg.Status.ThinPools[0].AvailableSpace = resource.MustParse("1Gi")

	llv := env.newLLV(ctx, t, "race-lv-1", "400Mi", nil)

	shouldRequeue, err := env.r.reconcileLLVCreateFunc(ctx, llv, env.lvg)
	require.Error(t, err)
	assert.True(t, shouldRequeue)
	assert.Equal(t,
		"not enough space: thin pool race-pool of VG race-vg has 800Mi allocated out of 1Gi allowed by allocationLimit 100% (pool size 1Gi), 224Mi left, 400Mi requested",
		err.Error())
	assert.Equal(t, bytesOf("800Mi"), env.pool.allocated())
}

func TestReconcileLLVCreateFunc_HeldThinPoolLockDoesNotBlockThickOrOtherPools(t *testing.T) {
	ctx := context.Background()
	env := newThinPoolRaceEnv(t, "100%", nil)
	env.lvg.Status.ThinPools = append(env.lvg.Status.ThinPools, v1alpha1.LVMVolumeGroupThinPoolStatus{
		Name:            "other-pool",
		ActualSize:      resource.MustParse("100Gi"),
		AvailableSpace:  resource.MustParse("100Gi"),
		AllocationLimit: "100%",
	})

	unlock, err := env.r.thinPools.lock(ctx, racePoolVG, racePoolName)
	require.NoError(t, err)
	defer unlock()

	thick := &v1alpha1.LVMLogicalVolume{
		ObjectMeta: v1.ObjectMeta{Name: "thick-new"},
		Spec: v1alpha1.LVMLogicalVolumeSpec{
			ActualLVNameOnTheNode: "thick-new",
			Type:                  internal.Thick,
			LVMVolumeGroupName:    racePoolLVG,
			Size:                  "400Mi",
		},
		Status: &v1alpha1.LVMLogicalVolumeStatus{Phase: v1alpha1.PhasePending},
	}
	require.NoError(t, env.cl.Create(ctx, thick))
	env.mockCmds.EXPECT().
		CreateThickLogicalVolume(racePoolVG, "thick-new", bytesOf("400Mi"), false).
		Return("lvcreate", nil).
		Times(1)

	otherPool := env.newLLV(ctx, t, "other-pool-new", "400Mi", nil)
	otherPool.Spec.Thin.PoolName = "other-pool"
	env.mockCmds.EXPECT().
		CreateThinLogicalVolume(racePoolVG, "other-pool", "other-pool-new", bytesOf("400Mi")).
		Return("lvcreate --thin", nil).
		Times(1)

	done := make(chan error, 2)
	for _, llv := range []*v1alpha1.LVMLogicalVolume{thick, otherPool} {
		go func() {
			_, _, err := env.r.createLV(ctx, llv, env.lvg.DeepCopy(), resource.MustParse("400Mi"), resource.MustParse("400Mi"))
			done <- err
		}()
	}

	for range 2 {
		select {
		case err := <-done:
			assert.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Fatal("a volume outside the locked thin pool waited for its lock")
		}
	}
}

func TestReconcileLLVCreateFunc_CancelledWhileWaitingForTheThinPoolLock(t *testing.T) {
	env := newThinPoolRaceEnv(t, "100%", nil)
	llv := env.newLLV(context.Background(), t, "race-lv-0", "400Mi", nil)

	unlock, err := env.r.thinPools.lock(context.Background(), racePoolVG, racePoolName)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, _, err := env.r.createLV(ctx, llv, env.lvg.DeepCopy(), resource.MustParse("400Mi"), resource.MustParse("400Mi"))
		result <- err
	}()
	require.Eventually(t, func() bool {
		return env.r.thinPools.refs(racePoolVG, racePoolName) == 2
	}, 5*time.Second, time.Millisecond)
	cancel()

	select {
	case err := <-result:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("a cancelled reconcile kept waiting for the thin pool lock")
	}
	assert.Zero(t, env.pool.allocated())

	unlock()
	unlock()
	assert.Zero(t, env.r.thinPools.len())
}

func TestReconcileLLVCreateFunc_CloneIsCountedAtItsSourceSize(t *testing.T) {
	ctx := context.Background()
	env := newThinPoolRaceEnv(t, "100%", map[string]int64{"race-source": bytesOf("600Mi")})

	source := env.newLLV(ctx, t, "race-source", "600Mi", nil)
	clone := env.newLLV(ctx, t, "race-clone", "400Mi", &v1alpha1.LVMLogicalVolumeSource{Kind: "LVMLogicalVolume", Name: source.Name})

	shouldRequeue, err := env.r.reconcileLLVCreateFunc(ctx, clone, env.lvg)
	require.Error(t, err)
	assert.True(t, shouldRequeue)
	assert.Equal(t,
		"not enough space: thin pool race-pool of VG race-vg has 600Mi allocated out of 1Gi allowed by allocationLimit 100% (pool size 1Gi), 424Mi left, 600Mi requested",
		err.Error())
	assert.Equal(t, bytesOf("600Mi"), env.pool.allocated())
}

func TestReconcileLLVUpdateFunc_ExtensionIsCountedFromTheSizeLVMReports(t *testing.T) {
	ctx := context.Background()
	env := newThinPoolRaceEnv(t, "100%", map[string]int64{
		"race-lv-0": bytesOf("300Mi"),
		"race-lv-1": bytesOf("650Mi"),
	})

	cached := env.pool.lv("race-lv-0")
	cached.LVSize = resource.MustParse("100Mi")
	env.r.sdsCache.StoreLVs([]internal.LVData{cached}, bytes.Buffer{})
	llv := env.newLLV(ctx, t, "race-lv-0", "348Mi", nil)

	shouldRequeue, err := env.r.reconcileLLVUpdateFunc(ctx, llv, env.lvg)
	require.NoError(t, err)
	assert.False(t, shouldRequeue)
	assert.Equal(t, bytesOf("998Mi"), env.pool.allocated())
}

func TestReconcileLLVCreateFunc_LimitFollowsThePoolSizeLVMReports(t *testing.T) {
	ctx := context.Background()
	env := newThinPoolRaceEnv(t, "100%", map[string]int64{"race-lv-0": bytesOf("800Mi")})
	env.pool.poolSize = bytesOf("2Gi")

	llv := env.newLLV(ctx, t, "race-lv-1", "400Mi", nil)

	shouldRequeue, err := env.r.reconcileLLVCreateFunc(ctx, llv, env.lvg)
	require.NoError(t, err)
	assert.False(t, shouldRequeue)
	assert.Equal(t, bytesOf("1200Mi"), env.pool.allocated())
}

func TestReconcileLLVCreateFunc_FailedPoolListingCreatesNothing(t *testing.T) {
	ctx := context.Background()
	env := newThinPoolRaceEnv(t, "100%", nil)
	env.pool.listErr = errors.New("lvs exited with 5")

	llv := env.newLLV(ctx, t, "race-lv-0", "400Mi", nil)

	shouldRequeue, err := env.r.reconcileLLVCreateFunc(ctx, llv, env.lvg)
	require.Error(t, err)
	assert.True(t, shouldRequeue)
	assert.ErrorIs(t, err, env.pool.listErr)
	assert.ErrorContains(t, err, "unable to count the space allocated in thin pool race-pool of VG race-vg")
	assert.Zero(t, env.pool.allocated())
	assert.Zero(t, env.r.thinPools.len())
}
