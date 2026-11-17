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

package llv_extender

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/cache"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/mock_utils"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/monitoring"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/test_utils"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/thinpoolalloc"
)

const (
	poolVG    = "pool-vg"
	poolName  = "pool"
	poolLVG   = "pool-lvg"
	otherPool = "other-pool"
)

type fakePool struct {
	mu      sync.Mutex
	lvs     map[string]internal.LVData
	listErr error
	extends int
}

func (p *fakePool) put(name, pool string, size int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	attr := "Vwi-a-tz--"
	if pool == "" {
		attr = "-wi-a-----"
	}
	p.lvs[name] = internal.LVData{LVName: name, VGName: poolVG, PoolName: pool, LVAttr: attr, LVSize: *resource.NewQuantity(size, resource.BinarySI)}
}

func (p *fakePool) get(name string) internal.LVData {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.lvs[name]
}

func (p *fakePool) size(name string) int64 {
	lv := p.get(name)
	return lv.LVSize.Value()
}

func (p *fakePool) allocated(pool string) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	var sum int64
	for _, lv := range p.lvs {
		if lv.PoolName == pool {
			sum += lv.LVSize.Value()
		}
	}
	return sum
}

func (p *fakePool) extendCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.extends
}

type thinPoolEnv struct {
	r     *Reconciler
	cl    client.Client
	locks *thinpoolalloc.Locks
	pool  *fakePool
	lvg   *v1alpha1.LVMVolumeGroup
}

func newThinPoolEnv(t *testing.T) *thinPoolEnv {
	t.Helper()

	ctrl := gomock.NewController(t)
	mockCmds := mock_utils.NewMockCommands(ctrl)
	locks := &thinpoolalloc.Locks{}
	cl := test_utils.NewFakeClient(&v1alpha1.LVMLogicalVolume{}, &v1alpha1.LVMVolumeGroup{})
	r := NewReconciler(cl, logger.NewNop(), monitoring.GetMetrics(""), cache.New(), mockCmds, nil, ReconcilerConfig{NodeName: testNodeName, ThinPools: locks}).(*Reconciler)

	pool := &fakePool{lvs: map[string]internal.LVData{}}
	pool.lvs[poolName] = internal.LVData{LVName: poolName, VGName: poolVG, LVAttr: "twi-aotz--", LVSize: resource.MustParse("1Gi")}
	pool.lvs[otherPool] = internal.LVData{LVName: otherPool, VGName: poolVG, LVAttr: "twi-aotz--", LVSize: resource.MustParse("10Gi")}

	mockCmds.EXPECT().GetVGLVs(poolVG).DoAndReturn(func(string) ([]internal.LVData, string, bytes.Buffer, error) {
		pool.mu.Lock()
		defer pool.mu.Unlock()
		if pool.listErr != nil {
			return nil, "lvs", bytes.Buffer{}, pool.listErr
		}
		lvs := make([]internal.LVData, 0, len(pool.lvs))
		for _, lv := range pool.lvs {
			lvs = append(lvs, lv)
		}
		return lvs, "lvs", bytes.Buffer{}, nil
	}).AnyTimes()
	mockCmds.EXPECT().ExtendLV(gomock.Any(), poolVG, gomock.Any()).DoAndReturn(func(size int64, _, lvName string) (string, error) {
		pool.mu.Lock()
		defer pool.mu.Unlock()
		lv := pool.lvs[lvName]
		lv.LVSize = *resource.NewQuantity(size, resource.BinarySI)
		pool.lvs[lvName] = lv
		pool.extends++
		return "lvextend", nil
	}).AnyTimes()
	mockCmds.EXPECT().GetLV(poolVG, gomock.Any()).DoAndReturn(func(_, lvName string) (internal.LVData, string, bytes.Buffer, error) {
		return pool.get(lvName), "lvs", bytes.Buffer{}, nil
	}).AnyTimes()

	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: poolLVG},
		Spec: v1alpha1.LVMVolumeGroupSpec{
			ActualVGNameOnTheNode: poolVG,
			Local:                 v1alpha1.LVMVolumeGroupLocalSpec{NodeName: testNodeName},
		},
		Status: v1alpha1.LVMVolumeGroupStatus{
			Phase:      internal.PhaseReady,
			ExtentSize: resource.MustParse("4Mi"),
			VGSize:     resource.MustParse("100Gi"),
			VGFree:     resource.MustParse("50Gi"),
			ThinPools: []v1alpha1.LVMVolumeGroupThinPoolStatus{
				{Name: poolName, ActualSize: resource.MustParse("1Gi"), AllocationLimit: "100%", AllocatedSize: resource.MustParse("400Mi"), AvailableSpace: resource.MustParse("624Mi")},
				{Name: otherPool, ActualSize: resource.MustParse("10Gi"), AllocationLimit: "100%", AvailableSpace: resource.MustParse("10Gi")},
			},
		},
	}
	require.NoError(t, cl.Create(context.Background(), lvg))

	return &thinPoolEnv{r: r, cl: cl, locks: locks, pool: pool, lvg: lvg}
}

func (e *thinPoolEnv) addLLV(t *testing.T, name, lvType, pool, size string, lvSize int64) {
	t.Helper()

	ctx := context.Background()
	llv := &v1alpha1.LVMLogicalVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1alpha1.LVMLogicalVolumeSpec{
			LVMVolumeGroupName:    poolLVG,
			ActualLVNameOnTheNode: name,
			Type:                  lvType,
			Size:                  size,
		},
	}
	if pool != "" {
		llv.Spec.Thin = &v1alpha1.LVMLogicalVolumeThinSpec{PoolName: pool}
	}
	require.NoError(t, e.cl.Create(ctx, llv))
	llv.Status = &v1alpha1.LVMLogicalVolumeStatus{Phase: v1alpha1.PhaseCreated, ActualSize: *resource.NewQuantity(lvSize, resource.BinarySI)}
	require.NoError(t, e.cl.Status().Update(ctx, llv))

	e.pool.put(name, pool, lvSize)
	cached := make([]internal.LVData, 0)
	e.pool.mu.Lock()
	for _, lv := range e.pool.lvs {
		cached = append(cached, lv)
	}
	e.pool.mu.Unlock()
	e.r.sdsCache.StoreLVs(cached, bytes.Buffer{})
}

func (e *thinPoolEnv) llv(t *testing.T, name string) *v1alpha1.LVMLogicalVolume {
	t.Helper()

	llv := &v1alpha1.LVMLogicalVolume{}
	require.NoError(t, e.cl.Get(context.Background(), client.ObjectKey{Name: name}, llv))
	return llv
}

func TestLLVExtenderWaitsForThePoolLockAndCountsWhatItsHolderAllocated(t *testing.T) {
	ctx := context.Background()
	env := newThinPoolEnv(t)
	env.addLLV(t, "grow-lv", internal.Thin, poolName, "50%", bytesOf("100Mi"))
	env.pool.put("held-lv", poolName, bytesOf("300Mi"))

	unlock, err := env.locks.Lock(ctx, poolVG, poolName)
	require.NoError(t, err)

	done := make(chan bool, 1)
	go func() {
		done <- env.r.ReconcileLVMLogicalVolumeExtension(ctx, env.lvg.DeepCopy())
	}()

	require.Eventually(t, func() bool {
		return env.locks.Refs(poolVG, poolName) == 2
	}, 5*time.Second, time.Millisecond, "the extension did not wait for the operation holding the thin pool")
	assert.Zero(t, env.pool.extendCount())

	env.pool.put("new-lv", poolName, bytesOf("400Mi"))
	unlock()

	select {
	case shouldRetry := <-done:
		assert.True(t, shouldRetry)
	case <-time.After(10 * time.Second):
		t.Fatal("the extension did not finish after the thin pool was released")
	}

	assert.Zero(t, env.pool.extendCount())
	assert.LessOrEqual(t, env.pool.allocated(poolName), bytesOf("1Gi"))
	llv := env.llv(t, "grow-lv")
	assert.Equal(t, v1alpha1.PhaseFailed, llv.Status.Phase)
	assert.Contains(t, llv.Status.Reason, "not enough space: thin pool pool of VG pool-vg has 800Mi allocated out of 1Gi")
	assert.Zero(t, env.locks.Len())
}

func TestLLVExtenderExtendsAThinVolumeThatFitsByLVs(t *testing.T) {
	ctx := context.Background()
	env := newThinPoolEnv(t)
	env.addLLV(t, "grow-lv", internal.Thin, poolName, "50%", bytesOf("100Mi"))
	env.pool.put("held-lv", poolName, bytesOf("300Mi"))

	assert.False(t, env.r.ReconcileLVMLogicalVolumeExtension(ctx, env.lvg.DeepCopy()))

	assert.Equal(t, 1, env.pool.extendCount())
	assert.Equal(t, bytesOf("512Mi"), env.pool.size("grow-lv"))
	llv := env.llv(t, "grow-lv")
	assert.Equal(t, v1alpha1.PhaseCreated, llv.Status.Phase)
	assert.Equal(t, bytesOf("512Mi"), llv.Status.ActualSize.Value())
	assert.Zero(t, env.locks.Len())
}

func TestLLVExtenderRetriesWhenLVsFails(t *testing.T) {
	ctx := context.Background()
	env := newThinPoolEnv(t)
	env.addLLV(t, "grow-lv", internal.Thin, poolName, "50%", bytesOf("100Mi"))
	env.pool.listErr = errors.New("lvs: exit status 5")

	assert.True(t, env.r.ReconcileLVMLogicalVolumeExtension(ctx, env.lvg.DeepCopy()))

	assert.Zero(t, env.pool.extendCount())
	llv := env.llv(t, "grow-lv")
	assert.Equal(t, v1alpha1.PhaseCreated, llv.Status.Phase)
	assert.Empty(t, llv.Status.Reason)
	assert.Zero(t, env.locks.Len())
}

func TestLLVExtenderDoesNotWaitForTheLockOfAnotherThinPool(t *testing.T) {
	ctx := context.Background()
	env := newThinPoolEnv(t)
	env.addLLV(t, "thick-lv", internal.Thick, "", "10%", bytesOf("1Gi"))
	env.addLLV(t, "other-lv", internal.Thin, otherPool, "50%", bytesOf("1Gi"))

	unlock, err := env.locks.Lock(ctx, poolVG, poolName)
	require.NoError(t, err)
	defer unlock()

	done := make(chan bool, 1)
	go func() {
		done <- env.r.ReconcileLVMLogicalVolumeExtension(ctx, env.lvg.DeepCopy())
	}()

	select {
	case shouldRetry := <-done:
		assert.False(t, shouldRetry)
	case <-time.After(10 * time.Second):
		t.Fatal("an extension outside the locked thin pool waited for its lock")
	}

	assert.Equal(t, 2, env.pool.extendCount())
	assert.Equal(t, bytesOf("10Gi"), env.pool.size("thick-lv"))
	assert.Equal(t, bytesOf("5Gi"), env.pool.size("other-lv"))
}

func bytesOf(size string) int64 {
	q := resource.MustParse(size)
	return q.Value()
}

func TestNewReconcilerRequiresTheSharedThinPoolLocks(t *testing.T) {
	assert.Panics(t, func() {
		NewReconciler(test_utils.NewFakeClient(), logger.NewNop(), monitoring.GetMetrics(""), cache.New(), nil, nil, ReconcilerConfig{})
	})
}
