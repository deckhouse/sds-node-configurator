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

package cache

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	snc "github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
)

func TestCache_ClearBoundExpiredPVC(t *testing.T) {
	log := logger.Logger{}
	const (
		lvgName                = "lvg-name"
		tpName                 = "thin-pool"
		thickBoundExpiredPVC   = "thick-bound-expired-pvc"
		thickPendingExpiredPVC = "thick-pending-expired-pvc"
		thickBoundFreshPVC     = "thick-bound-not-expired-pvc"
		thinBoundExpiredPVC    = "thin-bound-expired-pvc"
		thinPendingExpiredPVC  = "thin-pending-expired-pvc"
		thinBoundFreshPVC      = "thin-bound-not-expired-pvc"
	)

	ch := NewCache(log, DefaultPVCExpiredDurationSec)
	expiredTime := time.Now().Add((-DefaultPVCExpiredDurationSec - 1) * time.Second)

	// Seed internals directly to simulate already-reserved entries with various phases.
	ch.mtx.Lock()
	ch.lvgByName[lvgName] = &lvgEntry{
		lvg: &snc.LVMVolumeGroup{
			ObjectMeta: metav1.ObjectMeta{Name: lvgName},
		},
		thickByPVC: make(map[string]*pvcEntry),
		thinByPool: map[string]*thinPoolEntry{
			tpName: {pvcs: make(map[string]*pvcEntry)},
		},
	}
	// Thick
	ch.lvgByName[lvgName].thickByPVC["/"+thickBoundExpiredPVC] = &pvcEntry{
		pvc: &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: thickBoundExpiredPVC, CreationTimestamp: metav1.NewTime(expiredTime)},
			Status:     corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		},
	}
	ch.lvgByName[lvgName].thickByPVC["/"+thickPendingExpiredPVC] = &pvcEntry{
		pvc: &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: thickPendingExpiredPVC, CreationTimestamp: metav1.NewTime(expiredTime)},
			Status:     corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending},
		},
	}
	ch.lvgByName[lvgName].thickByPVC["/"+thickBoundFreshPVC] = &pvcEntry{
		pvc: &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: thickBoundFreshPVC, CreationTimestamp: metav1.NewTime(time.Now())},
			Status:     corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		},
	}
	// Thin
	ch.lvgByName[lvgName].thinByPool[tpName].pvcs["/"+thinBoundExpiredPVC] = &pvcEntry{
		pvc: &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: thinBoundExpiredPVC, CreationTimestamp: metav1.NewTime(expiredTime)},
			Status:     corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		},
	}
	ch.lvgByName[lvgName].thinByPool[tpName].pvcs["/"+thinPendingExpiredPVC] = &pvcEntry{
		pvc: &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: thinPendingExpiredPVC, CreationTimestamp: metav1.NewTime(expiredTime)},
			Status:     corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending},
		},
	}
	ch.lvgByName[lvgName].thinByPool[tpName].pvcs["/"+thinBoundFreshPVC] = &pvcEntry{
		pvc: &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: thinBoundFreshPVC, CreationTimestamp: metav1.NewTime(time.Now())},
			Status:     corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		},
	}
	ch.mtx.Unlock()

	ch.clearBoundExpiredPVC()

	ch.mtx.RLock()
	defer ch.mtx.RUnlock()
	// Thick assertions
	_, found := ch.lvgByName[lvgName].thickByPVC["/"+thickBoundExpiredPVC]
	assert.False(t, found)
	_, found = ch.lvgByName[lvgName].thickByPVC["/"+thickPendingExpiredPVC]
	assert.True(t, found)
	_, found = ch.lvgByName[lvgName].thickByPVC["/"+thickBoundFreshPVC]
	assert.True(t, found)
	// Thin assertions
	_, found = ch.lvgByName[lvgName].thinByPool[tpName].pvcs["/"+thinBoundExpiredPVC]
	assert.False(t, found)
	_, found = ch.lvgByName[lvgName].thinByPool[tpName].pvcs["/"+thinPendingExpiredPVC]
	assert.True(t, found)
	_, found = ch.lvgByName[lvgName].thinByPool[tpName].pvcs["/"+thinBoundFreshPVC]
	assert.True(t, found)
}

func TestCache_ReservedSpace_PublicAPI(t *testing.T) {
	log := logger.Logger{}
	ch := NewCache(log, DefaultPVCExpiredDurationSec)
	lvg := &snc.LVMVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "lvg-1"},
		Status: snc.LVMVolumeGroupStatus{
			ThinPools: []snc.LVMVolumeGroupThinPoolStatus{{Name: "tp-1"}},
		},
	}
	ch.AddLVG(lvg)

	pvc1 := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1", Namespace: "ns"},
		Spec: corev1.PersistentVolumeClaimSpec{
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: *resource.NewQuantity(1<<20, resource.BinarySI)},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending},
	}
	pvc2 := pvc1.DeepCopy()
	pvc2.Name = "pvc-2"
	pvc2.Spec.Resources.Requests[corev1.ResourceStorage] = *resource.NewQuantity(2<<20, resource.BinarySI)

	assert.NoError(t, ch.AddThickPVC(lvg.Name, pvc1))
	assert.NoError(t, ch.AddThickPVC(lvg.Name, pvc2))
	sum, err := ch.GetLVGThickReservedSpace(lvg.Name)
	assert.NoError(t, err)
	assert.Equal(t, int64((1<<20)+(2<<20)), sum)

	// Thin
	pvc3 := pvc1.DeepCopy()
	pvc3.Name = "pvc-3"
	pvc3.Spec.Resources.Requests[corev1.ResourceStorage] = *resource.NewQuantity(3<<20, resource.BinarySI)
	assert.NoError(t, ch.AddThinPVC(lvg.Name, "tp-1", pvc1))
	assert.NoError(t, ch.AddThinPVC(lvg.Name, "tp-1", pvc3))
	thinSum, err := ch.GetLVGThinReservedSpace(lvg.Name, "tp-1")
	assert.NoError(t, err)
	assert.Equal(t, int64((1<<20)+(3<<20)), thinSum)
}

func TestCache_UpdateLVG(t *testing.T) {
	cache := NewCache(logger.Logger{}, DefaultPVCExpiredDurationSec)
	name := "test-lvg"
	lvg := &snc.LVMVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Status: snc.LVMVolumeGroupStatus{
			AllocatedSize: resource.MustParse("1Gi"),
		},
	}
	cache.AddLVG(lvg)

	newLVG := &snc.LVMVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Status: snc.LVMVolumeGroupStatus{
			AllocatedSize: resource.MustParse("2Gi"),
		},
	}

	err := cache.UpdateLVG(newLVG)
	if err != nil {
		t.Error(err)
	}

	updatedLvg := cache.TryGetLVG(name)
	assert.Equal(t, newLVG.Status.AllocatedSize, updatedLvg.Status.AllocatedSize)
}

// Concurrency/race-oriented tests per Go race detector guidance:
// https://go.dev/doc/articles/race_detector
func TestCache_Race_AddUpdateRead(t *testing.T) {
	log := logger.Logger{}
	ch := NewCache(log, DefaultPVCExpiredDurationSec)

	lvg := &snc.LVMVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "lvg-race"},
		Status: snc.LVMVolumeGroupStatus{
			Nodes:     []snc.LVMVolumeGroupNode{{Name: "node-1"}},
			ThinPools: []snc.LVMVolumeGroupThinPoolStatus{{Name: "tp-1"}},
		},
	}
	ch.AddLVG(lvg)

	var wg sync.WaitGroup
	start := make(chan struct{})

	// Writers: add/update PVCs
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("pvc-%d", i),
					Namespace: "ns",
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceStorage: *resource.NewQuantity(int64(1<<20+i), resource.BinarySI)},
					},
				},
				Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending},
			}
			_ = ch.AddThickPVC(lvg.Name, pvc)
			_ = ch.AddThinPVC(lvg.Name, "tp-1", pvc)

			// Update with selected node annotation
			pvc.Annotations = map[string]string{SelectedNodeAnnotation: "node-1"}
			_ = ch.UpdateThickPVC(lvg.Name, pvc)
			_ = ch.UpdateThinPVC(lvg.Name, "tp-1", pvc)
		}(i)
	}

	// Readers: query functions
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_ = ch.GetAllLVG()
			_ = ch.GetLVGNamesByNodeName("node-1")
			_, _ = ch.GetAllPVCForLVG(lvg.Name)
			_, _ = ch.GetLVGThickReservedSpace(lvg.Name)
			_, _ = ch.GetLVGThinReservedSpace(lvg.Name, "tp-1")
		}()
	}

	// Removers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("pvc-%d", i), Namespace: "ns"},
			}
			ch.RemovePVCFromTheCache(pvc)
		}(i)
	}

	close(start)
	wg.Wait()
}

func TestCache_Race_AddDeleteLVG_GetAll(t *testing.T) {
	log := logger.Logger{}
	ch := NewCache(log, DefaultPVCExpiredDurationSec)

	var wg sync.WaitGroup
	start := make(chan struct{})

	// Add/Update LVGs
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			lvg := &snc.LVMVolumeGroup{
				ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("lvg-%d", i)},
			}
			ch.AddLVG(lvg)
			lvg.Status.AllocatedSize = resource.MustParse(fmt.Sprintf("%dGi", 1+i))
			_ = ch.UpdateLVG(lvg)
		}(i)
	}

	// Readers
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_ = ch.GetAllLVG()
		}()
	}

	// Deleters
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			ch.DeleteLVG(fmt.Sprintf("lvg-%d", i))
		}(i)
	}

	close(start)
	wg.Wait()
}

func TestCache_RemoveVolumeReservationsExcept_Thick(t *testing.T) {
	log := logger.Logger{}
	ch := NewCache(log, DefaultPVCExpiredDurationSec)

	// Seed three LVGs with thick volume reservations for the same volume
	ch.mtx.Lock()
	for _, name := range []string{"lvg-a", "lvg-b", "lvg-c"} {
		ch.lvgByName[name] = &lvgEntry{
			lvg:           &snc.LVMVolumeGroup{ObjectMeta: metav1.ObjectMeta{Name: name}},
			thickByPVC:    make(map[string]*pvcEntry),
			thickByVolume: map[string]*volumeEntry{"vol-1": {size: 1024, createdAt: time.Now()}},
			thinByPool:    make(map[string]*thinPoolEntry),
		}
	}
	ch.mtx.Unlock()

	// Bind vol-1 to lvg-a only
	ch.RemoveVolumeReservationsExcept("vol-1", []LVGRef{{Name: "lvg-a"}})

	ch.mtx.RLock()
	defer ch.mtx.RUnlock()

	// lvg-a should keep the reservation
	assert.Contains(t, ch.lvgByName["lvg-a"].thickByVolume, "vol-1")
	// lvg-b and lvg-c should have the reservation removed
	assert.NotContains(t, ch.lvgByName["lvg-b"].thickByVolume, "vol-1")
	assert.NotContains(t, ch.lvgByName["lvg-c"].thickByVolume, "vol-1")
}

func TestCache_RemoveVolumeReservationsExcept_Thin(t *testing.T) {
	log := logger.Logger{}
	ch := NewCache(log, DefaultPVCExpiredDurationSec)

	// Seed LVGs with thin volume reservations
	ch.mtx.Lock()
	ch.lvgByName["lvg-a"] = &lvgEntry{
		lvg:           &snc.LVMVolumeGroup{ObjectMeta: metav1.ObjectMeta{Name: "lvg-a"}},
		thickByPVC:    make(map[string]*pvcEntry),
		thickByVolume: make(map[string]*volumeEntry),
		thinByPool: map[string]*thinPoolEntry{
			"tp-1": {pvcs: make(map[string]*pvcEntry), volumes: map[string]*volumeEntry{"vol-1": {size: 1024, createdAt: time.Now()}}},
			"tp-2": {pvcs: make(map[string]*pvcEntry), volumes: map[string]*volumeEntry{"vol-1": {size: 1024, createdAt: time.Now()}}},
		},
	}
	ch.lvgByName["lvg-b"] = &lvgEntry{
		lvg:           &snc.LVMVolumeGroup{ObjectMeta: metav1.ObjectMeta{Name: "lvg-b"}},
		thickByPVC:    make(map[string]*pvcEntry),
		thickByVolume: make(map[string]*volumeEntry),
		thinByPool: map[string]*thinPoolEntry{
			"tp-1": {pvcs: make(map[string]*pvcEntry), volumes: map[string]*volumeEntry{"vol-1": {size: 1024, createdAt: time.Now()}}},
		},
	}
	ch.mtx.Unlock()

	// Bind vol-1 to lvg-a/tp-1 only
	ch.RemoveVolumeReservationsExcept("vol-1", []LVGRef{{Name: "lvg-a", ThinPoolName: "tp-1"}})

	ch.mtx.RLock()
	defer ch.mtx.RUnlock()

	// lvg-a/tp-1 should keep the reservation
	assert.Contains(t, ch.lvgByName["lvg-a"].thinByPool["tp-1"].volumes, "vol-1")
	// lvg-a/tp-2 should have the reservation removed
	assert.NotContains(t, ch.lvgByName["lvg-a"].thinByPool["tp-2"].volumes, "vol-1")
	// lvg-b/tp-1 should have the reservation removed
	assert.NotContains(t, ch.lvgByName["lvg-b"].thinByPool["tp-1"].volumes, "vol-1")
}

func TestCache_RemoveVolumeReservationsExcept_MultipleKeep(t *testing.T) {
	log := logger.Logger{}
	ch := NewCache(log, DefaultPVCExpiredDurationSec)

	// Seed three LVGs with thin volume reservations
	ch.mtx.Lock()
	for _, name := range []string{"lvg-a", "lvg-b", "lvg-c"} {
		ch.lvgByName[name] = &lvgEntry{
			lvg:           &snc.LVMVolumeGroup{ObjectMeta: metav1.ObjectMeta{Name: name}},
			thickByPVC:    make(map[string]*pvcEntry),
			thickByVolume: make(map[string]*volumeEntry),
			thinByPool: map[string]*thinPoolEntry{
				"tp-1": {pvcs: make(map[string]*pvcEntry), volumes: map[string]*volumeEntry{"vol-1": {size: 1024, createdAt: time.Now()}}},
			},
		}
	}
	ch.mtx.Unlock()

	// Bind vol-1 to lvg-a/tp-1 and lvg-b/tp-1 (two replicas)
	ch.RemoveVolumeReservationsExcept("vol-1", []LVGRef{
		{Name: "lvg-a", ThinPoolName: "tp-1"},
		{Name: "lvg-b", ThinPoolName: "tp-1"},
	})

	ch.mtx.RLock()
	defer ch.mtx.RUnlock()

	assert.Contains(t, ch.lvgByName["lvg-a"].thinByPool["tp-1"].volumes, "vol-1")
	assert.Contains(t, ch.lvgByName["lvg-b"].thinByPool["tp-1"].volumes, "vol-1")
	assert.NotContains(t, ch.lvgByName["lvg-c"].thinByPool["tp-1"].volumes, "vol-1")
}

func TestCache_RemoveVolumeReservationsExcept_NoReservations(t *testing.T) {
	log := logger.Logger{}
	ch := NewCache(log, DefaultPVCExpiredDurationSec)

	// Seed LVG with no volume reservations
	ch.mtx.Lock()
	ch.lvgByName["lvg-a"] = &lvgEntry{
		lvg:           &snc.LVMVolumeGroup{ObjectMeta: metav1.ObjectMeta{Name: "lvg-a"}},
		thickByPVC:    make(map[string]*pvcEntry),
		thickByVolume: make(map[string]*volumeEntry),
		thinByPool:    make(map[string]*thinPoolEntry),
	}
	ch.mtx.Unlock()

	// Should not panic on empty cache
	ch.RemoveVolumeReservationsExcept("vol-1", []LVGRef{{Name: "lvg-a"}})
	ch.RemoveVolumeReservationsExcept("vol-1", nil)
}

func TestCache_RemoveVolumeReservationsExcept_EmptyKeep(t *testing.T) {
	log := logger.Logger{}
	ch := NewCache(log, DefaultPVCExpiredDurationSec)

	// Seed two LVGs with thick volume reservations
	ch.mtx.Lock()
	for _, name := range []string{"lvg-a", "lvg-b"} {
		ch.lvgByName[name] = &lvgEntry{
			lvg:           &snc.LVMVolumeGroup{ObjectMeta: metav1.ObjectMeta{Name: name}},
			thickByPVC:    make(map[string]*pvcEntry),
			thickByVolume: map[string]*volumeEntry{"vol-1": {size: 1024, createdAt: time.Now()}},
			thinByPool:    make(map[string]*thinPoolEntry),
		}
	}
	ch.mtx.Unlock()

	// Empty keep = remove from all LVGs
	ch.RemoveVolumeReservationsExcept("vol-1", nil)

	ch.mtx.RLock()
	defer ch.mtx.RUnlock()

	assert.NotContains(t, ch.lvgByName["lvg-a"].thickByVolume, "vol-1")
	assert.NotContains(t, ch.lvgByName["lvg-b"].thickByVolume, "vol-1")
}

func TestCache_RemoveVolumeReservationsExcept_EmptyKeep_RemovesFromBoth(t *testing.T) {
	log := logger.Logger{}
	ch := NewCache(log, DefaultPVCExpiredDurationSec)

	// Seed: vol-1 in lvg-a thick AND vol-1 in lvg-b thin (edge case: same name in both)
	ch.mtx.Lock()
	ch.lvgByName["lvg-a"] = &lvgEntry{
		lvg:           &snc.LVMVolumeGroup{ObjectMeta: metav1.ObjectMeta{Name: "lvg-a"}},
		thickByPVC:    make(map[string]*pvcEntry),
		thickByVolume: map[string]*volumeEntry{"vol-1": {size: 1024, createdAt: time.Now()}},
		thinByPool:    make(map[string]*thinPoolEntry),
	}
	ch.lvgByName["lvg-b"] = &lvgEntry{
		lvg:           &snc.LVMVolumeGroup{ObjectMeta: metav1.ObjectMeta{Name: "lvg-b"}},
		thickByPVC:    make(map[string]*pvcEntry),
		thickByVolume: make(map[string]*volumeEntry),
		thinByPool: map[string]*thinPoolEntry{
			"tp-1": {pvcs: make(map[string]*pvcEntry), volumes: map[string]*volumeEntry{"vol-1": {size: 1024, createdAt: time.Now()}}},
		},
	}
	ch.mtx.Unlock()

	ch.RemoveVolumeReservationsExcept("vol-1", nil)

	ch.mtx.RLock()
	defer ch.mtx.RUnlock()

	assert.NotContains(t, ch.lvgByName["lvg-a"].thickByVolume, "vol-1")
	assert.NotContains(t, ch.lvgByName["lvg-b"].thinByPool["tp-1"].volumes, "vol-1")
}
