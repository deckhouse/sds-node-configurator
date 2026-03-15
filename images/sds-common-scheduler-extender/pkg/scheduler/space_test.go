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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/cache"
)

const (
	oneGiB     = int64(1 << 30) // 1 GiB in bytes
	hundredGiB = int64(100) * oneGiB
)

// --- sumLLVSpace tests ---

func TestSumLLVSpace_ThickOnly(t *testing.T) {
	ctx := context.Background()
	cl := newFakeClient(
		readyLVG("lvg1", hundredGiB, hundredGiB),
		thickLLV("llv-a", "lvg1", "10Gi", "Created"),
		thickLLV("llv-b", "lvg1", "20Gi", "Pending"),
		thickLLV("llv-c", "lvg2", "5Gi", "Created"), // different LVG
	)
	key := cache.StoragePoolKey{LVGName: "lvg1"}

	total, err := sumLLVSpace(ctx, cl, key, false)
	require.NoError(t, err)
	assert.Equal(t, int64(30)*oneGiB, total)

	created, err := sumLLVSpace(ctx, cl, key, true)
	require.NoError(t, err)
	assert.Equal(t, int64(10)*oneGiB, created)
}

func TestSumLLVSpace_ThinOnly(t *testing.T) {
	ctx := context.Background()
	cl := newFakeClient(
		thinLLV("llv-thin-1", "lvg1", "tp0", "8Gi", "Created"),
		thinLLV("llv-thin-2", "lvg1", "tp0", "4Gi", "Pending"),
		thinLLV("llv-thin-3", "lvg1", "tp1", "2Gi", "Created"), // different thin pool
		thickLLV("llv-thick", "lvg1", "16Gi", "Created"),       // thick, not thin
	)
	key := cache.StoragePoolKey{LVGName: "lvg1", ThinPoolName: "tp0"}

	total, err := sumLLVSpace(ctx, cl, key, false)
	require.NoError(t, err)
	assert.Equal(t, int64(12)*oneGiB, total)

	created, err := sumLLVSpace(ctx, cl, key, true)
	require.NoError(t, err)
	assert.Equal(t, int64(8)*oneGiB, created)
}

func TestSumLLVSpace_NoLLVs(t *testing.T) {
	ctx := context.Background()
	cl := newFakeClient(readyLVG("lvg1", hundredGiB, hundredGiB))
	key := cache.StoragePoolKey{LVGName: "lvg1"}

	total, err := sumLLVSpace(ctx, cl, key, false)
	require.NoError(t, err)
	assert.Equal(t, int64(0), total)
}

func TestSumLLVSpace_NilStatus(t *testing.T) {
	ctx := context.Background()
	cl := newFakeClient(
		thickLLV("llv-no-status", "lvg1", "10Gi", ""), // no status
	)
	key := cache.StoragePoolKey{LVGName: "lvg1"}

	total, err := sumLLVSpace(ctx, cl, key, false)
	require.NoError(t, err)
	assert.Equal(t, int64(10)*oneGiB, total)

	created, err := sumLLVSpace(ctx, cl, key, true)
	require.NoError(t, err)
	assert.Equal(t, int64(0), created)
}

// --- CalibratePoolUnaccountedSpace tests ---

func TestCalibratePool_NoManualLVs(t *testing.T) {
	ctx := context.Background()
	// VGSize=100Gi, VGFree=80Gi, 1 Created LLV of 20Gi → unaccounted=0
	lvg := readyLVG("lvg1", hundredGiB, 80*oneGiB)
	cl := newFakeClient(
		lvg,
		thickLLV("llv-a", "lvg1", "20Gi", "Created"),
	)
	c := newTestCache()
	key := cache.StoragePoolKey{LVGName: "lvg1"}

	err := CalibratePoolUnaccountedSpace(ctx, cl, c, lvg, key)
	require.NoError(t, err)
	assert.Equal(t, int64(0), c.GetUnaccountedSpace(key))
}

func TestCalibratePool_ManualLVsExist(t *testing.T) {
	ctx := context.Background()
	// VGSize=100Gi, VGFree=50Gi, 1 Created LLV of 20Gi
	// expected free without manual = 100-20 = 80Gi, but actual = 50Gi
	// unaccounted = 80-50 = 30Gi
	lvg := readyLVG("lvg1", hundredGiB, 50*oneGiB)
	cl := newFakeClient(
		lvg,
		thickLLV("llv-a", "lvg1", "20Gi", "Created"),
	)
	c := newTestCache()
	key := cache.StoragePoolKey{LVGName: "lvg1"}

	err := CalibratePoolUnaccountedSpace(ctx, cl, c, lvg, key)
	require.NoError(t, err)
	assert.Equal(t, int64(30)*oneGiB, c.GetUnaccountedSpace(key))
}

func TestCalibratePool_InFlightLLVsIgnoredForCalibration(t *testing.T) {
	ctx := context.Background()
	// VGSize=100Gi, VGFree=80Gi
	// Created LLV: 20Gi (reflected in VGFree), Pending LLV: 10Gi (not reflected)
	// Calibration uses only Created: unaccounted = (100-20) - 80 = 0
	lvg := readyLVG("lvg1", hundredGiB, 80*oneGiB)
	cl := newFakeClient(
		lvg,
		thickLLV("llv-a", "lvg1", "20Gi", "Created"),
		thickLLV("llv-b", "lvg1", "10Gi", "Pending"),
	)
	c := newTestCache()
	key := cache.StoragePoolKey{LVGName: "lvg1"}

	err := CalibratePoolUnaccountedSpace(ctx, cl, c, lvg, key)
	require.NoError(t, err)
	assert.Equal(t, int64(0), c.GetUnaccountedSpace(key))
}

func TestCalibratePool_NegativeClampedToZero(t *testing.T) {
	ctx := context.Background()
	// Edge case: VGFree > VGSize - sumCreated (shouldn't happen normally)
	lvg := readyLVG("lvg1", hundredGiB, 90*oneGiB)
	cl := newFakeClient(
		lvg,
		thickLLV("llv-a", "lvg1", "20Gi", "Created"),
	)
	c := newTestCache()
	key := cache.StoragePoolKey{LVGName: "lvg1"}

	err := CalibratePoolUnaccountedSpace(ctx, cl, c, lvg, key)
	require.NoError(t, err)
	assert.Equal(t, int64(0), c.GetUnaccountedSpace(key))
}

// --- getAvailableSpace tests (stored offset approach) ---

func TestGetAvailableSpace_BasicThick(t *testing.T) {
	ctx := context.Background()
	// VGSize=100Gi, no LLVs, no reservations, no unaccounted → available=100Gi
	cl := newFakeClient(readyLVG("lvg1", hundredGiB, hundredGiB))
	c := newTestCache()
	key := cache.StoragePoolKey{LVGName: "lvg1"}

	info, err := getAvailableSpace(ctx, cl, c, key)
	require.NoError(t, err)
	assert.Equal(t, hundredGiB, info.AvailableSpace)
	assert.Equal(t, hundredGiB, info.TotalSize)
}

func TestGetAvailableSpace_WithLLVs(t *testing.T) {
	ctx := context.Background()
	// VGSize=100Gi, 2 LLVs (20Gi+10Gi=30Gi), no reservations, no unaccounted
	// → available = 100 - 30 - 0 - 0 = 70Gi
	cl := newFakeClient(
		readyLVG("lvg1", hundredGiB, 70*oneGiB),
		thickLLV("llv-a", "lvg1", "20Gi", "Created"),
		thickLLV("llv-b", "lvg1", "10Gi", "Created"),
	)
	c := newTestCache()
	key := cache.StoragePoolKey{LVGName: "lvg1"}

	info, err := getAvailableSpace(ctx, cl, c, key)
	require.NoError(t, err)
	assert.Equal(t, int64(70)*oneGiB, info.AvailableSpace)
}

func TestGetAvailableSpace_WithUnaccounted(t *testing.T) {
	ctx := context.Background()
	// VGSize=100Gi, 1 Created LLV 20Gi, unaccounted=30Gi (manual LVs)
	// → available = 100 - 20 - 30 - 0 = 50Gi
	cl := newFakeClient(
		readyLVG("lvg1", hundredGiB, 50*oneGiB),
		thickLLV("llv-a", "lvg1", "20Gi", "Created"),
	)
	c := newTestCache()
	c.SetUnaccountedSpace(cache.StoragePoolKey{LVGName: "lvg1"}, 30*oneGiB)
	key := cache.StoragePoolKey{LVGName: "lvg1"}

	info, err := getAvailableSpace(ctx, cl, c, key)
	require.NoError(t, err)
	assert.Equal(t, int64(50)*oneGiB, info.AvailableSpace)
}

func TestGetAvailableSpace_WithReservation(t *testing.T) {
	ctx := context.Background()
	// VGSize=100Gi, 1 Created LLV 20Gi, reservation 16Gi, no unaccounted
	// → available = 100 - 20 - 0 - 16 = 64Gi
	cl := newFakeClient(
		readyLVG("lvg1", hundredGiB, 80*oneGiB),
		thickLLV("llv-a", "lvg1", "20Gi", "Created"),
	)
	c := newTestCache()
	key := cache.StoragePoolKey{LVGName: "lvg1"}
	c.AddReservation("pvc-1", 60*time.Second, 16*oneGiB, []cache.StoragePoolKey{key})

	info, err := getAvailableSpace(ctx, cl, c, key)
	require.NoError(t, err)
	assert.Equal(t, int64(64)*oneGiB, info.AvailableSpace)
}

func TestGetAvailableSpace_InFlightLLVReducesAvailable(t *testing.T) {
	ctx := context.Background()
	// VGSize=100Gi, VGFree=80Gi (stale—doesn't reflect new LLV yet)
	// Created LLV: 20Gi, Pending LLV: 16Gi
	// available = 100 - 36 - 0 - 0 = 64Gi (even though VGFree says 80)
	cl := newFakeClient(
		readyLVG("lvg1", hundredGiB, 80*oneGiB),
		thickLLV("llv-created", "lvg1", "20Gi", "Created"),
		thickLLV("llv-pending", "lvg1", "16Gi", "Pending"),
	)
	c := newTestCache()
	key := cache.StoragePoolKey{LVGName: "lvg1"}

	info, err := getAvailableSpace(ctx, cl, c, key)
	require.NoError(t, err)
	assert.Equal(t, int64(64)*oneGiB, info.AvailableSpace)
}

func TestGetAvailableSpace_DoubleCountingSafety(t *testing.T) {
	ctx := context.Background()
	// Reservation exists (16Gi) AND corresponding LLV exists (16Gi, Pending).
	// Both counted: available = 100 - (20+16) - 0 - 16 = 48Gi (conservative).
	// Once reservation is removed: available = 100 - 36 - 0 - 0 = 64Gi.
	cl := newFakeClient(
		readyLVG("lvg1", hundredGiB, 80*oneGiB),
		thickLLV("llv-old", "lvg1", "20Gi", "Created"),
		thickLLV("llv-new", "lvg1", "16Gi", "Pending"),
	)
	c := newTestCache()
	key := cache.StoragePoolKey{LVGName: "lvg1"}
	c.AddReservation("pvc-new", 60*time.Second, 16*oneGiB, []cache.StoragePoolKey{key})

	info, err := getAvailableSpace(ctx, cl, c, key)
	require.NoError(t, err)
	assert.Equal(t, int64(48)*oneGiB, info.AvailableSpace)

	c.RemoveReservation("pvc-new")

	info, err = getAvailableSpace(ctx, cl, c, key)
	require.NoError(t, err)
	assert.Equal(t, int64(64)*oneGiB, info.AvailableSpace)
}

func TestGetAvailableSpace_ThinPool(t *testing.T) {
	ctx := context.Background()
	// Thin pool: AllocatedSize=50Gi, 1 thin LLV 10Gi, no reservations, no unaccounted
	// → available = 50 - 10 - 0 - 0 = 40Gi
	cl := newFakeClient(
		readyLVGWithThinPool("lvg1", hundredGiB, "tp0", 50*oneGiB, 40*oneGiB),
		thinLLV("llv-thin", "lvg1", "tp0", "10Gi", "Created"),
	)
	c := newTestCache()
	key := cache.StoragePoolKey{LVGName: "lvg1", ThinPoolName: "tp0"}

	info, err := getAvailableSpace(ctx, cl, c, key)
	require.NoError(t, err)
	assert.Equal(t, int64(40)*oneGiB, info.AvailableSpace)
	assert.Equal(t, int64(50)*oneGiB, info.TotalSize)
}

func TestGetAvailableSpace_NotReady(t *testing.T) {
	ctx := context.Background()
	cl := newFakeClient(notReadyLVG("lvg1", "NotReady"))
	c := newTestCache()
	key := cache.StoragePoolKey{LVGName: "lvg1"}

	_, err := getAvailableSpace(ctx, cl, c, key)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not ready")
}
