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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
)

func newTestCache() *Cache {
	log, _ := logger.NewLogger("0")
	return &Cache{
		reservations: make(map[string]*Reservation),
		log:          log,
	}
}

func TestAddReservation(t *testing.T) {
	c := newTestCache()

	pool1 := StoragePoolKey{LVGName: "lvg1", ThinPoolName: ""}
	pool2 := StoragePoolKey{LVGName: "lvg2", ThinPoolName: "tp1"}

	c.AddReservation("res1", 30*time.Second, 100, []StoragePoolKey{pool1, pool2})

	assert.True(t, c.HasReservation("res1"))
	assert.Equal(t, int64(100), c.GetReservedSpace(pool1))
	assert.Equal(t, int64(100), c.GetReservedSpace(pool2))

	size, pools, found := c.GetReservation("res1")
	assert.True(t, found)
	assert.Equal(t, int64(100), size)
	assert.Len(t, pools, 2)
}

func TestAddReservation_Idempotent(t *testing.T) {
	c := newTestCache()

	pool1 := StoragePoolKey{LVGName: "lvg1"}
	pool2 := StoragePoolKey{LVGName: "lvg2"}

	c.AddReservation("res1", 30*time.Second, 100, []StoragePoolKey{pool1, pool2})
	assert.Equal(t, int64(100), c.GetReservedSpace(pool1))
	assert.Equal(t, int64(100), c.GetReservedSpace(pool2))

	// Replace with different pools and size
	pool3 := StoragePoolKey{LVGName: "lvg3"}
	c.AddReservation("res1", 30*time.Second, 200, []StoragePoolKey{pool3})

	// Old pools should be freed
	assert.Equal(t, int64(0), c.GetReservedSpace(pool1))
	assert.Equal(t, int64(0), c.GetReservedSpace(pool2))
	// New pool should have the new size
	assert.Equal(t, int64(200), c.GetReservedSpace(pool3))
}

func TestRemoveReservation(t *testing.T) {
	c := newTestCache()

	pool1 := StoragePoolKey{LVGName: "lvg1"}
	c.AddReservation("res1", 30*time.Second, 100, []StoragePoolKey{pool1})

	assert.Equal(t, int64(100), c.GetReservedSpace(pool1))

	c.RemoveReservation("res1")

	assert.False(t, c.HasReservation("res1"))
	assert.Equal(t, int64(0), c.GetReservedSpace(pool1))
}

func TestRemoveReservation_NotFound(t *testing.T) {
	c := newTestCache()
	// Should not panic
	c.RemoveReservation("nonexistent")
}

func TestNarrowReservation_SinglePool(t *testing.T) {
	c := newTestCache()

	pool1 := StoragePoolKey{LVGName: "lvg1"}
	pool2 := StoragePoolKey{LVGName: "lvg2"}
	pool3 := StoragePoolKey{LVGName: "lvg3"}

	c.AddReservation("res1", 30*time.Second, 100, []StoragePoolKey{pool1, pool2, pool3})

	assert.Equal(t, int64(100), c.GetReservedSpace(pool1))
	assert.Equal(t, int64(100), c.GetReservedSpace(pool2))
	assert.Equal(t, int64(100), c.GetReservedSpace(pool3))

	// Narrow to pool2 only
	ok := c.NarrowReservation("res1", []StoragePoolKey{pool2}, 30*time.Second)
	assert.True(t, ok)

	assert.Equal(t, int64(0), c.GetReservedSpace(pool1))
	assert.Equal(t, int64(100), c.GetReservedSpace(pool2))
	assert.Equal(t, int64(0), c.GetReservedSpace(pool3))

	size, pools, found := c.GetReservation("res1")
	assert.True(t, found)
	assert.Equal(t, int64(100), size)
	assert.Len(t, pools, 1)
	assert.Equal(t, pool2, pools[0])
}

func TestNarrowReservation_MultiplePools(t *testing.T) {
	c := newTestCache()

	pool1 := StoragePoolKey{LVGName: "lvg1"}
	pool2 := StoragePoolKey{LVGName: "lvg2"}
	pool3 := StoragePoolKey{LVGName: "lvg3"}

	c.AddReservation("res1", 30*time.Second, 50, []StoragePoolKey{pool1, pool2, pool3})

	// Keep pool1 and pool3
	ok := c.NarrowReservation("res1", []StoragePoolKey{pool1, pool3}, 30*time.Second)
	assert.True(t, ok)

	assert.Equal(t, int64(50), c.GetReservedSpace(pool1))
	assert.Equal(t, int64(0), c.GetReservedSpace(pool2))
	assert.Equal(t, int64(50), c.GetReservedSpace(pool3))
}

func TestNarrowReservation_NotFound(t *testing.T) {
	c := newTestCache()

	ok := c.NarrowReservation("nonexistent", []StoragePoolKey{{LVGName: "lvg1"}}, 30*time.Second)
	assert.False(t, ok)
}

func TestNarrowReservation_EmptyKeepPools(t *testing.T) {
	c := newTestCache()

	pool1 := StoragePoolKey{LVGName: "lvg1"}
	c.AddReservation("res1", 30*time.Second, 100, []StoragePoolKey{pool1})

	// Narrow with empty keep = removes all pools, reservation removed entirely
	ok := c.NarrowReservation("res1", []StoragePoolKey{}, 30*time.Second)
	assert.True(t, ok)

	assert.False(t, c.HasReservation("res1"))
	assert.Equal(t, int64(0), c.GetReservedSpace(pool1))
}

func TestMultipleReservations_SamePool(t *testing.T) {
	c := newTestCache()

	pool := StoragePoolKey{LVGName: "lvg1", ThinPoolName: "tp1"}

	c.AddReservation("res1", 30*time.Second, 100, []StoragePoolKey{pool})
	c.AddReservation("res2", 30*time.Second, 200, []StoragePoolKey{pool})

	// Pool should have sum of both reservations
	assert.Equal(t, int64(300), c.GetReservedSpace(pool))

	c.RemoveReservation("res1")
	assert.Equal(t, int64(200), c.GetReservedSpace(pool))

	c.RemoveReservation("res2")
	assert.Equal(t, int64(0), c.GetReservedSpace(pool))
}

func TestCleanupExpired(t *testing.T) {
	c := newTestCache()

	pool := StoragePoolKey{LVGName: "lvg1"}

	// Add reservation with very short TTL
	c.AddReservation("res1", 1*time.Millisecond, 100, []StoragePoolKey{pool})
	// Add reservation with long TTL
	c.AddReservation("res2", 1*time.Hour, 200, []StoragePoolKey{pool})

	assert.Equal(t, int64(300), c.GetReservedSpace(pool))

	// Wait for the short one to expire
	time.Sleep(10 * time.Millisecond)

	// Lazy check: GetReservedSpace should already skip the expired reservation
	assert.Equal(t, int64(200), c.GetReservedSpace(pool))

	// HasReservation should also report expired reservation as not found
	assert.False(t, c.HasReservation("res1"))
	assert.True(t, c.HasReservation("res2"))

	// cleanupExpired frees memory (removes expired entries from map)
	c.cleanupExpired()

	// After cleanup, the reservation entry is physically gone
	c.mtx.RLock()
	_, inMap := c.reservations["res1"]
	c.mtx.RUnlock()
	assert.False(t, inMap, "expired reservation should be removed from map after cleanup")

	assert.True(t, c.HasReservation("res2"))
	assert.Equal(t, int64(200), c.GetReservedSpace(pool))
}

func TestGetReservedSpace_SkipsExpired(t *testing.T) {
	c := newTestCache()

	pool := StoragePoolKey{LVGName: "lvg1"}

	// Add reservation with very short TTL
	c.AddReservation("res1", 1*time.Millisecond, 100, []StoragePoolKey{pool})

	// Before expiration, space is reserved
	assert.Equal(t, int64(100), c.GetReservedSpace(pool))

	// Wait for TTL to expire
	time.Sleep(10 * time.Millisecond)

	// GetReservedSpace should return 0 without explicit cleanup
	assert.Equal(t, int64(0), c.GetReservedSpace(pool))
}

func TestGetAllPools(t *testing.T) {
	c := newTestCache()

	pool1 := StoragePoolKey{LVGName: "lvg1"}
	pool2 := StoragePoolKey{LVGName: "lvg2", ThinPoolName: "tp1"}

	c.AddReservation("res1", 30*time.Second, 100, []StoragePoolKey{pool1, pool2})

	pools := c.GetAllPools()
	assert.Len(t, pools, 2)
	assert.Equal(t, int64(100), pools[pool1])
	assert.Equal(t, int64(100), pools[pool2])
}

func TestGetAllPools_SkipsExpired(t *testing.T) {
	c := newTestCache()

	pool1 := StoragePoolKey{LVGName: "lvg1"}
	pool2 := StoragePoolKey{LVGName: "lvg2"}

	c.AddReservation("res1", 1*time.Millisecond, 100, []StoragePoolKey{pool1})
	c.AddReservation("res2", 1*time.Hour, 200, []StoragePoolKey{pool2})

	time.Sleep(10 * time.Millisecond)

	pools := c.GetAllPools()
	// Only active reservation's pool should be present
	assert.Len(t, pools, 1)
	assert.Equal(t, int64(200), pools[pool2])
}

func TestGetAllReservations(t *testing.T) {
	c := newTestCache()

	pool1 := StoragePoolKey{LVGName: "lvg1"}
	pool2 := StoragePoolKey{LVGName: "lvg2"}

	c.AddReservation("res1", 30*time.Second, 100, []StoragePoolKey{pool1})
	c.AddReservation("res2", 30*time.Second, 200, []StoragePoolKey{pool1, pool2})

	reservations := c.GetAllReservations()
	assert.Len(t, reservations, 2)

	r1 := reservations["res1"]
	assert.Equal(t, int64(100), r1.Size)
	assert.Len(t, r1.Pools, 1)
	assert.False(t, r1.Expired)

	r2 := reservations["res2"]
	assert.Equal(t, int64(200), r2.Size)
	assert.Len(t, r2.Pools, 2)
	assert.False(t, r2.Expired)
}

func TestGetAllReservations_MarksExpired(t *testing.T) {
	c := newTestCache()

	pool := StoragePoolKey{LVGName: "lvg1"}

	c.AddReservation("res1", 1*time.Millisecond, 100, []StoragePoolKey{pool})
	c.AddReservation("res2", 1*time.Hour, 200, []StoragePoolKey{pool})

	time.Sleep(10 * time.Millisecond)

	reservations := c.GetAllReservations()
	assert.Len(t, reservations, 2)

	r1 := reservations["res1"]
	assert.True(t, r1.Expired, "expired reservation should be marked as Expired")
	assert.Equal(t, int64(100), r1.Size)

	r2 := reservations["res2"]
	assert.False(t, r2.Expired, "active reservation should not be marked as Expired")
	assert.Equal(t, int64(200), r2.Size)
}

func TestStoragePoolKey_String(t *testing.T) {
	thick := StoragePoolKey{LVGName: "lvg1"}
	assert.Equal(t, "lvg1", thick.String())

	thin := StoragePoolKey{LVGName: "lvg1", ThinPoolName: "tp1"}
	assert.Equal(t, "lvg1/tp1", thin.String())
}

func TestNarrowReservation_UpdatesTTL(t *testing.T) {
	c := newTestCache()

	pool1 := StoragePoolKey{LVGName: "lvg1"}
	pool2 := StoragePoolKey{LVGName: "lvg2"}

	c.AddReservation("res1", 1*time.Second, 100, []StoragePoolKey{pool1, pool2})

	// Narrow with a longer TTL
	c.NarrowReservation("res1", []StoragePoolKey{pool1}, 1*time.Hour)

	// Wait for the original TTL to expire
	time.Sleep(2 * time.Second)

	c.cleanupExpired()

	// Reservation should still be alive due to updated TTL
	assert.True(t, c.HasReservation("res1"))
}
