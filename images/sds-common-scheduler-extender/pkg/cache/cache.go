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
	"time"

	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
)

const (
	SelectedNodeAnnotation = "volume.kubernetes.io/selected-node"
)

// StoragePoolKey uniquely identifies a storage pool.
// For thick volumes, ThinPoolName is empty.
type StoragePoolKey struct {
	LVGName      string
	ThinPoolName string // empty for thick
}

// String returns a human-readable representation of the key.
func (k StoragePoolKey) String() string {
	if k.ThinPoolName == "" {
		return k.LVGName
	}
	return k.LVGName + "/" + k.ThinPoolName
}

// PoolEntry holds the pre-calculated total reserved size for a storage pool.
type PoolEntry struct {
	reservedSize int64
}

// Reservation tracks a single reservation across one or more storage pools.
type Reservation struct {
	expiresAt time.Time
	size      int64
	pools     map[StoragePoolKey]struct{}
}

// ReservationInfo is an exported snapshot of a Reservation for debug/inspection.
type ReservationInfo struct {
	Size      int64
	ExpiresAt time.Time
	Pools     []StoragePoolKey
}

// Cache is a pure reservation store. It tracks reserved space per storage pool
// and supports TTL-based expiration of reservations.
// LVG resources are NOT stored here; they are read from the controller-runtime
// informer cache via client.Client.
type Cache struct {
	mtx          sync.RWMutex
	pools        map[StoragePoolKey]*PoolEntry
	reservations map[string]*Reservation
	log          logger.Logger
}

// NewCache creates a new reservation cache and starts a background cleanup goroutine.
func NewCache(log logger.Logger, cleanupInterval time.Duration) *Cache {
	c := &Cache{
		pools:        make(map[StoragePoolKey]*PoolEntry),
		reservations: make(map[string]*Reservation),
		log:          log,
	}
	c.startCleanupLoop(cleanupInterval)
	return c
}

// GetReservedSpace returns the pre-calculated reserved size for a given pool. O(1) lookup.
func (c *Cache) GetReservedSpace(key StoragePoolKey) int64 {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	entry, ok := c.pools[key]
	if !ok {
		return 0
	}
	return entry.reservedSize
}

// GetReservation returns reservation data for debugging/inspection.
func (c *Cache) GetReservation(id string) (int64, []StoragePoolKey, bool) {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	r, ok := c.reservations[id]
	if !ok {
		return 0, nil, false
	}
	keys := make([]StoragePoolKey, 0, len(r.pools))
	for k := range r.pools {
		keys = append(keys, k)
	}
	return r.size, keys, true
}

// HasReservation checks whether a reservation with the given ID exists.
func (c *Cache) HasReservation(id string) bool {
	c.mtx.RLock()
	defer c.mtx.RUnlock()
	_, ok := c.reservations[id]
	return ok
}

// AddReservation adds a new reservation. If a reservation with the same ID already exists,
// it is removed first (idempotent replace). For each pool, increments reservedSize.
func (c *Cache) AddReservation(id string, ttl time.Duration, size int64, poolKeys []StoragePoolKey) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	// Idempotent: remove old reservation if exists
	if _, exists := c.reservations[id]; exists {
		c.log.Debug(fmt.Sprintf("[AddReservation] reservation %s already exists, replacing", id))
		c.removeReservation(id)
	}

	poolSet := make(map[StoragePoolKey]struct{}, len(poolKeys))
	for _, key := range poolKeys {
		poolSet[key] = struct{}{}
		c.addToPool(key, size)
	}

	c.reservations[id] = &Reservation{
		expiresAt: time.Now().Add(ttl),
		size:      size,
		pools:     poolSet,
	}

	c.log.Debug(fmt.Sprintf("[AddReservation] reservation %s added: size=%d, pools=%d, ttl=%s", id, size, len(poolSet), ttl))
}

// RemoveReservation removes a reservation and decrements reservedSize for each affected pool.
func (c *Cache) RemoveReservation(id string) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	c.removeReservation(id)
}

// NarrowReservation keeps only the specified pools in the reservation, removes the rest.
// Used for both single-pool narrowing (/v1/lvg/narrow-reservation) and multi-pool narrowing
// (PVC watcher on selectedNode, prioritize final node list).
// Returns false if the reservation was not found.
func (c *Cache) NarrowReservation(id string, keepPools []StoragePoolKey, newTTL time.Duration) bool {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	r, ok := c.reservations[id]
	if !ok {
		c.log.Debug(fmt.Sprintf("[NarrowReservation] reservation %s not found", id))
		return false
	}

	keepSet := make(map[StoragePoolKey]struct{}, len(keepPools))
	for _, k := range keepPools {
		keepSet[k] = struct{}{}
	}

	removedCount := 0
	// Remove reservation from pools NOT in keepSet
	for poolKey := range r.pools {
		if _, keep := keepSet[poolKey]; !keep {
			c.subtractFromPool(poolKey, r.size)
			delete(r.pools, poolKey)
			removedCount++
		}
	}

	r.expiresAt = time.Now().Add(newTTL)

	c.log.Debug(fmt.Sprintf("[NarrowReservation] reservation %s narrowed: removed %d pools, remaining %d pools", id, removedCount, len(r.pools)))

	// If no pools remain, remove the reservation entirely
	if len(r.pools) == 0 {
		delete(c.reservations, id)
		c.log.Debug(fmt.Sprintf("[NarrowReservation] reservation %s removed (no pools remaining)", id))
	}

	return true
}

// GetAllPools returns a snapshot of all pools and their reserved sizes. For debug endpoints.
func (c *Cache) GetAllPools() map[StoragePoolKey]int64 {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	result := make(map[StoragePoolKey]int64, len(c.pools))
	for key, entry := range c.pools {
		result[key] = entry.reservedSize
	}
	return result
}

// GetAllReservations returns a snapshot of all reservations. For debug endpoints.
func (c *Cache) GetAllReservations() map[string]ReservationInfo {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	result := make(map[string]ReservationInfo, len(c.reservations))
	for id, r := range c.reservations {
		pools := make([]StoragePoolKey, 0, len(r.pools))
		for k := range r.pools {
			pools = append(pools, k)
		}
		result[id] = ReservationInfo{
			Size:      r.size,
			ExpiresAt: r.expiresAt,
			Pools:     pools,
		}
	}
	return result
}

// --- Internal helpers (called under held lock) ---

// addToPool increments reserved size for a pool, creating the entry if needed.
func (c *Cache) addToPool(key StoragePoolKey, size int64) {
	entry, ok := c.pools[key]
	if !ok {
		entry = &PoolEntry{}
		c.pools[key] = entry
	}
	entry.reservedSize += size
}

// subtractFromPool decrements reserved size for a pool. Removes entry if it reaches 0.
func (c *Cache) subtractFromPool(key StoragePoolKey, size int64) {
	entry, ok := c.pools[key]
	if !ok {
		return
	}
	entry.reservedSize -= size
	if entry.reservedSize <= 0 {
		delete(c.pools, key)
	}
}

// removeReservation removes a reservation and updates all affected pools.
func (c *Cache) removeReservation(id string) {
	r, ok := c.reservations[id]
	if !ok {
		return
	}
	for poolKey := range r.pools {
		c.subtractFromPool(poolKey, r.size)
	}
	delete(c.reservations, id)
	c.log.Debug(fmt.Sprintf("[removeReservation] reservation %s removed", id))
}

// --- Background TTL cleanup ---

func (c *Cache) startCleanupLoop(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			c.cleanupExpired()
		}
	}()
}

func (c *Cache) cleanupExpired() {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	now := time.Now()
	for id, r := range c.reservations {
		if now.After(r.expiresAt) {
			c.log.Debug(fmt.Sprintf("[cleanupExpired] removing expired reservation %s", id))
			c.removeReservation(id)
		}
	}
}
