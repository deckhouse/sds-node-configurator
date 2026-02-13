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
	Expired   bool
}

// Cache is a pure reservation store. Reserved space per pool is computed lazily
// by iterating reservations and skipping expired entries.
// A background ticker periodically removes expired reservations to free memory.
// LVG resources are NOT stored here; they are read from the controller-runtime
// informer cache via client.Client.
type Cache struct {
	mtx          sync.RWMutex
	reservations map[string]*Reservation
	log          logger.Logger
}

// NewCache creates a new reservation cache and starts a background cleanup goroutine.
func NewCache(log logger.Logger, cleanupInterval time.Duration) *Cache {
	c := &Cache{
		reservations: make(map[string]*Reservation),
		log:          log,
	}
	c.startCleanupLoop(cleanupInterval)
	return c
}

// GetReservedSpace computes reserved size for a pool by summing active (non-expired) reservations.
func (c *Cache) GetReservedSpace(key StoragePoolKey) int64 {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	now := time.Now()
	var reserved int64
	for _, r := range c.reservations {
		if now.After(r.expiresAt) {
			continue
		}
		if _, ok := r.pools[key]; ok {
			reserved += r.size
		}
	}
	return reserved
}

// GetReservation returns reservation data for debugging/inspection.
// Returns false if the reservation does not exist or is expired.
func (c *Cache) GetReservation(id string) (int64, []StoragePoolKey, bool) {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	r, ok := c.reservations[id]
	if !ok {
		return 0, nil, false
	}
	if time.Now().After(r.expiresAt) {
		return 0, nil, false
	}
	keys := make([]StoragePoolKey, 0, len(r.pools))
	for k := range r.pools {
		keys = append(keys, k)
	}
	return r.size, keys, true
}

// HasReservation checks whether an active (non-expired) reservation with the given ID exists.
func (c *Cache) HasReservation(id string) bool {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	r, ok := c.reservations[id]
	if !ok {
		return false
	}
	return !time.Now().After(r.expiresAt)
}

// AddReservation adds a new reservation. If a reservation with the same ID already exists,
// it is removed first (idempotent replace).
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
	}

	c.reservations[id] = &Reservation{
		expiresAt: time.Now().Add(ttl),
		size:      size,
		pools:     poolSet,
	}

	c.log.Debug(fmt.Sprintf("[AddReservation] reservation %s added: size=%d, pools=%d, ttl=%s", id, size, len(poolSet), ttl))
}

// RemoveReservation removes a reservation.
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
	// Remove pool keys NOT in keepSet
	for poolKey := range r.pools {
		if _, keep := keepSet[poolKey]; !keep {
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

// GetAllPools computes an aggregated view of reserved sizes per pool from active reservations.
// For debug endpoints.
func (c *Cache) GetAllPools() map[StoragePoolKey]int64 {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	now := time.Now()
	result := make(map[StoragePoolKey]int64)
	for _, r := range c.reservations {
		if now.After(r.expiresAt) {
			continue
		}
		for key := range r.pools {
			result[key] += r.size
		}
	}
	return result
}

// GetAllReservations returns a snapshot of all reservations (including expired).
// Expired entries are marked with Expired=true. For debug endpoints.
func (c *Cache) GetAllReservations() map[string]ReservationInfo {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	now := time.Now()
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
			Expired:   now.After(r.expiresAt),
		}
	}
	return result
}

// --- Internal helpers (called under held lock) ---

// removeReservation removes a reservation from the map.
func (c *Cache) removeReservation(id string) {
	if _, ok := c.reservations[id]; !ok {
		return
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
			delete(c.reservations, id)
		}
	}
}
