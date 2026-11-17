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
	"fmt"
	"sync"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

var ErrNotEnoughSpace = errors.New("not enough space")

type LVLister interface {
	GetVGLVs(vgName string) ([]internal.LVData, string, bytes.Buffer, error)
}

type key struct {
	vgName, poolName string
}

type entry struct {
	held chan struct{}
	refs int
}

type Locks struct {
	mu    sync.Mutex
	locks map[key]*entry
}

func (l *Locks) Lock(ctx context.Context, vgName, poolName string) (func(), error) {
	k := key{vgName: vgName, poolName: poolName}

	l.mu.Lock()
	if l.locks == nil {
		l.locks = make(map[key]*entry)
	}
	e, ok := l.locks[k]
	if !ok {
		e = &entry{held: make(chan struct{}, 1)}
		l.locks[k] = e
	}
	e.refs++
	l.mu.Unlock()

	select {
	case e.held <- struct{}{}:
	case <-ctx.Done():
		l.forget(k, e)
		return nil, fmt.Errorf("unable to lock thin pool %s in VG %s: %w", poolName, vgName, ctx.Err())
	}

	var once sync.Once
	return func() {
		once.Do(func() {
			<-e.held
			l.forget(k, e)
		})
	}, nil
}

func (l *Locks) forget(k key, e *entry) {
	l.mu.Lock()
	defer l.mu.Unlock()

	e.refs--
	if e.refs == 0 {
		delete(l.locks, k)
	}
}

func (l *Locks) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()

	return len(l.locks)
}

func (l *Locks) Refs(vgName, poolName string) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	e, ok := l.locks[key{vgName: vgName, poolName: poolName}]
	if !ok {
		return 0
	}
	return e.refs
}

func CheckSpace(log logger.Logger, lister LVLister, lvg *v1alpha1.LVMVolumeGroup, poolName, lvName, sourceLVName string, size resource.Quantity) error {
	var pool *v1alpha1.LVMVolumeGroupThinPoolStatus
	for i := range lvg.Status.ThinPools {
		if lvg.Status.ThinPools[i].Name == poolName {
			pool = &lvg.Status.ThinPools[i]
			break
		}
	}
	if pool == nil {
		return fmt.Errorf("thin pool %s is not found in the status of the LVMVolumeGroup %s", poolName, lvg.Name)
	}

	vgName := lvg.Spec.ActualVGNameOnTheNode
	lvs, cmd, _, err := lister.GetVGLVs(vgName)
	log.Debug(fmt.Sprintf("ran cmd: %s", cmd))
	if err != nil {
		return fmt.Errorf("unable to count the space allocated in thin pool %s of VG %s: %w", poolName, vgName, err)
	}

	poolSize := pool.ActualSize
	var allocated, current, sourceSize int64
	for _, lv := range lvs {
		if lv.VGName != vgName {
			continue
		}
		if lv.LVName == poolName && utils.IsLVThinPool(lv.LVAttr) {
			poolSize = lv.LVSize
		}
		if sourceLVName != "" && lv.LVName == sourceLVName {
			sourceSize = lv.LVSize.Value()
		}
		if lv.PoolName == poolName {
			allocated += lv.LVSize.Value()
			if lv.LVName == lvName {
				current = lv.LVSize.Value()
			}
		}
	}

	limit, err := utils.GetThinPoolSpaceWithAllocationLimit(poolSize, pool.AllocationLimit)
	if err != nil {
		return fmt.Errorf("unable to apply allocationLimit %q of thin pool %s: %w", pool.AllocationLimit, poolName, err)
	}

	needed := max(size.Value(), sourceSize) - current
	left := limit.Value() - allocated
	if needed > 0 && left < needed {
		return fmt.Errorf(
			"%w: thin pool %s of VG %s has %s allocated out of %s allowed by allocationLimit %s (pool size %s), %s left, %s requested",
			ErrNotEnoughSpace,
			poolName,
			vgName,
			resource.NewQuantity(allocated, resource.BinarySI).String(),
			limit.String(),
			pool.AllocationLimit,
			poolSize.String(),
			resource.NewQuantity(max(left, 0), resource.BinarySI).String(),
			resource.NewQuantity(needed, resource.BinarySI).String(),
		)
	}

	return nil
}
