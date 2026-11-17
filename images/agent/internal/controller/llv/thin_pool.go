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
	"context"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/thinpoolalloc"
)

func (r *Reconciler) lockThinPool(ctx context.Context, llv *v1alpha1.LVMLogicalVolume, lvg *v1alpha1.LVMVolumeGroup) (func(), error) {
	if llv.Spec.Type != internal.Thin || llv.Spec.Thin == nil {
		return func() {}, nil
	}

	return r.thinPools.Lock(ctx, lvg.Spec.ActualVGNameOnTheNode, llv.Spec.Thin.PoolName)
}

func (r *Reconciler) checkThinPoolSpace(lvg *v1alpha1.LVMVolumeGroup, poolName, lvName, sourceLVName string, size resource.Quantity) error {
	return thinpoolalloc.CheckSpace(r.log, r.commands, lvg, poolName, lvName, sourceLVName, size)
}
