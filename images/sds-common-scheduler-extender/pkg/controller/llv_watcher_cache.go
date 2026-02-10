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

package controller

import (
	"context"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	snc "github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/cache"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
)

const (
	LLVWatcherCacheCtrlName = "llv-watcher-cache-controller"
)

// RunLLVWatcherCacheController creates a controller that watches LVMLogicalVolume resources.
// When an LLV transitions to Status.Phase == "Created" or is deleted, its reservation
// is removed from the cache. This prevents double-counting of reserved space once the
// actual LV has been provisioned on disk.
func RunLLVWatcherCacheController(
	mgr manager.Manager,
	log logger.Logger,
	schedulerCache *cache.Cache,
) error {
	log.Info("[RunLLVWatcherCacheController] starts the work")

	c, err := controller.New(LLVWatcherCacheCtrlName, mgr, controller.Options{
		Reconciler: &llvReconciler{
			cache: schedulerCache,
			log:   log,
		},
	})
	if err != nil {
		log.Error(err, "[RunLLVWatcherCacheController] unable to create controller")
		return err
	}

	err = c.Watch(source.Kind(mgr.GetCache(), &snc.LVMLogicalVolume{},
		&handler.TypedEnqueueRequestForObject[*snc.LVMLogicalVolume]{},
		predicate.TypedFuncs[*snc.LVMLogicalVolume]{
			// Ignore Create events -- LLV is created in Pending phase, reservation already exists
			CreateFunc: func(e event.TypedCreateEvent[*snc.LVMLogicalVolume]) bool {
				return false
			},
			// Only reconcile when Status.Phase transitions to Created
			UpdateFunc: func(e event.TypedUpdateEvent[*snc.LVMLogicalVolume]) bool {
				newLLV := e.ObjectNew
				oldLLV := e.ObjectOld

				if newLLV.Status == nil {
					return false
				}

				newCreated := newLLV.Status.Phase == snc.PhaseCreated
				oldCreated := oldLLV.Status != nil && oldLLV.Status.Phase == snc.PhaseCreated

				// Only reconcile on phase transition to Created
				return newCreated && !oldCreated
			},
			// Always reconcile on delete (cleanup if deleted before reaching Created)
			DeleteFunc: func(e event.TypedDeleteEvent[*snc.LVMLogicalVolume]) bool {
				return true
			},
		},
	))
	if err != nil {
		log.Error(err, "[RunLLVWatcherCacheController] unable to controller Watch")
		return err
	}

	log.Info("[RunLLVWatcherCacheController] controller started successfully")
	return nil
}

type llvReconciler struct {
	cache *cache.Cache
	log   logger.Logger
}

func (r *llvReconciler) Reconcile(_ context.Context, req reconcile.Request) (reconcile.Result, error) {
	r.log.Debug(fmt.Sprintf("[llvReconciler] reconciling LLV %s, removing reservation", req.Name))
	r.cache.RemoveReservation(req.Name)
	return reconcile.Result{}, nil
}
