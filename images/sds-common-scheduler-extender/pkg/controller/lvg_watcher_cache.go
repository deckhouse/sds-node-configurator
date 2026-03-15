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

	"sigs.k8s.io/controller-runtime/pkg/client"
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
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/scheduler"
)

const (
	LVGWatcherCacheCtrlName = "lvg-watcher-cache-controller"
)

// RunLVGWatcherCacheController creates a controller that watches LVMVolumeGroup resources.
// On every Create (including initial informer sync at startup) and Update event, it
// recalibrates the unaccounted space offset for all storage pools on the LVG.
func RunLVGWatcherCacheController(
	mgr manager.Manager,
	log logger.Logger,
	schedulerCache *cache.Cache,
) error {
	log.Info("[RunLVGWatcherCacheController] starts the work")

	c, err := controller.New(LVGWatcherCacheCtrlName, mgr, controller.Options{
		Reconciler: &lvgReconciler{
			cl:    mgr.GetClient(),
			cache: schedulerCache,
			log:   log,
		},
	})
	if err != nil {
		log.Error(err, "[RunLVGWatcherCacheController] unable to create controller")
		return err
	}

	err = c.Watch(source.Kind(mgr.GetCache(), &snc.LVMVolumeGroup{},
		&handler.TypedEnqueueRequestForObject[*snc.LVMVolumeGroup]{},
		predicate.TypedFuncs[*snc.LVMVolumeGroup]{
			CreateFunc: func(_ event.TypedCreateEvent[*snc.LVMVolumeGroup]) bool {
				return true
			},
			UpdateFunc: func(_ event.TypedUpdateEvent[*snc.LVMVolumeGroup]) bool {
				return true
			},
			DeleteFunc: func(_ event.TypedDeleteEvent[*snc.LVMVolumeGroup]) bool {
				return false
			},
		},
	))
	if err != nil {
		log.Error(err, "[RunLVGWatcherCacheController] unable to controller Watch")
		return err
	}

	log.Info("[RunLVGWatcherCacheController] controller started successfully")
	return nil
}

type lvgReconciler struct {
	cl    client.Client
	cache *cache.Cache
	log   logger.Logger
}

func (r *lvgReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	r.log.Debug(fmt.Sprintf("[lvgReconciler] reconciling LVG %s, recalibrating unaccounted space", req.Name))

	lvg := &snc.LVMVolumeGroup{}
	if err := r.cl.Get(ctx, req.NamespacedName, lvg); err != nil {
		if client.IgnoreNotFound(err) == nil {
			r.log.Debug(fmt.Sprintf("[lvgReconciler] LVG %s not found (deleted), skipping", req.Name))
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("unable to get LVG %s: %w", req.Name, err)
	}

	if err := scheduler.CalibrateAllPoolsForLVG(ctx, r.cl, r.cache, lvg); err != nil {
		r.log.Error(err, fmt.Sprintf("[lvgReconciler] failed to calibrate LVG %s", req.Name))
		return reconcile.Result{}, err
	}

	r.log.Debug(fmt.Sprintf("[lvgReconciler] successfully calibrated unaccounted space for LVG %s", req.Name))
	return reconcile.Result{}, nil
}
