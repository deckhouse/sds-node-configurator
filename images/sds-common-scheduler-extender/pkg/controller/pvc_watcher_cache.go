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
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	snc "github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/cache"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/scheduler"
)

const (
	PVCWatcherCacheCtrlName = "pvc-watcher-cache-controller"
	defaultNarrowTTL        = 60 * time.Second
)

func RunPVCWatcherCacheController(
	mgr manager.Manager,
	log logger.Logger,
	schedulerCache *cache.Cache,
) error {
	log.Info("[RunPVCWatcherCacheController] starts the work")

	c, err := controller.New(PVCWatcherCacheCtrlName, mgr, controller.Options{
		Reconciler: reconcile.Func(func(_ context.Context, _ reconcile.Request) (reconcile.Result, error) {
			return reconcile.Result{}, nil
		}),
	})
	if err != nil {
		log.Error(err, "[RunPVCWatcherCacheController] unable to create controller")
		return err
	}

	err = c.Watch(source.Kind(mgr.GetCache(), &corev1.PersistentVolumeClaim{}, handler.TypedFuncs[*corev1.PersistentVolumeClaim, reconcile.Request]{
		CreateFunc: func(ctx context.Context, e event.TypedCreateEvent[*corev1.PersistentVolumeClaim], _ workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			log.Info("[RunPVCWatcherCacheController] CreateFunc reconciliation starts")
			pvc := e.Object
			log.Debug(fmt.Sprintf("[RunPVCWatcherCacheController] CreateFunc starts the reconciliation for the PVC %s/%s", pvc.Namespace, pvc.Name))

			if pvc.Annotations == nil {
				log.Debug(fmt.Sprintf("[RunPVCWatcherCacheController] PVC %s/%s should not be reconciled by CreateFunc due to annotations is nil", pvc.Namespace, pvc.Name))
				return
			}

			selectedNodeName, wasSelected := pvc.Annotations[cache.SelectedNodeAnnotation]
			if !wasSelected || pvc.Status.Phase == corev1.ClaimBound || pvc.DeletionTimestamp != nil {
				log.Debug(fmt.Sprintf("[RunPVCWatcherCacheController] PVC %s/%s should not be reconciled by CreateFunc", pvc.Namespace, pvc.Name))
				return
			}

			log.Debug(fmt.Sprintf("[RunPVCWatcherCacheController] PVC %s/%s has selected node %s, narrowing reservation", pvc.Namespace, pvc.Name, selectedNodeName))
			narrowReservationToNode(ctx, mgr.GetClient(), log, schedulerCache, pvc, selectedNodeName)
			log.Info("[RunPVCWatcherCacheController] CreateFunc reconciliation ends")
		},
		UpdateFunc: func(ctx context.Context, e event.TypedUpdateEvent[*corev1.PersistentVolumeClaim], _ workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			log.Info("[RunPVCWatcherCacheController] UpdateFunc reconciliation starts")
			pvc := e.ObjectNew
			log.Debug(fmt.Sprintf("[RunPVCWatcherCacheController] UpdateFunc starts the reconciliation for the PVC %s/%s", pvc.Namespace, pvc.Name))

			pvcKey := pvc.Namespace + "/" + pvc.Name

			// If PVC is bound, remove reservation entirely
			if pvc.Status.Phase == corev1.ClaimBound {
				log.Debug(fmt.Sprintf("[RunPVCWatcherCacheController] PVC %s is bound, removing reservation", pvcKey))
				schedulerCache.RemoveReservation(pvcKey)
				log.Info("[RunPVCWatcherCacheController] UpdateFunc reconciliation ends")
				return
			}

			if pvc.DeletionTimestamp != nil {
				log.Debug(fmt.Sprintf("[RunPVCWatcherCacheController] PVC %s is being deleted, removing reservation", pvcKey))
				schedulerCache.RemoveReservation(pvcKey)
				log.Info("[RunPVCWatcherCacheController] UpdateFunc reconciliation ends")
				return
			}

			if pvc.Annotations == nil {
				log.Debug(fmt.Sprintf("[RunPVCWatcherCacheController] PVC %s/%s annotations is nil, skipping", pvc.Namespace, pvc.Name))
				log.Info("[RunPVCWatcherCacheController] UpdateFunc reconciliation ends")
				return
			}

			selectedNodeName, wasSelected := pvc.Annotations[cache.SelectedNodeAnnotation]
			if !wasSelected {
				log.Debug(fmt.Sprintf("[RunPVCWatcherCacheController] PVC %s/%s has no selected node annotation, skipping", pvc.Namespace, pvc.Name))
				log.Info("[RunPVCWatcherCacheController] UpdateFunc reconciliation ends")
				return
			}

			log.Debug(fmt.Sprintf("[RunPVCWatcherCacheController] PVC %s/%s has selected node %s, narrowing reservation", pvc.Namespace, pvc.Name, selectedNodeName))
			narrowReservationToNode(ctx, mgr.GetClient(), log, schedulerCache, pvc, selectedNodeName)
			log.Info("[RunPVCWatcherCacheController] UpdateFunc reconciliation ends")
		},
		DeleteFunc: func(_ context.Context, e event.TypedDeleteEvent[*corev1.PersistentVolumeClaim], _ workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			log.Info("[RunPVCWatcherCacheController] DeleteFunc reconciliation starts")
			pvc := e.Object
			pvcKey := pvc.Namespace + "/" + pvc.Name
			log.Debug(fmt.Sprintf("[RunPVCWatcherCacheController] PVC %s was deleted, removing reservation", pvcKey))
			schedulerCache.RemoveReservation(pvcKey)
			log.Info("[RunPVCWatcherCacheController] DeleteFunc reconciliation ends")
		},
	},
	),
	)
	if err != nil {
		log.Error(err, "[RunPVCWatcherCacheController] unable to controller Watch")
		return err
	}

	return nil
}

// narrowReservationToNode narrows the reservation to only include pools on the selected node.
// Uses the field indexer to find all LVGs on the node and builds the keepPools set.
func narrowReservationToNode(
	ctx context.Context,
	cl client.Client,
	log logger.Logger,
	schedulerCache *cache.Cache,
	pvc *corev1.PersistentVolumeClaim,
	selectedNodeName string,
) {
	pvcKey := pvc.Namespace + "/" + pvc.Name

	if !schedulerCache.HasReservation(pvcKey) {
		log.Debug(fmt.Sprintf("[narrowReservationToNode] no reservation found for PVC %s, skipping", pvcKey))
		return
	}

	// Get all LVGs on the selected node via field indexer
	var lvgList snc.LVMVolumeGroupList
	if err := cl.List(ctx, &lvgList, client.MatchingFields{scheduler.IndexFieldLVGNodeName: selectedNodeName}); err != nil {
		log.Error(err, fmt.Sprintf("[narrowReservationToNode] unable to list LVGs for node %s", selectedNodeName))
		return
	}

	// Build keepPools: all possible StoragePoolKeys on this node
	var keepPools []cache.StoragePoolKey
	for _, lvg := range lvgList.Items {
		// Add thick pool key
		keepPools = append(keepPools, cache.StoragePoolKey{LVGName: lvg.Name})
		// Add all thin pool keys
		for _, tp := range lvg.Status.ThinPools {
			keepPools = append(keepPools, cache.StoragePoolKey{LVGName: lvg.Name, ThinPoolName: tp.Name})
		}
	}

	ok := schedulerCache.NarrowReservation(pvcKey, keepPools, defaultNarrowTTL)
	if ok {
		log.Debug(fmt.Sprintf("[narrowReservationToNode] narrowed reservation %s to node %s (%d pools)", pvcKey, selectedNodeName, len(keepPools)))
	} else {
		log.Debug(fmt.Sprintf("[narrowReservationToNode] reservation %s not found (may have expired)", pvcKey))
	}
}
