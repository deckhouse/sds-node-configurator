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
	"errors"
	"fmt"
	"slices"

	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/cache"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/consts"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/scheduler"

	slv "github.com/deckhouse/sds-local-volume/api/v1alpha1"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const (
	PVCWatcherCacheCtrlName = "pvc-watcher-cache-controller"
)

func RunPVCWatcherCacheController(
	mgr manager.Manager,
	log *logger.Logger,
	cacheMgr *cache.CacheManager,
) error {
	log.Info("[RunPVCWatcherCacheController] starts the work")

	c, err := controller.New("pvc-watcher-cache-controller", mgr, controller.Options{
		Reconciler: reconcile.Func(func(_ context.Context, _ reconcile.Request) (reconcile.Result, error) {
			return reconcile.Result{}, nil
		}),
	})
	if err != nil {
		log.Error(err, "[RunPVCWatcherCacheController] unable to create controller")
		return err
	}

	err = c.Watch(source.Kind(mgr.GetCache(), &v1.PersistentVolumeClaim{}, handler.TypedFuncs[*v1.PersistentVolumeClaim, reconcile.Request]{
		CreateFunc: func(ctx context.Context, e event.TypedCreateEvent[*v1.PersistentVolumeClaim], _ workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			log.Info("[RunPVCWatcherCacheController] CreateFunc reconciliation starts")
			pvc := e.Object
			log.Debug(fmt.Sprintf("[RunPVCWatcherCacheController] CreateFunc starts the reconciliation for the PVC %s/%s", pvc.Namespace, pvc.Name))

			if pvc.Annotations == nil {
				log.Debug(fmt.Sprintf("[RunPVCWatcherCacheController] PVC %s/%s should not be reconciled by CreateFunc due to annotations is nil", pvc.Namespace, pvc.Name))
				return
			}

			selectedNodeName, wasSelected := pvc.Annotations[cache.SelectedNodeAnnotation]
			if !wasSelected || pvc.Status.Phase == v1.ClaimBound || pvc.DeletionTimestamp != nil {
				log.Debug(fmt.Sprintf("[RunPVCWatcherCacheController] PVC %s/%s should not be reconciled by CreateFunc due to no selected node annotation found or deletion timestamp is not nil", pvc.Namespace, pvc.Name))
				return
			}
			log.Debug(fmt.Sprintf("[RunPVCWatcherCacheController] PVC %s/%s has selected node annotation, it will be reconciled in CreateFunc", pvc.Namespace, pvc.Name))
			log.Trace(fmt.Sprintf("[RunPVCWatcherCacheController] PVC %s/%s has been selected to the node %s", pvc.Namespace, pvc.Name, selectedNodeName))

			reconcilePVC(ctx, mgr, log, cacheMgr, pvc, selectedNodeName)
			log.Info("[RunPVCWatcherCacheController] CreateFunc reconciliation ends")
		},
		UpdateFunc: func(ctx context.Context, e event.TypedUpdateEvent[*v1.PersistentVolumeClaim], _ workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			log.Info("[RunPVCWatcherCacheController] Update Func reconciliation starts")
			pvc := e.ObjectNew
			log.Debug(fmt.Sprintf("[RunPVCWatcherCacheController] UpdateFunc starts the reconciliation for the PVC %s/%s", pvc.Namespace, pvc.Name))

			if pvc.Annotations == nil {
				log.Debug(fmt.Sprintf("[RunPVCWatcherCacheController] PVC %s/%s should not be reconciled by UpdateFunc due to annotations is nil", pvc.Namespace, pvc.Name))
				return
			}

			selectedNodeName, wasSelected := pvc.Annotations[cache.SelectedNodeAnnotation]
			if !wasSelected || pvc.DeletionTimestamp != nil {
				log.Debug(fmt.Sprintf("[RunPVCWatcherCacheController] PVC %s/%s should not be reconciled by UpdateFunc due to no selected node annotation found or deletion timestamp is not nil", pvc.Namespace, pvc.Name))
				return
			}
			log.Debug(fmt.Sprintf("[RunPVCWatcherCacheController] PVC %s/%s has selected node annotation, it will be reconciled in UpdateFunc", pvc.Namespace, pvc.Name))
			log.Trace(fmt.Sprintf("[RunPVCWatcherCacheController] PVC %s/%s has been selected to the node %s", pvc.Namespace, pvc.Name, selectedNodeName))

			reconcilePVC(ctx, mgr, log, cacheMgr, pvc, selectedNodeName)
			log.Info("[RunPVCWatcherCacheController] Update Func reconciliation ends")
		},
		DeleteFunc: func(_ context.Context, e event.TypedDeleteEvent[*v1.PersistentVolumeClaim], _ workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			log.Info("[RunPVCWatcherCacheController] Delete Func reconciliation starts")
			pvc := e.Object
			log.Debug(fmt.Sprintf("[RunPVCWatcherCacheController] DeleteFunc starts the reconciliation for the PVC %s/%s", pvc.Namespace, pvc.Name))

			log.Debug(fmt.Sprintf("[RunPVCWatcherCacheController] PVC %s/%s was removed from the cluster. It will be fully removed from the cache", pvc.Namespace, pvc.Name))
			cacheMgr.RemovePVCFromTheCache(pvc)
			log.Debug(fmt.Sprintf("[RunPVCWatcherCacheController] successfully fully removed PVC %s/%s from the cache", pvc.Namespace, pvc.Name))
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

func reconcilePVC(ctx context.Context, mgr manager.Manager, log *logger.Logger, cacheMgr *cache.CacheManager, pvc *v1.PersistentVolumeClaim, selectedNodeName string) {
	sc := &storagev1.StorageClass{}
	err := mgr.GetClient().Get(ctx, client.ObjectKey{
		Name: *pvc.Spec.StorageClassName,
	}, sc)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcilePVC] unable to get Storage Class %s for PVC %s/%s", *pvc.Spec.StorageClassName, pvc.Namespace, pvc.Name))
		return
	}

	if sc.Provisioner != consts.SdsReplicatedVolumeProvisioner {
		log.Debug(fmt.Sprintf("[reconcilePVC] Storage Class %s for PVC %s/%s is not managed by sds-replicated-volume-provisioner. Ends the reconciliation", sc.Name, pvc.Namespace, pvc.Name))
		return
	}

	log.Debug(fmt.Sprintf("[reconcilePVC] tries to extract LVGs from the Storage Class %s for PVC %s/%s", sc.Name, pvc.Namespace, pvc.Name))
	lvgsFromSc, err := scheduler.ExtractLVGsFromSC(sc)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcilePVC] unable to extract LVMVolumeGroups from the Storage Class %s", sc.Name))
	}
	log.Debug(fmt.Sprintf("[reconcilePVC] successfully extracted LVGs from the Storage Class %s for PVC %s/%s", sc.Name, pvc.Namespace, pvc.Name))

	lvgsForPVC := cacheMgr.GetLVGNamesForPVC(pvc)
	if len(lvgsForPVC) == 0 {
		log.Debug(fmt.Sprintf("[reconcilePVC] no LVMVolumeGroups were found in the cache for PVC %s/%s. Use Storage Class %s instead", pvc.Namespace, pvc.Name, *pvc.Spec.StorageClassName))

		for _, lvg := range lvgsFromSc {
			lvgsForPVC = append(lvgsForPVC, lvg.Name)
		}
	}
	for _, lvgName := range lvgsForPVC {
		log.Trace(fmt.Sprintf("[reconcilePVC] LVMVolumeGroup %s belongs to PVC %s/%s", lvgName, pvc.Namespace, pvc.Name))
	}

	log.Debug(fmt.Sprintf("[reconcilePVC] starts to find common LVMVolumeGroup for the selected node %s and PVC %s/%s", selectedNodeName, pvc.Namespace, pvc.Name))
	lvgsOnTheNode := cacheMgr.GetLVGNamesByNodeName(selectedNodeName)
	for _, lvgName := range lvgsOnTheNode {
		log.Trace(fmt.Sprintf("[reconcilePVC] LVMVolumeGroup %s belongs to the node %s", lvgName, selectedNodeName))
	}

	var commonLVGName string
	for _, pvcLvg := range lvgsForPVC {
		if slices.Contains(lvgsOnTheNode, pvcLvg) {
			commonLVGName = pvcLvg
			break
		}
	}
	if commonLVGName == "" {
		log.Error(errors.New("common LVMVolumeGroup was not found"), fmt.Sprintf("[reconcilePVC] unable to identify a LVMVolumeGroup for PVC %s/%s", pvc.Namespace, pvc.Name))
		return
	}

	log.Debug(fmt.Sprintf("[reconcilePVC] successfully found common LVMVolumeGroup %s for the selected node %s and PVC %s/%s", commonLVGName, selectedNodeName, pvc.Namespace, pvc.Name))
	log.Debug(fmt.Sprintf("[reconcilePVC] starts to update PVC %s/%s in the cache", pvc.Namespace, pvc.Name))

	log.Trace(fmt.Sprintf("[reconcilePVC] %s PVC %s/%s has status phase: %s", sc.Parameters[consts.LvmTypeParamKey], pvc.Namespace, pvc.Name, pvc.Status.Phase))
	switch sc.Parameters[consts.LvmTypeParamKey] {
	case consts.Thick:
		err = cacheMgr.UpdateThickPVC(commonLVGName, pvc, sc.Provisioner)
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcilePVC] unable to update Thick PVC %s/%s in the cache", pvc.Namespace, pvc.Name))
			return
		}
	case consts.Thin:
		for _, lvg := range lvgsFromSc {
			if lvg.Name == commonLVGName {
				err = cacheMgr.UpdateThinPVC(commonLVGName, lvg.Thin.PoolName, pvc, sc.Provisioner)
				if err != nil {
					log.Error(err, fmt.Sprintf("[reconcilePVC] unable to update Thin PVC %s/%s in the cache", pvc.Namespace, pvc.Name))
					return
				}
				break
			}
		}
	}
	log.Debug(fmt.Sprintf("[reconcilePVC] successfully updated %s PVC %s/%s in the cache", sc.Parameters[consts.LvmTypeParamKey], pvc.Namespace, pvc.Name))

	log.Cache(fmt.Sprintf("[reconcilePVC] cache state BEFORE the removal space reservation for PVC %s/%s", pvc.Namespace, pvc.Name))
	cacheMgr.PrintTheCacheLog()
	log.Debug(fmt.Sprintf("[reconcilePVC] starts to remove space reservation for PVC %s/%s with selected node from the cache", pvc.Namespace, pvc.Name))

	err = mgr.GetClient().Get(ctx, client.ObjectKey{Name: sc.Name, Namespace: sc.Namespace}, &slv.LocalStorageClass{})
	if err != nil {
		// Space reservation removal when node is selected now happens in sds-local-volume-only
		if !k8serrors.IsNotFound(err) {
			removalErr := cacheMgr.RemoveSpaceReservationForPVCWithSelectedNode(pvc, sc.Parameters[consts.LvmTypeParamKey])
			if removalErr != nil {
				log.Error(removalErr, fmt.Sprintf("[reconcilePVC] unable to remove PVC %s/%s space reservation in the cache", pvc.Namespace, pvc.Name))
			}
		} else {
			log.Error(err, fmt.Sprintf("[reconcilePVC] unable to get LocalStorageClass %s/%s", sc.Namespace, sc.Name))
			return
		}
	}
	log.Debug(fmt.Sprintf("[reconcilePVC] successfully removed space reservation for PVC %s/%s with selected node", pvc.Namespace, pvc.Name))

	log.Cache(fmt.Sprintf("[reconcilePVC] cache state AFTER the removal space reservation for PVC %s/%s", pvc.Namespace, pvc.Name))
	cacheMgr.PrintTheCacheLog()
}
