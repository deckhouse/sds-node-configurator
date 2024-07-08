/*
Copyright 2023 Flant JSC

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
	"agent/config"
	"agent/internal"
	"agent/pkg/cache"
	"agent/pkg/logger"
	"agent/pkg/monitoring"
	"agent/pkg/utils"
	"context"
	"errors"
	"fmt"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	errors2 "k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/apimachinery/pkg/types"
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
	LVMVolumeGroupWatcherCtrlName = "lvm-volume-group-watcher-controller"
)

func RunLVMVolumeGroupWatcherController(
	mgr manager.Manager,
	cfg config.Options,
	log logger.Logger,
	metrics monitoring.Metrics,
	sdsCache *cache.Cache,
) (controller.Controller, error) {
	cl := mgr.GetClient()
	mgrCache := mgr.GetCache()

	c, err := controller.New(LVMVolumeGroupWatcherCtrlName, mgr, controller.Options{
		Reconciler: reconcile.Func(func(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
			log.Info(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] Reconciler starts to reconcile the request %s", request.NamespacedName.String()))

			lvg := &v1alpha1.LvmVolumeGroup{}
			err := cl.Get(ctx, request.NamespacedName, lvg)
			if err != nil {
				if errors2.IsNotFound(err) {
					log.Warning(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] seems like the LVMVolumeGroup was deleted as unable to get it, err: %s. Stop to reconcile", err.Error()))
					return reconcile.Result{}, nil
				}

				log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to get a LVMVolumeGroup by NamespacedName %s", request.NamespacedName.String()))
				return reconcile.Result{}, err
			}

			if lvg.Name == "" {
				log.Info(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] seems like the LVMVolumeGroup for the request %s was deleted. Reconcile retrying will stop.", request.Name))
				return reconcile.Result{}, nil
			}

			log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] tries to add the finalizer %s to the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
			added, err := addLVGFinalizerIfNotExist(ctx, cl, lvg)
			if err != nil {
				log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to add the finalizer %s to the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
				return reconcile.Result{}, err
			}

			if added {
				log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] successfully added a finalizer %s to the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
			} else {
				log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] no need to add a finalizer %s to the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
			}

			blockDevices, err := GetAPIBlockDevices(ctx, cl, metrics)
			if err != nil {
				log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to get BlockDevices. Retry in %s", cfg.BlockDeviceScanIntervalSec.String()))
				err = updateLVGConditionIfNeeded(ctx, cl, log, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, "NoBlockDevices", fmt.Sprintf("unable to get block devices resources, err: %s", err.Error()))
				if err != nil {
					log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to add a condition %s to the LVMVolumeGroup %s. Retry in %s", internal.TypeVGConfigurationApplied, lvg.Name, cfg.BlockDeviceScanIntervalSec.String()))
				}

				return reconcile.Result{
					RequeueAfter: cfg.BlockDeviceScanIntervalSec,
				}, nil
			}
			log.Debug("[RunLVMVolumeGroupController] successfully got BlockDevices")

			valid, reason := validateSpecBlockDevices(lvg, blockDevices)
			if !valid {
				log.Warning(fmt.Sprintf("[RunLVMVolumeGroupController] validation failed for the LVMVolumeGroup %s, reason: %s", lvg.Name, reason))
				err = updateLVGConditionIfNeeded(ctx, cl, log, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, "InvalidSpec", reason)
				if err != nil {
					log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to add a condition %s to the LVMVolumeGroup %s. Retry in %s", internal.TypeVGConfigurationApplied, lvg.Name, cfg.VolumeGroupScanIntervalSec.String()))
					return reconcile.Result{}, err
				}

				return reconcile.Result{}, nil
			}
			log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] successfully validated BlockDevices of the LVMVolumeGroup %s", lvg.Name))

			belongs := checkIfLVGBelongsToNode(lvg, blockDevices, cfg.NodeName)
			if !belongs {
				log.Info(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] the LVMVolumeGroup %s does not belong to the node %s", lvg.Name, cfg.NodeName))
				return reconcile.Result{}, nil
			}
			log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] the LVMVolumeGroup %s belongs to the node %s. Starts to reconcile", lvg.Name, cfg.NodeName))

			// this case handles the situation when a user decides to remove LVMVolumeGroup resource without created VG
			vgs, _ := sdsCache.GetVGs()
			if !checkIfVGExist(lvg.Spec.ActualVGNameOnTheNode, vgs) && lvg.DeletionTimestamp != nil {
				log.Info(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] VG %s was not yet created for the LVMVolumeGroup %s and the resource is marked as deleting. Delete the resource", lvg.Spec.ActualVGNameOnTheNode, lvg.Name))
				removed, err := removeLVGFinalizerIfExist(ctx, cl, lvg)
				if err != nil {
					log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to remove the finalizer %s from the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
					return reconcile.Result{}, err
				}

				if removed {
					log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] successfully removed the finalizer %s from the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
				} else {
					log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] no need to remove the finalizer %s from the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
				}

				err = DeleteLVMVolumeGroup(ctx, cl, log, metrics, lvg, cfg.NodeName)
				if err != nil {
					log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to delete the LVMVolumeGroup %s", lvg.Name))
					return reconcile.Result{}, err
				} else {
					log.Info(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] successfully deleted the LVMVolumeGroup %s", lvg.Name))
				}

				return reconcile.Result{}, nil
			}
			// We do this after BlockDevices validation and node belonging check to prevent multiple updates by all agents pods
			bds, _ := sdsCache.GetDevices()
			if len(bds) == 0 {
				log.Warning(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] no block devices in the cache, add the LVMVolumeGroup %s to requeue", lvg.Name))
				err = updateLVGConditionIfNeeded(ctx, cl, log, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, "CacheEmpty", "unable to apply configuration due to the cache's state")
				if err != nil {
					log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to add a condition %s to the LVMVolumeGroup %s. Retry in %s", internal.TypeVGConfigurationApplied, lvg.Name, cfg.VolumeGroupScanIntervalSec.String()))
				}

				return reconcile.Result{
					RequeueAfter: cfg.VolumeGroupScanIntervalSec,
				}, nil
			}

			log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] tries to sync status and spec thin-pool AllicationLimit fields for the LVMVolumeGroup %s", lvg.Name))
			err = syncThinPoolsAllocationLimit(ctx, cl, log, lvg)
			if err != nil {
				log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to sync status and spec thin-pool AllocationLimit fields for the LVMVolumeGroup %s", lvg.Name))
				return reconcile.Result{}, err
			}

			shouldRequeue, err := runEventReconcile(ctx, cl, log, metrics, sdsCache, cfg, lvg, blockDevices)
			if err != nil {
				log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to reconcile the LVMVolumeGroup %s", lvg.Name))
			}

			if shouldRequeue {
				log.Warning(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] the LVMVolumeGroup %s event will be requeued in %s", lvg.Name, cfg.VolumeGroupScanIntervalSec.String()))
				return reconcile.Result{
					RequeueAfter: cfg.VolumeGroupScanIntervalSec,
				}, nil
			}
			log.Info(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] Reconciler successfully reconciled the LVMVolumeGroup %s", lvg.Name))

			return reconcile.Result{}, nil
		}),
	})

	if err != nil {
		log.Error(err, "[RunLVMVolumeGroupWatcherController] Unable to create controller RunLVMVolumeGroupWatcherController")
		return nil, err
	}

	err = c.Watch(source.Kind(mgrCache, &v1alpha1.LvmVolumeGroup{}), handler.Funcs{
		CreateFunc: func(ctx context.Context, e event.CreateEvent, q workqueue.RateLimitingInterface) {
			log.Info(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] createFunc got a create event for the LVMVolumeGroup, name: %s", e.Object.GetName()))

			request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: e.Object.GetNamespace(), Name: e.Object.GetName()}}
			q.Add(request)

			log.Info(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] createFunc added a request for the LVMVolumeGroup %s to the Reconcilers queue", e.Object.GetName()))
		},
		UpdateFunc: func(ctx context.Context, e event.UpdateEvent, q workqueue.RateLimitingInterface) {
			log.Info(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] UpdateFunc got a update event for the LVMVolumeGroup %s", e.ObjectNew.GetName()))

			oldLVG, ok := e.ObjectOld.(*v1alpha1.LvmVolumeGroup)
			if !ok {
				err = errors.New("unable to cast event object to a given type")
				log.Error(err, "[RunLVMVolumeGroupWatcherController] an error occurred while handling a create event")
				return
			}
			log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] successfully casted an old state of the LVMVolumeGroup %s", oldLVG.Name))

			newLVG, ok := e.ObjectNew.(*v1alpha1.LvmVolumeGroup)
			if !ok {
				err = errors.New("unable to cast event object to a given type")
				log.Error(err, "[RunLVMVolumeGroupWatcherController] an error occurred while handling a create event")
				return
			}
			log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] successfully casted a new state of the LVMVolumeGroup %s", newLVG.Name))

			if !shouldReconcileUpdateEvent(log, oldLVG, newLVG) {
				log.Info(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] update event for the LVMVolumeGroup %s should not be reconciled as not target changed were made", newLVG.Name))
				return
			}

			request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: e.ObjectNew.GetNamespace(), Name: e.ObjectNew.GetName()}}
			q.Add(request)

			log.Info(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] updateFunc added a request for the LVMVolumeGroup %s to the Reconcilers queue", newLVG.Name))
		},
	})

	if err != nil {
		log.Error(err, "[RunLVMVolumeGroupWatcherController] error Watch controller RunLVMVolumeGroupWatcherController")
		return nil, err
	}
	return c, err
}

func runEventReconcile(
	ctx context.Context,
	cl client.Client,
	log logger.Logger,
	metrics monitoring.Metrics,
	sdsCache *cache.Cache,
	cfg config.Options,
	lvg *v1alpha1.LvmVolumeGroup,
	blockDevices map[string]v1alpha1.BlockDevice,
) (bool, error) {
	recType := identifyLVGReconcileFunc(lvg, sdsCache)

	switch recType {
	case CreateReconcile:
		log.Info(fmt.Sprintf("[runEventReconcile] CreateReconcile starts the reconciliation for the LVMVolumeGroup %s", lvg.Name))
		return reconcileLVGCreateFunc(ctx, cl, log, metrics, lvg, blockDevices)
	case UpdateReconcile:
		log.Info(fmt.Sprintf("[runEventReconcile] UpdateReconcile starts the reconciliation for the LVMVolumeGroup %s", lvg.Name))
		return reconcileLVGUpdateFunc(ctx, cl, log, metrics, sdsCache, lvg, blockDevices)
	case DeleteReconcile:
		log.Info(fmt.Sprintf("[runEventReconcile] DeleteReconcile starts the reconciliation for the LVMVolumeGroup %s", lvg.Name))
		return reconcileLVGDeleteFunc(ctx, cl, log, metrics, sdsCache, cfg, lvg)
	default:
		log.Info(fmt.Sprintf("[runEventReconcile] no need to reconcile the LVMVolumeGroup %s", lvg.Name))
	}
	return false, nil
}

func reconcileLVGDeleteFunc(ctx context.Context, cl client.Client, log logger.Logger, metrics monitoring.Metrics, sdsCache *cache.Cache, cfg config.Options, lvg *v1alpha1.LvmVolumeGroup) (bool, error) {
	log.Debug(fmt.Sprintf("[reconcileLVGDeleteFunc] starts to reconcile the LVMVolumeGroup %s", lvg.Name))
	log.Debug(fmt.Sprintf("[reconcileLVGDeleteFunc] tries to add the condition %s status false to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))

	// this check prevents the LVMVolumeGroup resource's infinity updating after a retry
	for _, c := range lvg.Status.Conditions {
		if c.Type == internal.TypeVGConfigurationApplied && c.Reason != internal.ReasonTerminating {
			err := updateLVGConditionIfNeeded(ctx, cl, log, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonTerminating, "trying to delete VG")
			if err != nil {
				log.Error(err, fmt.Sprintf("[reconcileLVGDeleteFunc] unable to add the condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
				return true, err
			}
			break
		}
	}

	_, exist := lvg.Annotations[deletionProtectionAnnotation]
	if exist {
		log.Debug(fmt.Sprintf("[reconcileLVGDeleteFunc] the LVMVolumeGroup %s has a deletion timestamp but also has a deletion protection annotation %s. Remove it to proceed the delete operation", lvg.Name, deletionProtectionAnnotation))
		err := updateLVGConditionIfNeeded(ctx, cl, log, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonTerminating, fmt.Sprintf("to delete the LVG remove the annotation %s", deletionProtectionAnnotation))
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLVGDeleteFunc] unable to add the condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
			return true, err
		}

		return false, nil
	}

	log.Debug(fmt.Sprintf("[reconcileLVGDeleteFunc] check if VG %s of the LVMVolumeGroup %s uses LVs", lvg.Spec.ActualVGNameOnTheNode, lvg.Name))
	usedLVs := checkIfVGHasLV(sdsCache, lvg.Spec.ActualVGNameOnTheNode)
	if len(usedLVs) > 0 {
		err := fmt.Errorf("VG %s uses LVs: %v. Delete used LVs first", lvg.Spec.ActualVGNameOnTheNode, usedLVs)
		log.Error(err, fmt.Sprintf("[reconcileLVGDeleteFunc] unable to reconcile LVG %s", lvg.Name))
		log.Debug(fmt.Sprintf("[reconcileLVGDeleteFunc] tries to add the condition %s status False to the LVMVolumeGroup %s due to LV does exist", internal.TypeVGConfigurationApplied, lvg.Name))
		err = updateLVGConditionIfNeeded(ctx, cl, log, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonTerminating, err.Error())
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLVGDeleteFunc] unable to add the condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
			return true, err
		}

		return true, nil
	}

	log.Debug(fmt.Sprintf("[reconcileLVGDeleteFunc] VG %s of the LVMVolumeGroup %s does not use any LV. Start to delete the VG", lvg.Spec.ActualVGNameOnTheNode, lvg.Name))
	err := DeleteVGIfExist(log, metrics, sdsCache, lvg.Spec.ActualVGNameOnTheNode)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLVGDeleteFunc] unable to delete VG %s", lvg.Spec.ActualVGNameOnTheNode))
		err = updateLVGConditionIfNeeded(ctx, cl, log, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonTerminating, err.Error())
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLVGDeleteFunc] unable to add the condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
			return true, err
		}

		return true, err
	}

	removed, err := removeLVGFinalizerIfExist(ctx, cl, lvg)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLVGDeleteFunc] unable to remove a finalizer %s from the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
		err = updateLVGConditionIfNeeded(ctx, cl, log, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonTerminating, err.Error())
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLVGDeleteFunc] unable to add the condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
		}
		return true, err
	}

	if removed {
		log.Debug(fmt.Sprintf("[reconcileLVGDeleteFunc] successfully removed a finalizer %s from the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
	} else {
		log.Debug(fmt.Sprintf("[reconcileLVGDeleteFunc] no need to remove a finalizer %s from the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
	}

	err = DeleteLVMVolumeGroup(ctx, cl, log, metrics, lvg, cfg.NodeName)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLVGDeleteFunc] unable to delete the LVMVolumeGroup %s", lvg.Name))
		return true, err
	}

	log.Info(fmt.Sprintf("[reconcileLVGDeleteFunc] successfully reconciled VG %s of the LVMVolumeGroup %s", lvg.Spec.ActualVGNameOnTheNode, lvg.Name))
	return false, nil
}

func reconcileLVGUpdateFunc(ctx context.Context, cl client.Client, log logger.Logger, metrics monitoring.Metrics, sdsCache *cache.Cache, lvg *v1alpha1.LvmVolumeGroup, blockDevices map[string]v1alpha1.BlockDevice) (bool, error) {
	log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] starts to reconcile the LVMVolumeGroup %s", lvg.Name))

	log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] tries to validate the LVMVolumeGroup %s", lvg.Name))
	pvs, _ := sdsCache.GetPVs()
	valid, reason := validateLVGForUpdateFunc(log, lvg, blockDevices, pvs)
	if !valid {
		log.Warning(fmt.Sprintf("[reconcileLVGUpdateFunc] the LVMVolumeGroup %s is not valid", lvg.Name))
		err := updateLVGConditionIfNeeded(ctx, cl, log, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonValidationFailed, reason)
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to add a condition %s reason %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, internal.ReasonValidationFailed, lvg.Name))
			return true, err
		}

		return false, err
	}
	log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] successfully validated the LVMVolumeGroup %s", lvg.Name))

	log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] tries to get VG %s for the LVMVolumeGroup %s", lvg.Spec.ActualVGNameOnTheNode, lvg.Name))
	found, vg := tryGetVG(sdsCache, lvg.Spec.ActualVGNameOnTheNode)
	if !found {
		err := errors.New(fmt.Sprintf("VG %s not found", lvg.Spec.ActualVGNameOnTheNode))
		log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to reconcile the LVMVolumeGroup %s", lvg.Name))
		err = updateLVGConditionIfNeeded(ctx, cl, log, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, "VGNotFound", err.Error())
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
		}
		return true, err
	}
	log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] VG %s found for the LVMVolumeGroup %s", vg.VGName, lvg.Name))

	log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] tries to check and update VG %s tag %s", lvg.Spec.ActualVGNameOnTheNode, internal.LVMTags[0]))
	updated, err := UpdateVGTagIfNeeded(ctx, cl, log, metrics, lvg, vg)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to update VG %s tag of the LVMVolumeGroup %s", vg.VGName, lvg.Name))
		err = updateLVGConditionIfNeeded(ctx, cl, log, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, "VGUpdateFailed", fmt.Sprintf("unable to update VG tag, err: %s", err.Error()))
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
		}

		return true, err
	}

	if updated {
		log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] successfully updated VG %s tag of the LVMVolumeGroup %s", vg.VGName, lvg.Name))
	} else {
		log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] no need to update VG %s tag of the LVMVolumeGroup %s", vg.VGName, lvg.Name))
	}

	log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] starts to extend VG %s of the LVMVolumeGroup %s", vg.VGName, lvg.Name))
	err = ExtendVGIfNeeded(ctx, cl, log, metrics, lvg, vg, pvs, blockDevices)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to extend VG of the LVMVolumeGroup %s", lvg.Name))
		err = updateLVGConditionIfNeeded(ctx, cl, log, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, "VGExtendFailed", fmt.Sprintf("unable to extend VG, err: %s", err.Error()))
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
		}

		return true, err
	}
	log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] successfully ended the extend operation for VG of the LVMVolumeGroup %s", lvg.Name))

	log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] starts to resize PV of the LVMVolumeGroup %s", lvg.Name))
	err = ResizePVIfNeeded(ctx, cl, log, metrics, lvg)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to resize PV of the LVMVolumeGroup %s", lvg.Name))
		err = updateLVGConditionIfNeeded(ctx, cl, log, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, "PVResizeFailed", fmt.Sprintf("unable to resize PV, err: %s", err.Error()))
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
		}
		return true, err
	}
	log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] successfully ended the resize operation for PV of the LVMVolumeGroup %s", lvg.Name))

	if lvg.Spec.ThinPools != nil {
		log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] starts to reconcile thin-pools of the LVMVolumeGroup %s", lvg.Name))
		lvs, _ := sdsCache.GetLVs()
		err = ReconcileThinPoolsIfNeeded(ctx, cl, log, metrics, lvg, vg, lvs)
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to reconcile thin-pools of the LVMVolumeGroup %s", lvg.Name))
			err = updateLVGConditionIfNeeded(ctx, cl, log, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, "ThinPoolReconcileFailed", fmt.Sprintf("unable to reconcile thin-pools, err: %s", err.Error()))
			if err != nil {
				log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
			}
			return true, err
		}
		log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] successfully reconciled thin-pools operation of the LVMVolumeGroup %s", lvg.Name))
	}

	log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] tries to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
	err = updateLVGConditionIfNeeded(ctx, cl, log, lvg, v1.ConditionTrue, internal.TypeVGConfigurationApplied, "Applied", "configuration has been applied")
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
		return true, err
	}
	log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] successfully added a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
	log.Info(fmt.Sprintf("[reconcileLVGUpdateFunc] successfully reconciled the LVMVolumeGroup %s", lvg.Name))

	return false, nil
}

func reconcileLVGCreateFunc(ctx context.Context, cl client.Client, log logger.Logger, metrics monitoring.Metrics, lvg *v1alpha1.LvmVolumeGroup, blockDevices map[string]v1alpha1.BlockDevice) (bool, error) {
	log.Debug(fmt.Sprintf("[reconcileLVGCreateFunc] starts to reconcile the LVMVolumeGroup %s", lvg.Name))

	// this check prevents the LVMVolumeGroup resource's infinity updating after a retry
	exist := false
	for _, c := range lvg.Status.Conditions {
		if c.Type == internal.TypeVGConfigurationApplied {
			exist = true
			break
		}
	}

	if !exist {
		log.Debug(fmt.Sprintf("[reconcileLVGCreateFunc] tries to add the condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
		err := updateLVGConditionIfNeeded(ctx, cl, log, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonCreating, "trying to apply the configuration")
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLVGCreateFunc] unable to add the condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
			return true, err
		}
	}

	log.Debug(fmt.Sprintf("[reconcileLVGCreateFunc] tries to validate the LVMVolumeGroup %s", lvg.Name))
	valid, reason := validateLVGForCreateFunc(log, lvg, blockDevices)
	if !valid {
		log.Warning(fmt.Sprintf("[reconcileLVGCreateFunc] validation fails for the LVMVolumeGroup %s", lvg.Name))
		err := updateLVGConditionIfNeeded(ctx, cl, log, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonValidationFailed, reason)
		if err != nil {
			log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
			return true, err
		}

		return false, err
	}
	log.Debug(fmt.Sprintf("[reconcileLVGCreateFunc] successfully validated the LVMVolumeGroup %s", lvg.Name))

	log.Debug(fmt.Sprintf("[reconcileLVGCreateFunc] tries to create VG for the LVMVolumeGroup %s", lvg.Name))
	err := CreateVGComplex(metrics, log, lvg, blockDevices)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLVGCreateFunc] unable to create VG for the LVMVolumeGroup %s", lvg.Name))
		err = updateLVGConditionIfNeeded(ctx, cl, log, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, "VGCreationFailed", fmt.Sprintf("unable to create VG, err: %s", err.Error()))
		if err != nil {
			log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
		}
		return true, err
	}
	log.Info(fmt.Sprintf("[reconcileLVGCreateFunc] successfully created VG for the LVMVolumeGroup %s", lvg.Name))

	if lvg.Spec.ThinPools != nil {
		log.Debug(fmt.Sprintf("[reconcileLVGCreateFunc] the LVMVolumeGroup %s has thin-pools. Tries to create them", lvg.Name))

		for _, tp := range lvg.Spec.ThinPools {
			cmd, err := utils.CreateThinPool(tp.Name, lvg.Spec.ActualVGNameOnTheNode, tp.Size.Value())
			if err != nil {
				log.Error(err, fmt.Sprintf("[reconcileLVGCreateFunc] unable to create thin-pool %s of the LVMVolumeGroup %s, cmd: %s", tp.Name, lvg.Name, cmd))
				err = updateLVGConditionIfNeeded(ctx, cl, log, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, "ThinPoolCreationFailed", fmt.Sprintf("unable to create thin-pool, err: %s", err.Error()))
				if err != nil {
					log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
				}

				return true, err
			}
		}
		log.Debug(fmt.Sprintf("[reconcileLVGCreateFunc] successfully created thin-pools for the LVMVolumeGroup %s", lvg.Name))
	}

	err = updateLVGConditionIfNeeded(ctx, cl, log, lvg, v1.ConditionTrue, internal.TypeVGConfigurationApplied, "Success", "all configuration has been applied")
	if err != nil {
		log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
		return true, err
	}

	return false, nil
}
