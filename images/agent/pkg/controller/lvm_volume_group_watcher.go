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
	"context"
	"errors"
	"fmt"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/strings/slices"
	"reflect"
	"sds-node-configurator/api/v1alpha1"
	"sds-node-configurator/internal"
	"sds-node-configurator/pkg/cache"
	"sds-node-configurator/pkg/logger"
	"sds-node-configurator/pkg/monitoring"
	"sds-node-configurator/pkg/utils"
	"strings"
	"time"

	"sds-node-configurator/config"

	"k8s.io/apimachinery/pkg/api/resource"
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
				log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to get a LVMVolumeGroup by NamespacedName %s", request.NamespacedName.String()))
				return reconcile.Result{}, err
			}

			if lvg.Name == "" {
				log.Info(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] seems like the LVMVolumeGroup for the request %s was deleted. Reconcile retrying will stop.", request.Name))
				return reconcile.Result{}, nil
			}

			blockDevices, err := GetAPIBlockDevices(ctx, cl, metrics)
			if err != nil {
				log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to get BlockDevices. Retry in %s", cfg.BlockDeviceScanIntervalSec.String()))
				err = addConditionToLVG(ctx, cl, log, lvg, v1.ConditionFalse, internal.VGConfigurationAppliedType, "NoBlockDevices", fmt.Sprintf("unable to get block devices resources, err: %s", err.Error()))
				if err != nil {
					log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to add a condition %s to the LVMVolumeGroup %s. Retry in %s", internal.VGConfigurationAppliedType, lvg.Name, cfg.BlockDeviceScanIntervalSec.String()))
				}

				return reconcile.Result{
					RequeueAfter: cfg.BlockDeviceScanIntervalSec,
				}, nil
			}
			log.Debug("[RunLVMVolumeGroupController] successfully got BlockDevices")

			valid, reason := validateSpecBlockDevices(lvg, blockDevices)
			if !valid {
				err = updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, lvg, NonOperational, reason)
				if err != nil {
					log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to update the LVMVolumeGroup %s", lvg.Name))
				}
				err = addConditionToLVG(ctx, cl, log, lvg, v1.ConditionFalse, internal.VGConfigurationAppliedType, "InvalidSpec", "invalid configuration. Check the status.message for more information")
				if err != nil {
					log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to add a condition %s to the LVMVolumeGroup %s. Retry in %s", internal.VGConfigurationAppliedType, lvg.Name, cfg.VolumeGroupScanIntervalSec.String()))
				}
				return reconcile.Result{
					RequeueAfter: cfg.VolumeGroupScanIntervalSec,
				}, nil
			}
			log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] successfully validated BlockDevices of the LVMVolumeGroup %s", lvg.Name))

			belongs := checkIfLVGBelongsToNode(lvg, blockDevices, cfg.NodeName)
			if !belongs {
				log.Info(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] the LVMVolumeGroup %s does not belong to the node %s", lvg.Name, cfg.NodeName))
				return reconcile.Result{}, nil
			}
			log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] the LVMVolumeGroup %s belongs to the node %s. Starts to reconcile", lvg.Name, cfg.NodeName))

			bds, _ := sdsCache.GetDevices()
			if len(bds) == 0 {
				log.Warning(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] no block devices in the cache, add to requeue the LVMVolumeGroup %s", lvg.Name))
				err = addConditionToLVG(ctx, cl, log, lvg, v1.ConditionFalse, internal.VGConfigurationAppliedType, "CacheEmpty", "unable to apply configuration due to the cache's state")
				if err != nil {
					log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to add a condition %s to the LVMVolumeGroup %s. Retry in %s", internal.VGConfigurationAppliedType, lvg.Name, cfg.VolumeGroupScanIntervalSec.String()))
				}

				return reconcile.Result{
					RequeueAfter: cfg.VolumeGroupScanIntervalSec,
				}, nil
			}

			shouldRequeue, err := runEventReconcile(ctx, cl, log, metrics, sdsCache, lvg, blockDevices)
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

			newLVG, ok := e.ObjectNew.(*v1alpha1.LvmVolumeGroup)
			if !ok {
				err = errors.New("unable to cast event object to a given type")
				log.Error(err, "[RunLVMVolumeGroupWatcherController] an error occurred while handling a create event")
				return
			}
			log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] successfully casted a new state of the LVMVolumeGroup %s", newLVG.Name))

			oldLVG, ok := e.ObjectOld.(*v1alpha1.LvmVolumeGroup)
			if !ok {
				err = errors.New("unable to cast event object to a given type")
				log.Error(err, "[RunLVMVolumeGroupWatcherController] an error occurred while handling a create event")
				return
			}
			log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] successfully casted an old state of the LVMVolumeGroup %s", newLVG.Name))

			if !shouldLVGUpdateEventTriggers(log, oldLVG, newLVG) {
				log.Info(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] the LVMVolumeGroup %s should not be reconciled", newLVG.Name))
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

func addConditionToLVG(ctx context.Context, cl client.Client, log logger.Logger, lvg *v1alpha1.LvmVolumeGroup, status v1.ConditionStatus, conType, reason, message string) error {
	exist := false
	index := 0
	condition := v1.Condition{
		Type:               conType,
		Status:             status,
		ObservedGeneration: lvg.Generation,
		LastTransitionTime: v1.NewTime(time.Now()),
		Reason:             reason,
		Message:            message,
	}

	if lvg.Status.Conditions == nil {
		log.Debug(fmt.Sprintf("[addConditionToLVG] the LVMVolumeGroup %s conditions is nil. Initialize them", lvg.Name))
		lvg.Status.Conditions = make([]v1.Condition, 0, 2)
	}

	if len(lvg.Status.Conditions) > 0 {
		log.Debug(fmt.Sprintf("[addConditionToLVG] there are some conditions in the LVMVolumeGroup %s. Tries to find a condition %s", lvg.Name, conType))
		for i, c := range lvg.Status.Conditions {
			if c.Type == conType {
				index = i
				exist = true
				log.Debug(fmt.Sprintf("[addConditionToLVG] a condition %s was found in the LVMVolumeGroup %s at the index %d", conType, lvg.Name, i))
			}
		}

		if !exist {
			log.Debug(fmt.Sprintf("[addConditionToLVG] a condition %s was not found. Append it in the end of the LVMVolumeGroup %s conditions", conType, lvg.Name))
			lvg.Status.Conditions = append(lvg.Status.Conditions, condition)
		} else {
			log.Debug(fmt.Sprintf("[addConditionToLVG] insert the condition %s at index %d of the LVMVolumeGroup %s conditions", conType, index, lvg.Name))
			lvg.Status.Conditions[index] = condition
		}
	} else {
		log.Debug(fmt.Sprintf("[addConditionToLVG] no conditions were found in the LVMVolumeGroup %s. Append the condition %s in the end", lvg.Name, conType))
		lvg.Status.Conditions = append(lvg.Status.Conditions, condition)
	}

	return cl.Status().Update(ctx, lvg)
}

func shouldLVGUpdateEventTriggers(log logger.Logger, oldLVG, newLVG *v1alpha1.LvmVolumeGroup) bool {
	if hasLVGSpecDiff(oldLVG.Spec, newLVG.Spec) {
		log.Debug(fmt.Sprintf("[shouldLVGUpdateEventTriggers] the LVMVolumeGroup %s old and new states have different Spec", newLVG.Name))
		return true
	}
	log.Debug(fmt.Sprintf("[shouldLVGUpdateEventTriggers] the LVMVolumeGroup %s old and new states have the same Spec", newLVG.Name))

	log.Trace(fmt.Sprintf("[shouldLVGUpdateEventTriggers] old LVMVolumeGroup %s nodes: %+v", oldLVG.Name, oldLVG.Status.Nodes))
	log.Trace(fmt.Sprintf("[shouldLVGUpdateEventTriggers] new LVMVolumeGroup %s nodes: %+v", newLVG.Name, newLVG.Status.Nodes))
	if hasStatusNodesDiff(log, oldLVG.Status.Nodes, newLVG.Status.Nodes) {
		log.Debug(fmt.Sprintf("[shouldLVGUpdateEventTriggers] the LVMVolumeGroup %s old and new states have different Status.Nodes", newLVG.Name))
		return true
	}
	log.Debug(fmt.Sprintf("[shouldLVGUpdateEventTriggers] the LVMVolumeGroup %s old and new states have the same Status.Nodes", newLVG.Name))

	if hasPVSizeDevSizeDiff(newLVG) {
		log.Debug(fmt.Sprintf("[shouldLVGUpdateEventTriggers] the LVMVolumeGroup %s Status.Nodes device size is different from its PV size", newLVG.Name))
		return true
	}
	log.Debug(fmt.Sprintf("[shouldLVGUpdateEventTriggers] the LVMVolumeGroup %s Status.Nodes device sizes are the same as PV ones", newLVG.Name))

	if !reflect.DeepEqual(oldLVG.Annotations, newLVG.Annotations) {
		log.Trace(fmt.Sprintf("[shouldLVGUpdateEventTriggers] the LVMVolumeGroup %s old annotaions: %v, new annotations: %v", newLVG.Name, oldLVG.Annotations, newLVG.Annotations))
		log.Debug(fmt.Sprintf("[shouldLVGUpdateEventTriggers] the LVMVolumeGroup %s old and new states have different Annotations", newLVG.Name))
		return true
	}
	log.Debug(fmt.Sprintf("[shouldLVGUpdateEventTriggers] the LVMVolumeGroup %s old and new states have the same Annotations", newLVG.Name))

	return false
}

func hasPVSizeDevSizeDiff(lvg *v1alpha1.LvmVolumeGroup) bool {
	for _, n := range lvg.Status.Nodes {
		for _, d := range n.Devices {
			if !utils.AreSizesEqualWithinDelta(d.DevSize, d.PVSize, resource.MustParse(internal.ResizeDelta)) {
				return true
			}
		}
	}

	return false
}

func hasLVGSpecDiff(first, second v1alpha1.LvmVolumeGroupSpec) bool {
	if len(first.ThinPools) != len(second.ThinPools) {
		return true
	}

	for i := range first.ThinPools {
		if first.ThinPools[i].Size.Value() != second.ThinPools[i].Size.Value() ||
			first.ThinPools[i].Name != second.ThinPools[i].Name {
			return true
		}
	}

	if first.ActualVGNameOnTheNode != second.ActualVGNameOnTheNode ||
		!reflect.DeepEqual(first.BlockDeviceNames, second.BlockDeviceNames) ||
		first.Type != second.Type {
		return true
	}

	return false
}

func runEventReconcile(
	ctx context.Context,
	cl client.Client,
	log logger.Logger,
	metrics monitoring.Metrics,
	sdsCache *cache.Cache,
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
		return reconcileLVGDeleteFunc(ctx, cl, log, metrics, sdsCache, lvg)
	default:
		log.Info(fmt.Sprintf("[runEventReconcile] no need to reconcile the LVMVolumeGroup %s", lvg.Name))
	}
	return false, nil
}

func reconcileLVGDeleteFunc(ctx context.Context, cl client.Client, log logger.Logger, metrics monitoring.Metrics, sdsCache *cache.Cache, lvg *v1alpha1.LvmVolumeGroup) (bool, error) {
	log.Debug(fmt.Sprintf("[reconcileLVGDeleteFunc] starts to reconcile the LVMVolumeGroup %s", lvg.Name))

	_, exist := lvg.Annotations[delAnnotation]
	if lvg.DeletionTimestamp != nil && !exist {
		err := updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, lvg, NonOperational, fmt.Sprintf("to delete the LVG annotate it with %s", delAnnotation))
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLVGDeleteFunc] unable to update the LVMVolumeGroup %s", lvg.Name))
			return true, err
		}

		return false, nil
	}

	log.Debug(fmt.Sprintf("[reconcileLVGDeleteFunc] check if VG %s of the LVMVolumeGroup %s uses LVs", lvg.Spec.ActualVGNameOnTheNode, lvg.Name))
	usedLVs := checkIfVGHasLV(sdsCache, lvg.Spec.ActualVGNameOnTheNode)

	if len(usedLVs) > 0 {
		err := fmt.Errorf("VG %s uses LVs: %v. Delete used LVs first", lvg.Spec.ActualVGNameOnTheNode, usedLVs)
		log.Error(err, fmt.Sprintf("[reconcileLVGDeleteFunc] unable to reconcile LVG %s", lvg.Name))
		err = updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, lvg, NonOperational, err.Error())
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLVGDeleteFunc] unable to update the LVMVolumeGroup %s", lvg.Name))
		}
		return true, nil
	}

	log.Debug(fmt.Sprintf("[reconcileLVGDeleteFunc] VG %s of the LVMVolumeGroup %s does not use any LV. Start to delete the VG", lvg.Spec.ActualVGNameOnTheNode, lvg.Name))
	err := DeleteVGIfExist(log, metrics, sdsCache, lvg.Spec.ActualVGNameOnTheNode)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLVGDeleteFunc] unable to delete VG %s", lvg.Spec.ActualVGNameOnTheNode))
		err = updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, lvg, NonOperational, err.Error())
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLVGDeleteFunc] unable to update the LVMVolumeGroup %s", lvg.Name))
		}
		return true, err
	}

	removed, err := removeLVGFinalizerIfExist(ctx, cl, lvg)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLVGDeleteFunc] unable to remove a finalizer %s from the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
		return true, err
	}

	if removed {
		log.Debug(fmt.Sprintf("[reconcileLVGDeleteFunc] successfully removed a finalizer %s from the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
	} else {
		log.Debug(fmt.Sprintf("[reconcileLVGDeleteFunc] no need to remove a finalizer %s from the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
	}

	log.Info(fmt.Sprintf("[reconcileLVGDeleteFunc] successfully deleted VG %s of the LVMVolumeGroup %s", lvg.Spec.ActualVGNameOnTheNode, lvg.Name))
	return false, nil
}

func removeLVGFinalizerIfExist(ctx context.Context, cl client.Client, lvg *v1alpha1.LvmVolumeGroup) (bool, error) {
	if !slices.Contains(lvg.Finalizers, internal.SdsNodeConfiguratorFinalizer) {
		return false, nil
	}

	for i := range lvg.Finalizers {
		if lvg.Finalizers[i] == internal.SdsNodeConfiguratorFinalizer {
			lvg.Finalizers = append(lvg.Finalizers[:i], lvg.Finalizers[i+1:]...)
			break
		}
	}

	err := cl.Update(ctx, lvg)
	if err != nil {
		return false, err
	}

	return true, nil
}

func checkIfVGHasLV(ch *cache.Cache, vgName string) []string {
	lvs, _ := ch.GetLVs()
	usedLVs := make([]string, 0, len(lvs))
	for _, lv := range lvs {
		if lv.VGName == vgName {
			usedLVs = append(usedLVs, lv.LVName)
		}
	}

	return usedLVs
}

func reconcileLVGUpdateFunc(ctx context.Context, cl client.Client, log logger.Logger, metrics monitoring.Metrics, sdsCache *cache.Cache, lvg *v1alpha1.LvmVolumeGroup, blockDevices map[string]v1alpha1.BlockDevice) (bool, error) {
	log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] starts to reconcile the LVMVolumeGroup %s", lvg.Name))

	log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] tries to add a finalizer to the LVMVolumeGroup %s", lvg.Name))
	added, err := addLVGFinalizerIfNotExist(ctx, cl, lvg)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLVGCreateFunc] unable to add a finalizer %s to the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
		err = addConditionToLVG(ctx, cl, log, lvg, v1.ConditionFalse, internal.VGConfigurationAppliedType, "UpdatingFailed", fmt.Sprintf("unable to add a finalizer, err: %s", err.Error()))
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to add a condition %s to the LVMVolumeGroup %s", internal.VGConfigurationAppliedType, lvg.Name))
		}
		return true, err
	}

	if added {
		log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] successfully added a finalizer %s to the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
	} else {
		log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] no need to add a finalizer %s to the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
	}

	log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] tries to validate the LVMVolumeGroup %s", lvg.Name))
	pvs, _ := sdsCache.GetPVs()
	valid, reason := validateLVGForUpdateFunc(log, lvg, blockDevices, pvs)
	if !valid {
		log.Warning(fmt.Sprintf("[reconcileLVGUpdateFunc] the LVMVolumeGroup %s is not valid", lvg.Name))
		err := updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, lvg, NonOperational, reason)
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to update the LVMVolumeGroup %s", lvg.Name))
			return true, err
		}

		err = addConditionToLVG(ctx, cl, log, lvg, v1.ConditionFalse, internal.VGConfigurationAppliedType, "ValidationFailed", "configuration is not valid, check status.message for more information")
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to add a condition %s to the LVMVolumeGroup %s", internal.VGConfigurationAppliedType, lvg.Name))
		}

		return false, err
	}
	log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] successfully validated the LVMVolumeGroup %s", lvg.Name))

	log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] tries to get VG %s for the LVMVolumeGroup %s", lvg.Spec.ActualVGNameOnTheNode, lvg.Name))
	found, vg := tryGetVG(sdsCache, lvg.Spec.ActualVGNameOnTheNode)
	if !found {
		err := errors.New(fmt.Sprintf("VG %s not found", lvg.Spec.ActualVGNameOnTheNode))
		log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to reconcile the LVMVolumeGroup %s", lvg.Name))
		err = updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, lvg, NonOperational, err.Error())
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to update the LVMVolumeGroup %s", lvg.Name))
		}

		err = addConditionToLVG(ctx, cl, log, lvg, v1.ConditionFalse, internal.VGConfigurationAppliedType, "VGNotFound", fmt.Sprintf("and existing VG %s as not found", lvg.Spec.ActualVGNameOnTheNode))
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to add a condition %s to the LVMVolumeGroup %s", internal.VGConfigurationAppliedType, lvg.Name))
		}
		return true, err
	}
	log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] VG %s found for the LVMVolumeGroup %s", vg.VGName, lvg.Name))

	log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] tries to check and update VG %s tag %s", lvg.Spec.ActualVGNameOnTheNode, internal.LVMTags[0]))
	updated, err := UpdateVGTagIfNeeded(log, metrics, vg, lvg.Name)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to update VG %s tag of the LVMVolumeGroup %s", vg.VGName, lvg.Name))
		err = addConditionToLVG(ctx, cl, log, lvg, v1.ConditionFalse, internal.VGConfigurationAppliedType, "VGUpdateFailed", fmt.Sprintf("unable to update VG tag, err: %s", err.Error()))
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to add a condition %s to the LVMVolumeGroup %s", internal.VGConfigurationAppliedType, lvg.Name))
		}

		err = updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, lvg, NonOperational, err.Error())
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to update the LVMVolumeGroup %s", lvg.Name))
		}

		return true, err
	}

	if updated {
		log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] successfully updated VG %s tag of the LVMVolumeGroup %s", vg.VGName, lvg.Name))
	} else {
		log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] no need to update VG %s tag of the LVMVolumeGroup %s", vg.VGName, lvg.Name))
	}

	log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] starts to extend VG %s of the LVMVolumeGroup %s", vg.VGName, lvg.Name))
	err = ExtendVGIfNeeded(log, metrics, lvg, vg, pvs, blockDevices)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to extend VG of the LVMVolumeGroup %s", lvg.Name))
		err = addConditionToLVG(ctx, cl, log, lvg, v1.ConditionFalse, internal.VGConfigurationAppliedType, "VGExtendFailed", fmt.Sprintf("unable to extend VG, err: %s", err.Error()))
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to add a condition %s to the LVMVolumeGroup %s", internal.VGConfigurationAppliedType, lvg.Name))
		}

		err = updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, lvg, NonOperational, err.Error())
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to update the LVMVolumeGroup %s", lvg.Name))
		}

		return true, err
	}
	log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] successfully ended the extend operation for VG of the LVMVolumeGroup %s", lvg.Name))

	log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] starts to resize PV of the LVMVolumeGroup %s", lvg.Name))
	err = ResizePVIfNeeded(log, metrics, lvg)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to resize PV of the LVMVolumeGroup %s", lvg.Name))
		err = addConditionToLVG(ctx, cl, log, lvg, v1.ConditionFalse, internal.VGConfigurationAppliedType, "PVResizeFailed", fmt.Sprintf("unable to resize PV, err: %s", err.Error()))
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to add a condition %s to the LVMVolumeGroup %s", internal.VGConfigurationAppliedType, lvg.Name))
		}

		err = updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, lvg, NonOperational, err.Error())
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to update the LVMVolumeGroup %s", lvg.Name))
		}
		return true, err
	}
	log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] successfully ended the resize operation for PV of the LVMVolumeGroup %s", lvg.Name))

	if lvg.Spec.ThinPools != nil {
		log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] starts to reconcile thin-pools of the LVMVolumeGroup %s", lvg.Name))
		lvs, _ := sdsCache.GetLVs()
		err = ReconcileThinPoolsIfNeeded(log, metrics, lvg, vg, lvs)
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to reconcile thin-pools of the LVMVolumeGroup %s", lvg.Name))
			err = addConditionToLVG(ctx, cl, log, lvg, v1.ConditionFalse, internal.VGConfigurationAppliedType, "ThinPoolReconcileFailed", fmt.Sprintf("unable to reconcile thin-pools, err: %s", err.Error()))
			if err != nil {
				log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to add a condition %s to the LVMVolumeGroup %s", internal.VGConfigurationAppliedType, lvg.Name))
			}

			err = updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, lvg, NonOperational, err.Error())
			if err != nil {
				log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to update the LVMVolumeGroup %s", lvg.Name))
			}
			return true, err
		}
		log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] successfully reconciled thin-pools operation of the LVMVolumeGroup %s", lvg.Name))
	}

	log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] tries to add a condition %s to the LVMVolumeGroup %s", internal.VGConfigurationAppliedType, lvg.Name))
	err = addConditionToLVG(ctx, cl, log, lvg, v1.ConditionTrue, internal.VGConfigurationAppliedType, "Success", "all configuration has been applied")
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to add a condition %s to the LVMVolumeGroup %s", internal.VGConfigurationAppliedType, lvg.Name))
		return true, err
	}
	log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] successfully added a condition %s to the LVMVolumeGroup %s", internal.VGConfigurationAppliedType, lvg.Name))

	log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] tries to update health status of the LVMVolumeGroup %s", lvg.Name))
	err = updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, lvg, Operational, "")
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to update the LVMVolumeGroup %s", lvg.Name))
		return true, err
	}
	log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] successfully updated health status of the LVMVolumeGroup %s", lvg.Name))

	log.Info(fmt.Sprintf("[reconcileLVGUpdateFunc] successfully reconciled the LVMVolumeGroup %s", lvg.Name))

	return false, nil
}

func ReconcileThinPoolsIfNeeded(log logger.Logger, metrics monitoring.Metrics, lvg *v1alpha1.LvmVolumeGroup, vg internal.VGData, lvs []internal.LVData) error {
	actualThinPools := make(map[string]v1alpha1.StatusThinPool, len(lvg.Status.ThinPools))
	for _, tp := range lvg.Status.ThinPools {
		actualThinPools[tp.Name] = tp
	}

	lvsMap := make(map[string]struct{}, len(lvs))
	for _, lv := range lvs {
		lvsMap[lv.LVName] = struct{}{}
	}

	errs := strings.Builder{}
	for _, specTp := range lvg.Spec.ThinPools {
		if statusTp, exist := actualThinPools[specTp.Name]; !exist {
			if _, lvExist := lvsMap[specTp.Name]; lvExist {
				log.Warning(fmt.Sprintf("[ReconcileThinPoolsIfNeeded] thin-pool %s is created on the node, but isn't shown in the LVMVolumeGroup %s status. Check the status after the next resources update", specTp.Name, lvg.Name))
				continue
			}
			log.Debug(fmt.Sprintf("[ReconcileThinPoolsIfNeeded] thin-pool %s of the LVMVolumeGroup %s is not created yet. Create it", specTp.Name, lvg.Name))

			start := time.Now()
			cmd, err := utils.CreateThinPool(specTp.Name, vg.VGName, specTp.Size.Value())
			metrics.UtilsCommandsDuration(LVMVolumeGroupWatcherCtrlName, "lvcreate").Observe(metrics.GetEstimatedTimeInSeconds(start))
			metrics.UtilsCommandsExecutionCount(LVMVolumeGroupWatcherCtrlName, "lvcreate").Inc()
			if err != nil {
				metrics.UtilsCommandsErrorsCount(LVMVolumeGroupWatcherCtrlName, "lvcreate").Inc()
				log.Error(err, fmt.Sprintf("[ReconcileThinPoolsIfNeeded] unable to create thin-pool %s of the LVMVolumeGroup %s, cmd: %s", specTp.Name, lvg.Name, cmd))
				errs.WriteString(fmt.Sprintf("unable to create thin-pool %s, err: %s. ", specTp.Name, err.Error()))
				continue
			}

			log.Info(fmt.Sprintf("[ReconcileThinPoolsIfNeeded] thin-pool %s of the LVMVolumeGroup %s has been successfully created", specTp.Name, lvg.Name))
		} else {
			log.Debug(fmt.Sprintf("[ReconcileThinPoolsIfNeeded] thin-pool %s of the LVMVolumeGroup %s is already created. Check its size", specTp.Name, lvg.Name))
			delta, err := resource.ParseQuantity(internal.ResizeDelta)
			if err != nil {
				log.Error(err, fmt.Sprintf("[ReconcileThinPoolsIfNeeded] unable to parse the resize delta: %s", internal.ResizeDelta))
				errs.WriteString(err.Error())
				continue
			}
			log.Debug(fmt.Sprintf("[ReconcileThinPoolsIfNeeded] successfully parsed the resize delta %s", internal.ResizeDelta))

			if utils.AreSizesEqualWithinDelta(specTp.Size, statusTp.ActualSize, delta) {
				log.Debug(fmt.Sprintf("[ReconcileThinPoolsIfNeeded] the LVMVolumeGroup %s requested thin pool %s size is equal to actual one within delta %s", lvg.Name, specTp.Size.String(), delta.String()))
				continue
			}

			log.Debug(fmt.Sprintf("[ReconcileThinPoolsIfNeeded] the LVMVolumeGroup %s requested thin pool %s size is more than actual one. Resize it", lvg.Name, specTp.Size.String()))
			err = ResizeThinPool(log, metrics, lvg, specTp, statusTp)
			if err != nil {
				log.Error(err, fmt.Sprintf("[ReconcileThinPoolsIfNeeded] unable to resize thin-pool %s of the LVMVolumeGroup %s", specTp.Name, lvg.Name))
				errs.WriteString(fmt.Sprintf("unable to resize thin-pool %s, err: %s. ", specTp.Name, err.Error()))
				continue
			}
		}
	}

	if errs.Len() != 0 {
		return errors.New(errs.String())
	}

	return nil
}

func ResizePVIfNeeded(log logger.Logger, metrics monitoring.Metrics, lvg *v1alpha1.LvmVolumeGroup) error {
	delta, err := resource.ParseQuantity(internal.ResizeDelta)
	if err != nil {
		log.Error(err, fmt.Sprintf("[ResizePVIfNeeded] unable to parse the resize delta: %s", internal.ResizeDelta))
	}
	log.Debug(fmt.Sprintf("[ResizePVIfNeeded] successfully parsed the resize delta %s", internal.ResizeDelta))

	if len(lvg.Status.Nodes) == 0 {
		log.Warning(fmt.Sprintf("[ResizePVIfNeeded] the LVMVolumeGroup %s nodes are empty. Wait for the next update", lvg.Name))
		return nil
	}

	errs := strings.Builder{}
	for _, n := range lvg.Status.Nodes {
		for _, d := range n.Devices {
			if d.DevSize.Value()-d.PVSize.Value() > delta.Value() {
				log.Debug(fmt.Sprintf("[ResizePVIfNeeded] the LVMVolumeGroup %s BlockDevice %s PVSize is less than actual device size. Resize PV", lvg.Name, d.BlockDevice))

				start := time.Now()
				cmd, err := utils.ResizePV(d.Path)
				metrics.UtilsCommandsDuration(LVMVolumeGroupWatcherCtrlName, "pvresize").Observe(metrics.GetEstimatedTimeInSeconds(start))
				metrics.UtilsCommandsExecutionCount(LVMVolumeGroupWatcherCtrlName, "pvresize")
				if err != nil {
					metrics.UtilsCommandsErrorsCount(LVMVolumeGroupWatcherCtrlName, "pvresize").Inc()
					log.Error(err, fmt.Sprintf("[ResizePVIfNeeded] unable to resize PV %s of BlockDevice %s of LVMVolumeGroup %s, cmd: %s", d.Path, d.BlockDevice, lvg.Name, cmd))
					errs.WriteString(fmt.Sprintf("unable to resize PV %s, err: %s. ", d.Path, err.Error()))
					continue
				}

				log.Info(fmt.Sprintf("[ResizePVIfNeeded] successfully resized PV %s of BlockDevice %s of LVMVolumeGroup %s", d.Path, d.BlockDevice, lvg.Name))
			} else {
				log.Debug(fmt.Sprintf("[ResizePVIfNeeded] no need to resize PV %s of BlockDevice %s of the LVMVolumeGroup %s", d.Path, d.BlockDevice, lvg.Name))
			}
		}
	}

	if errs.Len() != 0 {
		return errors.New(errs.String())
	}

	return nil
}

func ExtendVGIfNeeded(log logger.Logger, metrics monitoring.Metrics, lvg *v1alpha1.LvmVolumeGroup, vg internal.VGData, pvs []internal.PVData, blockDevices map[string]v1alpha1.BlockDevice) error {
	for _, n := range lvg.Status.Nodes {
		for _, d := range n.Devices {
			log.Trace(fmt.Sprintf("[ExtendVGIfNeeded] the LVMVolumeGroup %s status block device: %s", lvg.Name, d.BlockDevice))
		}
	}

	pvsMap := make(map[string]struct{}, len(pvs))
	for _, pv := range pvs {
		pvsMap[pv.PVName] = struct{}{}
	}

	devicesToExtend := make([]string, 0, len(lvg.Spec.BlockDeviceNames))
	for _, bdName := range lvg.Spec.BlockDeviceNames {
		bd := blockDevices[bdName]
		if _, exist := pvsMap[bd.Status.Path]; !exist {
			log.Debug(fmt.Sprintf("[ExtendVGIfNeeded] the BlockDevice %s of LVMVolumeGroup %s Spec is not counted as used", bdName, lvg.Name))
			devicesToExtend = append(devicesToExtend, bdName)
		}
	}

	if len(devicesToExtend) == 0 {
		log.Debug(fmt.Sprintf("[ExtendVGIfNeeded] VG %s of the LVMVolumeGroup %s should not be extended", vg.VGName, lvg.Name))
		return nil
	}

	log.Debug(fmt.Sprintf("[ExtendVGIfNeeded] VG %s should be extended as there are some BlockDevices were added to Spec field of the LVMVolumeGroup %s", vg.VGName, lvg.Name))
	paths := extractPathsFromBlockDevices(devicesToExtend, blockDevices)
	err := ExtendVGComplex(metrics, paths, vg.VGName, log)
	if err != nil {
		log.Error(err, fmt.Sprintf("[ExtendVGIfNeeded] unable to extend VG %s of the LVMVolumeGroup %s", vg.VGName, lvg.Name))
		return err
	}
	log.Info(fmt.Sprintf("[ExtendVGIfNeeded] VG %s of the LVMVolumeGroup %s was extended", vg.VGName, lvg.Name))

	return nil
}

func tryGetVG(sdsCache *cache.Cache, vgName string) (bool, internal.VGData) {
	vgs, _ := sdsCache.GetVGs()
	for _, vg := range vgs {
		if vg.VGName == vgName {
			return true, vg
		}
	}

	return false, internal.VGData{}
}

func reconcileLVGCreateFunc(ctx context.Context, cl client.Client, log logger.Logger, metrics monitoring.Metrics, lvg *v1alpha1.LvmVolumeGroup, blockDevices map[string]v1alpha1.BlockDevice) (bool, error) {
	log.Debug(fmt.Sprintf("[reconcileLVGCreateFunc] starts to reconcile the LVMVolumeGroup %s", lvg.Name))
	log.Debug(fmt.Sprintf("[reconcileLVGCreateFunc] tries to add a finalizer to the LVMVolumeGroup %s", lvg.Name))
	added, err := addLVGFinalizerIfNotExist(ctx, cl, lvg)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLVGCreateFunc] unable to add a finalizer %s to the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
		err = addConditionToLVG(ctx, cl, log, lvg, v1.ConditionFalse, internal.VGConfigurationAppliedType, "UpdatingFailed", fmt.Sprintf("unable to add a finalizer, err: %s", err.Error()))
		if err != nil {
			log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to add a condition %s to the LVMVolumeGroup %s", internal.VGConfigurationAppliedType, lvg.Name))
		}
		return true, err
	}

	if added {
		log.Debug(fmt.Sprintf("[reconcileLVGCreateFunc] successfully added a finalizer %s to the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
	} else {
		log.Debug(fmt.Sprintf("[reconcileLVGCreateFunc] no need to add a finalizer %s to the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
	}

	log.Debug(fmt.Sprintf("[reconcileLVGCreateFunc] tries to validate the LVMVolumeGroup %s", lvg.Name))
	valid, reason := validateLVGForCreateFunc(log, lvg, blockDevices)
	if !valid {
		log.Warning(fmt.Sprintf("[reconcileLVGCreateFunc] validation fails for the LVMVolumeGroup %s", lvg.Name))
		err = addConditionToLVG(ctx, cl, log, lvg, v1.ConditionFalse, internal.VGConfigurationAppliedType, "ValidationFailed", "configuration is not valid, check status.message for more information")
		if err != nil {
			log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to add a condition %s to the LVMVolumeGroup %s", internal.VGConfigurationAppliedType, lvg.Name))
		}

		err := updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, lvg, NonOperational, reason)
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLVGCreateFunc] unable to update the LVMVolumeGroup %s", lvg.Name))
		}

		return true, err
	}
	log.Debug(fmt.Sprintf("[reconcileLVGCreateFunc] successfully validated the LVMVolumeGroup %s", lvg.Name))

	log.Debug(fmt.Sprintf("[reconcileLVGCreateFunc] tries to create VG for the LVMVolumeGroup %s", lvg.Name))
	err = CreateVGComplex(metrics, log, lvg, blockDevices)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLVGCreateFunc] unable to create VG for the LVMVolumeGroup %s", lvg.Name))
		err = addConditionToLVG(ctx, cl, log, lvg, v1.ConditionFalse, internal.VGConfigurationAppliedType, "VGCreationFailed", fmt.Sprintf("unable to create VG, err: %s", err.Error()))
		if err != nil {
			log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to add a condition %s to the LVMVolumeGroup %s", internal.VGConfigurationAppliedType, lvg.Name))
		}

		err = updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, lvg, NonOperational, err.Error())
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLVGCreateFunc] unable to update the LVMVolumeGroup %s", lvg.Name))
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
				err = addConditionToLVG(ctx, cl, log, lvg, v1.ConditionFalse, internal.VGConfigurationAppliedType, "ThinPoolCreationFailed", fmt.Sprintf("unable to create thin-pool, err: %s", err.Error()))
				if err != nil {
					log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to add a condition %s to the LVMVolumeGroup %s", internal.VGConfigurationAppliedType, lvg.Name))
				}

				err = updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, lvg, NonOperational, err.Error())
				if err != nil {
					log.Error(err, fmt.Sprintf("[reconcileLVGCreateFunc] unable to update the LVMVolumeGroup %s", lvg.Name))
				}

				return true, err
			}
		}
		log.Debug(fmt.Sprintf("[reconcileLVGCreateFunc] successfully created thin-pools for the LVMVolumeGroup %s", lvg.Name))
	}

	err = addConditionToLVG(ctx, cl, log, lvg, v1.ConditionTrue, internal.VGConfigurationAppliedType, "Success", "all configuration has been applied")
	if err != nil {
		log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to add a condition %s to the LVMVolumeGroup %s", internal.VGConfigurationAppliedType, lvg.Name))
		return true, err
	}

	err = updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, lvg, Operational, "")
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLVGCreateFunc] unable to update the LVMVolumeGroup %s", lvg.Name))
		return true, err
	}

	return false, nil
}

func addLVGFinalizerIfNotExist(ctx context.Context, cl client.Client, lvg *v1alpha1.LvmVolumeGroup) (bool, error) {
	if slices.Contains(lvg.Finalizers, internal.SdsNodeConfiguratorFinalizer) {
		return false, nil
	}

	lvg.Finalizers = append(lvg.Finalizers, internal.SdsNodeConfiguratorFinalizer)
	lvg.Status.Health = Operational
	err := cl.Update(ctx, lvg)
	if err != nil {
		return false, err
	}

	return true, nil
}

func validateSpecBlockDevices(lvg *v1alpha1.LvmVolumeGroup, blockDevices map[string]v1alpha1.BlockDevice) (bool, string) {
	reason := strings.Builder{}

	targetNodeName := ""
	for _, bdName := range lvg.Spec.BlockDeviceNames {
		bd, exist := blockDevices[bdName]

		if !exist {
			reason.WriteString(fmt.Sprintf("the BlockDevice %s does not exist", bdName))
			continue
		}

		if targetNodeName == "" {
			targetNodeName = bd.Status.NodeName
		}

		if bd.Status.NodeName != targetNodeName {
			reason.WriteString(fmt.Sprintf("the BlockDevice %s has the node %s though the target node %s", bd.Name, bd.Status.NodeName, targetNodeName))
		}
	}

	if reason.Len() != 0 {
		return false, reason.String()
	}

	return true, ""
}

func checkIfLVGBelongsToNode(lvg *v1alpha1.LvmVolumeGroup, blockDevices map[string]v1alpha1.BlockDevice, nodeName string) bool {
	bd := blockDevices[lvg.Spec.BlockDeviceNames[0]]
	if bd.Status.NodeName != nodeName {
		return false
	}

	return true
}

func extractPathsFromBlockDevices(blockDevicesNames []string, blockDevices map[string]v1alpha1.BlockDevice) []string {
	paths := make([]string, 0, len(blockDevicesNames))

	for _, bdName := range blockDevicesNames {
		bd := blockDevices[bdName]
		paths = append(paths, bd.Status.Path)
	}

	return paths
}

func validateLVGForCreateFunc(log logger.Logger, lvg *v1alpha1.LvmVolumeGroup, blockDevices map[string]v1alpha1.BlockDevice) (bool, string) {
	reason := strings.Builder{}

	log.Debug(fmt.Sprintf("[validateLVGForCreateFunc] check if every selected BlockDevice of the LVMVolumeGroup %s is consumable", lvg.Name))
	// totalVGSize needs to count if there is enough space for requested thin-pools
	var totalVGSize int64
	for _, bdName := range lvg.Spec.BlockDeviceNames {
		bd := blockDevices[bdName]
		totalVGSize += bd.Status.Size.Value()

		if !bd.Status.Consumable {
			reason.WriteString(fmt.Sprintf("BlockDevice %s is not consumable.", bdName))
		}
	}

	if reason.Len() == 0 {
		log.Debug(fmt.Sprintf("[validateLVGForCreateFunc] all BlockDevices of the LVMVolumeGroup %s are consumable", lvg.Name))
	}

	if lvg.Spec.ThinPools != nil {
		log.Debug(fmt.Sprintf("[validateLVGForCreateFunc] the LVMVolumeGroup %s has thin-pools. Validate if VG size has enough space for the thin-pools", lvg.Name))
		log.Trace(fmt.Sprintf("[validateLVGForCreateFunc] the LVMVolumeGroup %s has thin-pools %v", lvg.Name, lvg.Spec.ThinPools))
		log.Trace(fmt.Sprintf("[validateLVGForCreateFunc] total LVMVolumeGroup %s size: %d", lvg.Name, totalVGSize))

		var totalThinPoolSize int64
		for _, tp := range lvg.Spec.ThinPools {
			if tp.Size.Value() == 0 {
				reason.WriteString(fmt.Sprintf("[validateLVGForCreateFunc] thin pool %s has zero size", tp.Name))
				continue
			}

			totalThinPoolSize += tp.Size.Value()
		}
		log.Trace(fmt.Sprintf("[validateLVGForCreateFunc] LVMVolumeGroup %s thin-pools requested space: %d", lvg.Name, totalThinPoolSize))

		resizeDelta, err := resource.ParseQuantity(internal.ResizeDelta)
		if err != nil {
			log.Error(err, fmt.Sprintf("[validateLVGForCreateFunc] unable to parse the resize delta %s", internal.ResizeDelta))
		}
		log.Debug(fmt.Sprintf("[validateLVGForCreateFunc] successfully parsed the resize delta: %s", resizeDelta.String()))

		if totalThinPoolSize+resizeDelta.Value() >= totalVGSize {
			reason.WriteString(fmt.Sprintf("required space for thin-pools %d is more than VG size %d", totalThinPoolSize, totalVGSize))
		}
	}

	if reason.Len() != 0 {
		return false, reason.String()
	}

	return true, ""
}

func validateLVGForUpdateFunc(log logger.Logger, lvg *v1alpha1.LvmVolumeGroup, blockDevices map[string]v1alpha1.BlockDevice, pvs []internal.PVData) (bool, string) {
	reason := strings.Builder{}

	log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] check if every new BlockDevice of the LVMVolumeGroup %s is comsumable", lvg.Name))
	pvMap := make(map[string]struct{}, len(pvs))
	for _, pv := range pvs {
		pvMap[pv.PVName] = struct{}{}
	}

	//TODO: add a check if BlockDevice size got less than PV size

	// Check if added BlockDevices are consumable
	// additionBlockDeviceSpace value is needed to count if VG will have enough space for thin-pools
	var additionBlockDeviceSpace int64
	for _, bdName := range lvg.Spec.BlockDeviceNames {
		bd := blockDevices[bdName]
		if _, used := pvMap[bd.Status.Path]; !used {
			log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] PV %s for BlockDevice %s of the LVMVolumeGroup %s is not created yet, check if the BlockDevice is consumable", bd.Status.Path, bdName, lvg.Name))

			if !blockDevices[bdName].Status.Consumable {
				reason.WriteString(fmt.Sprintf("BlockDevice %s is not consumable. ", bdName))
				continue
			}

			log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] BlockDevice %s is consumable", bdName))
			additionBlockDeviceSpace += bd.Status.Size.Value()
		}
	}

	if len(lvg.Status.ThinPools) > 0 {
		log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] the LVMVolumeGroup %s has thin-pools. Validate them", lvg.Name))
		usedThinPools := make(map[string]v1alpha1.StatusThinPool, len(lvg.Status.ThinPools))
		for _, tp := range lvg.Status.ThinPools {
			usedThinPools[tp.Name] = tp
		}

		// check if added thin-pools has valid requested size
		resizeDelta, err := resource.ParseQuantity(internal.ResizeDelta)
		if err != nil {
			log.Error(err, fmt.Sprintf("[validateLVGForCreateFunc] unable to parse the resize delta %s", internal.ResizeDelta))
		}
		log.Debug(fmt.Sprintf("[validateLVGForCreateFunc] successfully parsed the resize delta: %s", resizeDelta.String()))

		var newThinPoolsSize int64
		for _, specTp := range lvg.Spec.ThinPools {
			if specTp.Size.Value() == 0 {
				reason.WriteString(fmt.Sprintf("[validateLVGForCreateFunc] thin pool %s has zero size", specTp.Name))
				continue
			}

			if statusTp, used := usedThinPools[specTp.Name]; !used {
				log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] thin-pool %s of the LVMVolumeGroup %s is not used yet, check its requested size", specTp.Name, lvg.Name))
				newThinPoolsSize += specTp.Size.Value()
			} else {
				log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] thin-pool %s of the LVMVolumeGroup %s is already created, check its requested size", specTp.Name, lvg.Name))
				if specTp.Size.Value()+resizeDelta.Value() < statusTp.ActualSize.Value() {
					log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] the LVMVolumeGroup %s Spec.ThinPool %s size %s is less than Status one: %s", lvg.Name, specTp.Name, specTp.Size.String(), statusTp.ActualSize.String()))
					reason.WriteString(fmt.Sprintf("requested Spec.ThinPool %s size is less than actual one", specTp.Name))
					continue
				}

				thinPoolSizeDiff := specTp.Size.Value() - statusTp.ActualSize.Value()
				if thinPoolSizeDiff > resizeDelta.Value() {
					log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] the LVMVolumeGroup %s Spec.ThinPool %s size %s more than Status one: %s", lvg.Name, specTp.Name, specTp.Size.String(), statusTp.ActualSize.String()))
					newThinPoolsSize += thinPoolSizeDiff
				}
			}
		}

		totalFreeSpace := lvg.Status.VGSize.Value() - lvg.Status.AllocatedSize.Value() + additionBlockDeviceSpace
		log.Trace(fmt.Sprintf("[validateLVGForUpdateFunc] new LVMVolumeGroup %s thin-pools requested %d size, additional BlockDevices space %d, total: %d", lvg.Name, newThinPoolsSize, additionBlockDeviceSpace, totalFreeSpace))
		if newThinPoolsSize+resizeDelta.Value() > totalFreeSpace {
			reason.WriteString("added thin-pools requested sizes are more than allowed free space in VG")
		}
	}

	if reason.Len() != 0 {
		return false, reason.String()
	}

	return true, ""
}

func identifyLVGReconcileFunc(lvg *v1alpha1.LvmVolumeGroup, sdsCache *cache.Cache) reconcileType {
	if shouldReconcileLVGByCreateFunc(lvg, sdsCache) {
		return CreateReconcile
	}

	if shouldReconcileLVGByUpdateFunc(lvg, sdsCache) {
		return UpdateReconcile
	}

	if shouldReconcileLVGByDeleteFunc(lvg) {
		return DeleteReconcile
	}

	return "none"
}

func shouldReconcileLVGByCreateFunc(lvg *v1alpha1.LvmVolumeGroup, ch *cache.Cache) bool {
	_, exist := lvg.Annotations[delAnnotation]
	if lvg.DeletionTimestamp != nil || exist {
		return false
	}

	vgs, _ := ch.GetVGs()
	for _, vg := range vgs {
		if vg.VGName == lvg.Spec.ActualVGNameOnTheNode {
			return false
		}
	}

	return true
}

func shouldReconcileLVGByUpdateFunc(lvg *v1alpha1.LvmVolumeGroup, ch *cache.Cache) bool {
	_, exist := lvg.Annotations[delAnnotation]
	if lvg.DeletionTimestamp != nil || exist {
		return false
	}

	vgs, _ := ch.GetVGs()
	for _, vg := range vgs {
		if vg.VGName == lvg.Spec.ActualVGNameOnTheNode {
			return true
		}
	}

	return false
}

func shouldReconcileLVGByDeleteFunc(lvg *v1alpha1.LvmVolumeGroup) bool {
	_, exist := lvg.Annotations[delAnnotation]
	if lvg.DeletionTimestamp != nil || exist {
		return true
	}

	return false
}
