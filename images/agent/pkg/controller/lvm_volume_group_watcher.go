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
	cache := mgr.GetCache()

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
				log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to get BlockDevices. Retry in %d", cfg.BlockDeviceScanInterval))
				return reconcile.Result{
					RequeueAfter: cfg.VolumeGroupScanInterval * time.Second,
				}, nil
			}
			log.Debug("[RunLVMVolumeGroupController] successfully got BlockDevices")

			valid, reason := validateSpecBlockDevices(lvg, blockDevices)
			if !valid {
				err = updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, lvg, NonOperational, reason)
				if err != nil {
					log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to update the LVMVolumeGroup %s", lvg.Name))
				}
				return reconcile.Result{
					RequeueAfter: cfg.VolumeGroupScanInterval * time.Second,
				}, nil
			}
			log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] successfully validated BlockDevices of the LVMVolumeGroup %s", lvg.Name))

			shouldRequeue, err := runEventReconcile(ctx, cl, log, metrics, sdsCache, lvg, blockDevices)
			if err != nil {
				log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to reconcile the LVMVolumeGroup %s", lvg.Name))
			}

			if shouldRequeue {
				log.Warning(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] the LVMVolumeGroup %s event will be requeued in %ds", lvg.Name, cfg.BlockDeviceScanInterval))
				return reconcile.Result{
					RequeueAfter: cfg.VolumeGroupScanInterval * time.Second,
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

	createFunc := func(ctx context.Context, e event.CreateEvent, q workqueue.RateLimitingInterface) {
		log.Info(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] createFunc starts reconciliation for the LVMVolumeGroup, name: %s", e.Object.GetName()))

		lvg, ok := e.Object.(*v1alpha1.LvmVolumeGroup)
		if !ok {
			err = errors.New("unable to cast event object to a given type")
			log.Error(err, "[RunLVMVolumeGroupWatcherController] an error occurred while handling a create event")
			return
		}
		log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] successfully casted the LVMVolumeGroup %s", lvg.Name))

		blockDevices, err := GetAPIBlockDevices(ctx, cl, metrics)
		if err != nil {
			log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to get BlockDevices. Retry in %d", cfg.BlockDeviceScanInterval))
			requeue(q, lvg, cfg.BlockDeviceScanInterval)
			return
		}
		log.Debug("[RunLVMVolumeGroupWatcherController] successfully got BlockDevices")

		valid, reason := validateSpecBlockDevices(lvg, blockDevices)
		if !valid {
			err = updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, lvg, NonOperational, reason)
			if err != nil {
				log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to update the LVMVolumeGroup %s", lvg.Name))
			}
			requeue(q, lvg, cfg.BlockDeviceScanInterval)
		}
		log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] successfully validated BlockDevices of the LVMVolumeGroup %s", lvg.Name))

		belongs := checkIfLVGBelongsToNode(lvg, blockDevices, cfg.NodeName)
		if !belongs {
			log.Info(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] the LVMVolumeGroup %s does not belong to the node %s", lvg.Name, cfg.NodeName))
			return
		}

		log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] the LVMVolumeGroup %s belongs to the node %s. Starts to reconcile", lvg.Name, cfg.NodeName))
		shouldRequeue, err := runEventReconcile(ctx, cl, log, metrics, sdsCache, lvg, blockDevices)
		if err != nil {
			log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to reconcile the LVMVolumeGroup %s", lvg.Name))
		}

		if shouldRequeue {
			log.Warning(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] the LVMVolumeGroup %s event will be requeued in %ds", lvg.Name, cfg.BlockDeviceScanInterval))
			requeue(q, lvg, cfg.BlockDeviceScanInterval)
		}

		log.Info(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] createFunc ends reconciliation for the LVMVolumeGroup %s", lvg.Name))
	}

	updateFunc := func(ctx context.Context, e event.UpdateEvent, q workqueue.RateLimitingInterface) {
		log.Info(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] UpdateFunc starts the reconciliation for the LVMVolumeGroup %s", e.ObjectNew.GetName()))

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

		blockDevices, err := GetAPIBlockDevices(ctx, cl, metrics)
		if err != nil {
			log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to get BlockDevices. Retry in %d", cfg.BlockDeviceScanInterval))
			requeue(q, newLVG, cfg.BlockDeviceScanInterval)
			return
		}
		log.Debug("[RunLVMVolumeGroupWatcherController] successfully got BlockDevices")

		valid, reason := validateSpecBlockDevices(newLVG, blockDevices)
		if !valid {
			err = updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, newLVG, NonOperational, reason)
			if err != nil {
				log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to update the LVMVolumeGroup %s", newLVG.Name))
			}
			requeue(q, newLVG, cfg.BlockDeviceScanInterval)
		}
		log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] successfully validated BlockDevices of the LVMVolumeGroup %s", newLVG.Name))

		belongs := checkIfLVGBelongsToNode(newLVG, blockDevices, cfg.NodeName)
		if !belongs {
			log.Info(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] the LVMVolumeGroup %s does not belong to the node %s", newLVG.Name, cfg.NodeName))
			return
		}
		log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] the LVMVolumeGroup %s belongs to the node %s. Starts to reconcile", newLVG.Name, cfg.NodeName))

		if reflect.DeepEqual(oldLVG.Spec, newLVG.Spec) && reflect.DeepEqual(oldLVG.Status.Nodes, newLVG.Status.Nodes) {
			log.Info(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] the LVMVolumeGroup %s should not be reconciled", newLVG.Name))
			return
		}

		shouldRequeue, err := runEventReconcile(ctx, cl, log, metrics, sdsCache, newLVG, blockDevices)
		if err != nil {
			log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to reconcile the LVMVolumeGroup %s", newLVG.Name))
		}

		if shouldRequeue {
			log.Warning(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] the LVMVolumeGroup %s event will be requeued in %ds", newLVG.Name, cfg.BlockDeviceScanInterval))
			requeue(q, newLVG, cfg.BlockDeviceScanInterval)
		}

		log.Info(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] updateFunc ends reconciliation for the LVMVolumeGroup %s", newLVG.Name))
	}

	err = c.Watch(source.Kind(cache, &v1alpha1.LvmVolumeGroup{}), handler.Funcs{
		CreateFunc: createFunc,
		UpdateFunc: updateFunc,
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

	log.Info(fmt.Sprintf("[reconcileLVGDeleteFunc] successfully deleted VG %s of the LVMVolumeGroup %s", lvg.Spec.ActualVGNameOnTheNode, lvg.Name))
	return false, nil
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
		return true, err
	}
	log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] VG %s found for the LVMVolumeGroup %s", vg.VGName, lvg.Name))

	log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] tries to check and update VG %s tag %s", lvg.Spec.ActualVGNameOnTheNode, internal.LVMTags[0]))
	updated, err := UpdateVGTagIfNeeded(log, metrics, vg, lvg.Name)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to update VG %s tag of the LVMVolumeGroup %s", vg.VGName, lvg.Name))
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
			err = updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, lvg, NonOperational, err.Error())
			if err != nil {
				log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to update the LVMVolumeGroup %s", lvg.Name))
			}
			return true, err
		}
		log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] successfully reconciled thin-pools operation of the LVMVolumeGroup %s", lvg.Name))
	}

	log.Info(fmt.Sprintf("[reconcileLVGUpdateFunc] successfully reconciled the LVMVolumeGroup %s", lvg.Name))

	err = updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, lvg, Operational, "")
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLVGCreateFunc] unable to update the LVMVolumeGroup %s", lvg.Name))
		return true, err
	}

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

	log.Debug(fmt.Sprintf("[reconcileLVGCreateFunc] tries to validate the LVMVolumeGroup %s", lvg.Name))
	valid, reason := validateLVGForCreateFunc(log, lvg, blockDevices)
	if !valid {
		log.Warning(fmt.Sprintf("[reconcileLVGCreateFunc] validation fails for the LVMVolumeGroup %s", lvg.Name))
		err := updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, lvg, NonOperational, reason)
		if err != nil {
			log.Error(err, fmt.Sprintf("[reconcileLVGCreateFunc] unable to update the LVMVolumeGroup %s", lvg.Name))
		}

		return true, err
	}
	log.Debug(fmt.Sprintf("[reconcileLVGCreateFunc] successfully validated the LVMVolumeGroup %s", lvg.Name))

	log.Debug(fmt.Sprintf("[reconcileLVGCreateFunc] tries to create VG for the LVMVolumeGroup %s", lvg.Name))
	err := CreateVGComplex(metrics, log, lvg, blockDevices)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLVGCreateFunc] unable to create VG for the LVMVolumeGroup %s", lvg.Name))
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
				err = updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, lvg, NonOperational, err.Error())
				if err != nil {
					log.Error(err, fmt.Sprintf("[reconcileLVGCreateFunc] unable to update the LVMVolumeGroup %s", lvg.Name))
				}
				return true, err
			}
		}
		log.Debug(fmt.Sprintf("[reconcileLVGCreateFunc] successfully created thin-pools for the LVMVolumeGroup %s", lvg.Name))
	}

	err = updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, lvg, Operational, "")
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLVGCreateFunc] unable to update the LVMVolumeGroup %s", lvg.Name))
		return true, err
	}

	return false, nil
}

func requeue(q workqueue.RateLimitingInterface, lvg *v1alpha1.LvmVolumeGroup, interval time.Duration) {
	q.AddAfter(reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: lvg.Namespace,
			Name:      lvg.Name,
		},
	}, interval*time.Second)
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

	// Check if added BlockDevices are consumable
	// additionBlockDeviceSpace value is needed to count if VG will have enough space for new thin-pools
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
			bd := blockDevices[bdName]
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
			if statusTp, used := usedThinPools[specTp.Name]; !used {
				log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] thin-pool %s of the LVMVolumeGroup %s is not used yet, check its requested size", specTp.Name, lvg.Name))
				newThinPoolsSize += specTp.Size.Value()
			} else {
				log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] thin-pool %s of the LVMVolumeGroup %s is already used, check its requested size", specTp.Name, lvg.Name))
				if specTp.Size.Value() == 0 ||
					specTp.Size.Value()+resizeDelta.Value() < statusTp.ActualSize.Value() {
					log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] the LVMVolumeGroup %s Spec.ThinPool %s size %s is less than Status one: %s", lvg.Name, specTp.Name, specTp.Size.String(), statusTp.ActualSize.String()))
					reason.WriteString(fmt.Sprintf("requested Spec.ThinPool %s size is less than actual one (or equals zero)", specTp.Name))
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

//func ReconcileLVMVG(
//	ctx context.Context,
//	metrics monitoring.Metrics,
//	objectName, objectNameSpace, nodeName string,
//	log logger.Logger,
//	cl client.Client,
//) (bool, error) {
//	log.Info("[ReconcileLVMVG] reconcile loop start")
//	lvg, err := getLVMVolumeGroup(ctx, cl, metrics, objectNameSpace, objectName)
//	if err != nil {
//		log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error getLVMVolumeGroup, objectname: %s", objectName))
//		return false, err
//	}
//	if lvg == nil {
//		err = errors.New("nil pointer detected")
//		log.Error(err, fmt.Sprintf("[ReconcileLVMVG] requested LVMVolumeGroup %s is nil", objectName))
//		return true, err
//	}
//
//	isOwnedByNode, status, err := CheckLVMVGNodeOwnership(ctx, cl, metrics, lvg, objectNameSpace, nodeName)
//	if err != nil {
//		log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error CheckLVMVGNodeOwnership, LVMVolumeGroup name: %s", lvg.Name))
//		if status.Health == NonOperational {
//			health := status.Health
//			message := status.Message
//			log.Error(err, fmt.Sprintf("[ReconcileLVMVG] ValidateLVMGroup, LVMVolumeGroup name: %s, health: %s, phase: %s, message: %s", lvg.Name, health, status.Phase, message))
//			err = updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, lvg.Name, lvg.Namespace, message, health)
//			if err != nil {
//				log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error update LVMVolumeGroup %s", lvg.Name))
//				return true, err
//			}
//		}
//		return true, err
//	}
//
//	if !isOwnedByNode {
//		log.Debug(fmt.Sprintf("[ReconcileLVMVG] LVMVolumeGroup %s is not owned by the current node, skip it", lvg.Name))
//		return false, nil
//	}
//
//	var totalVGSize int64
//	for _, bdName := range lvg.Spec.BlockDeviceNames {
//		bd, err := getBlockDevice(ctx, cl, metrics, "", bdName)
//		if err != nil {
//			log.Error(err, fmt.Sprintf("[ReconcileLVMVG] unable to get BlockDevice %s for LVMVolumeGroup %s", bdName, lvg.Name))
//			return true, err
//		}
//
//		totalVGSize += bd.Status.Size.Value()
//	}
//
//	var totalThinPoolSize int64
//	for _, tp := range lvg.Spec.ThinPools {
//		totalThinPoolSize += tp.Size.Value()
//	}
//
//	resizeDelta, _ := utils.QuantityToBytes(internal.ResizeDelta)
//
//	if totalThinPoolSize+resizeDelta >= totalVGSize {
//		err := errors.New("required space for thin pools is more than VG size")
//		log.Error(err, fmt.Sprintf("[ReconcileLVMVG] validation fails for LVMVolumeGroup %s", lvg.Name))
//
//		err = updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, lvg.Name, "", err.Error(), NonOperational)
//		if err != nil {
//			log.Error(err, fmt.Sprintf("[ReconcileLVMVG] unable to update LVMVolumeGroup %s", lvg.Name))
//			return true, err
//		}
//
//		return false, err
//	}
//
//	log.Info(fmt.Sprintf("[ReconcileLVMVG] LVMVolumeGroup %s validation passed", lvg.Name))
//
//	_, exist := lvg.Annotations[delAnnotation]
//	log.Debug(fmt.Sprintf("[ReconcileLVMVG] LVMVolumeGroup %s contains contains delete annotation: %t", lvg.Name, exist))
//	if exist {
//		// lock
//		log.Info(fmt.Sprintf("[ReconcileLVMVG] create event %s for LVMVolumeGroup %s", EventActionDeleting, lvg.Name))
//		err = CreateEventLVMVolumeGroup(ctx, cl, metrics, EventReasonDeleting, EventActionDeleting, nodeName, lvg)
//		if err != nil {
//			log.Error(err, "[ReconcileLVMVG] error CreateEventLVMVolumeGroup")
//			return true, err
//		}
//
//		err = DeleteVGIfExist(lvg.Spec.ActualVGNameOnTheNode, log, metrics)
//		if err != nil {
//			log.Error(err, fmt.Sprintf("[ReconcileLVMVG] delete VG %s of LVMVolumeGroup %s", lvg.Spec.ActualVGNameOnTheNode, lvg.Name))
//			return true, err
//		}
//
//		log.Info(fmt.Sprintf("[ReconcileLVMVG] VG %s deleted of LVMVolumeGroup %s", lvg.Spec.ActualVGNameOnTheNode, lvg.Name))
//		return false, nil
//	}
//
//	log.Info(fmt.Sprintf("[ReconcileLVMVG] start reconciliation VG process for LVMVolumeGroup %s", lvg.Name))
//	log.Info(fmt.Sprintf("[ReconcileLVMVG] create event %s for LVMVolumeGroup %s", EventActionProvisioning, lvg.Name))
//	err = CreateEventLVMVolumeGroup(ctx, cl, metrics, EventReasonProvisioning, EventActionProvisioning, nodeName, lvg)
//	if err != nil {
//		log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error CreateEvent for LVMVolumeGroup %s", lvg.Name))
//		return true, err
//	}
//	log.Info(fmt.Sprintf(`[ReconcileLVMVG] event was created for LVMVolumeGroup %s`, lvg.Name))
//
//	isVgExist, vg, err := GetVGFromNode(lvg.Spec.ActualVGNameOnTheNode, log, metrics)
//	if err != nil {
//		log.Error(err, fmt.Sprintf("[ReconcileLVMVG] unable to get VG %s from the node for LVMVolumeGroup %s", lvg.Spec.ActualVGNameOnTheNode, lvg.Name))
//		return true, err
//	}
//
//	log.Trace(fmt.Sprintf("VG %s of LVMVolumeGroup %s does exist: %t", lvg.Spec.ActualVGNameOnTheNode, lvg.Name, isVgExist))
//	if isVgExist {
//		log.Debug(fmt.Sprintf("[ReconcileLVMVG] start UpdateVGTagIfNeeded for LVMVolumeGroup %s", lvg.Name))
//		updated, err := UpdateVGTagIfNeeded(log, metrics, vg, lvg.Name)
//		if err != nil {
//			log.Error(err, fmt.Sprintf("[ReconcileLVMVG] unable to update VG tags on VG, name: %s", lvg.Spec.ActualVGNameOnTheNode))
//			return true, err
//		}
//
//		if updated {
//			log.Info(fmt.Sprintf("[ReconcileLVMVG] tag storage.deckhouse.io/lvmVolumeGroupName was updated on VG, name: %s", lvg.Spec.ActualVGNameOnTheNode))
//		} else {
//			log.Debug(fmt.Sprintf("[ReconcileLVMVG] no need to update tag storage.deckhouse.io/lvmVolumeGroupName on VG, name: %s", lvg.Spec.ActualVGNameOnTheNode))
//		}
//
//		log.Info("[ReconcileLVMVG] validation and choosing the type of operation")
//		extendPVs, shrinkPVs, err := ValidateOperationTypeLVMGroup(ctx, cl, metrics, lvg, log)
//		if err != nil {
//			log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error ValidateOperationTypeLVMGroup, name: %s", lvg.Name))
//			return true, err
//		}
//
//		// if err == nil && extendPVs == nil && shrinkPVs == nil {
//		// 	log.Warning(fmt.Sprintf("[ReconcileLVMVG] ValidateOperationTypeLVMGroup FAIL for resource %s", lvg.Name))
//		// 	//todo retry and send message
//		// }
//
//		log.Debug("----- extendPVs list -----")
//		for _, pvExt := range extendPVs {
//			log.Debug(pvExt)
//		}
//		log.Debug("----- extendPVs list -----")
//		log.Debug("----- shrinkPVs list -----")
//		for _, pvShr := range shrinkPVs {
//			log.Debug(pvShr)
//		}
//		log.Debug("----- shrinkPVs list -----")
//
//		if len(extendPVs) != 0 {
//			log.Info(fmt.Sprintf("[ReconcileLVMVG] CREATE event: %s", EventActionExtending))
//			err = CreateEventLVMVolumeGroup(ctx, cl, metrics, EventReasonExtending, EventActionExtending, nodeName, lvg)
//			if err != nil {
//				log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error CreateEventLVMVolumeGroup, name: %s", lvg.Name))
//				return true, err
//			}
//			err := ExtendVGComplex(metrics, extendPVs, lvg.Spec.ActualVGNameOnTheNode, log)
//			if err != nil {
//				log.Error(err, fmt.Sprintf("[ReconcileLVMVG] unable to ExtendVGComplex for resource, name: %s", lvg.Name))
//				return true, err
//			}
//		}
//
//		// Check Resize
//		for _, n := range lvg.Status.Nodes {
//			for _, d := range n.Devices {
//
//				if d.DevSize.Value() == 0 {
//					log.Error(errors.New("check resize d.DevSize = "), d.DevSize.String())
//					log.Error(errors.New(fmt.Sprintf("check resize for device, name: %s", d.BlockDevice)),
//						fmt.Sprintf("[ReconcileLVMVG] block device's size equals zero, block device name: %s, size: %s", d.BlockDevice, d.DevSize.String()))
//					return false, err
//				}
//
//				log.Debug(fmt.Sprintf("[ReconcileLVMVG] check Resize d.PVSize = %s", d.PVSize.String()))
//
//				if d.PVSize.Value() == 0 {
//					log.Warning(fmt.Sprintf("[ReconcileLVMVG] check dev PV size device.PVSize = %s", d.PVSize.String()))
//					return false, nil
//				}
//
//				delta, _ := utils.QuantityToBytes(internal.ResizeDelta)
//
//				log.Debug("---------- Reconcile ---------------")
//				log.Debug(fmt.Sprintf("[ReconcileLVMVG] PVSize = %s", d.PVSize.String()))
//				log.Debug(fmt.Sprintf("[ReconcileLVMVG] DevSize = %s %s", d.DevSize.String(), d.PVSize.String()))
//				log.Debug(fmt.Sprintf("[ReconcileLVMVG] Resize flag = %t", d.DevSize.Value()-d.PVSize.Value() > delta))
//				log.Debug("---------- Reconcile ---------------")
//
//				if d.DevSize.Value() < d.PVSize.Value() {
//					return false, nil
//				}
//
//				log.Info(fmt.Sprintf("[ReconcileLVMVG] devSize changed, block device name: %s", d.BlockDevice))
//				log.Info(fmt.Sprintf("[ReconcileLVMVG] devSize %s ", d.DevSize.String()))
//				log.Info(fmt.Sprintf("[ReconcileLVMVG] pvSize %s ", d.DevSize.String()))
//
//				if d.DevSize.Value()-d.PVSize.Value() > delta {
//					log.Info(fmt.Sprintf("[ReconcileLVMVG] create event: %s", EventActionResizing))
//					err = CreateEventLVMVolumeGroup(ctx, cl, metrics, EventReasonResizing, EventActionResizing, nodeName, lvg)
//					if err != nil {
//						log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error CreateEventLVMVolumeGroup, resource name: %s", lvg.Name))
//					}
//					command, err := utils.ResizePV(d.Path)
//					log.Debug(fmt.Sprintf("[ReconcileLVMVG] exec command: %s", command))
//					if err != nil {
//						log.Error(errors.New("check size error"), fmt.Sprintf("[ReconcileLVMVG] devSize <= pvSize, block device: %s", d.BlockDevice))
//						err = updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, lvg.Name, lvg.Namespace, "devSize <= pvSize", NonOperational)
//						if err != nil {
//							log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error update LVMVolumeGroup %s", lvg.Name))
//						}
//						return true, err
//					}
//				}
//			}
//		}
//
//		if lvg.Spec.ThinPools != nil {
//			log.Info(fmt.Sprintf("[ReconcileLVMVG] reconcile thin pools for LVMVolumeGroup %s", lvg.Name))
//
//			statusThinPoolMap := make(map[string]v1alpha1.StatusThinPool)
//			for _, statusThinPool := range lvg.Status.ThinPools {
//				statusThinPoolMap[statusThinPool.Name] = statusThinPool
//			}
//
//			for _, specThinPool := range lvg.Spec.ThinPools {
//				log.Debug(fmt.Sprintf("[ReconcileLVMVG] Start reconcile thin pool %s of LVMVolumeGroup %s", specThinPool.Name, lvg.Name))
//				log.Debug(fmt.Sprintf("[ReconcileLVMVG] LVMVolumeGroup %s thin pool %s size: %d", lvg.Name, specThinPool.Name, specThinPool.Size.Value()))
//
//				statusThinPool, ok := statusThinPoolMap[specThinPool.Name]
//				if !ok {
//					log.Info(fmt.Sprintf("[ReconcileLVMVG] create event: %s", EventActionCreating))
//					err = CreateEventLVMVolumeGroup(ctx, cl, metrics, EventReasonCreating, EventActionCreating, nodeName, lvg)
//					if err != nil {
//						log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error CreateEventLVMVolumeGroup, resource name: %s", lvg.Name))
//					}
//					start := time.Now()
//					command, err := utils.CreateThinPool(specThinPool, lvg.Spec.ActualVGNameOnTheNode)
//					metrics.UtilsCommandsDuration(LVMVolumeGroupWatcherCtrlName, "lvcreate").Observe(metrics.GetEstimatedTimeInSeconds(start))
//					metrics.UtilsCommandsExecutionCount(LVMVolumeGroupWatcherCtrlName, "lvcreate").Inc()
//					log.Debug(command)
//					if err != nil {
//						log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error CreateThinPool, thin pool: %s", specThinPool.Name))
//						metrics.UtilsCommandsErrorsCount(LVMVolumeGroupWatcherCtrlName, "lvcreate").Inc()
//						if err = updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, lvg.Name, lvg.Namespace, err.Error(), NonOperational); err != nil {
//							log.Error(err, fmt.Sprintf("[ReconcileLVMVG] unable to update LVMVolumeGroupStatus, resource name: %s", lvg.Name))
//						}
//						return true, err
//					}
//					continue
//				}
//
//				resizeDelta, err := resource.ParseQuantity(internal.ResizeDelta)
//				if err != nil {
//					log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error ParseQuantity, resource name: %s", lvg.Name))
//					return false, err
//				}
//
//				if utils.AreSizesEqualWithinDelta(specThinPool.Size, statusThinPool.ActualSize, resizeDelta) {
//					log.Debug(fmt.Sprintf("[ReconcileLVMVG] No need to resize thin pool: %s; specThinPool.Size = %s, statusThinPool.ActualSize = %s", specThinPool.Name, specThinPool.Size.String(), statusThinPool.ActualSize.String()))
//					continue
//				}
//
//				shouldRequeue, err := ResizeThinPool(ctx, cl, log, metrics, lvg, specThinPool, statusThinPool, nodeName, resizeDelta)
//				if err != nil {
//					log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error ResizeThinPool, thin pool: %s", specThinPool.Name))
//					return shouldRequeue, err
//				}
//
//				log.Debug(fmt.Sprintf("[ReconcileLVMVG] END reconcile thin pool: %s", specThinPool.Name))
//			}
//
//		}
//
//	} else {
//		log.Info(fmt.Sprintf("[ReconcileLVMVG] create event %s for LVMVolumeGroup %s", EventActionCreating, lvg.Name))
//		err = CreateEventLVMVolumeGroup(ctx, cl, metrics, EventReasonCreating, EventActionCreating, nodeName, lvg)
//		if err != nil {
//			log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error CreateEvent for LVMVolumeGroup %s", lvg.Name))
//		}
//
//		log.Debug(fmt.Sprintf("[ReconcileLVMVG] Start CreateVGComplex function for LVMVolumeGroup %s", lvg.Name))
//		err = CreateVGComplex(ctx, cl, metrics, lvg, log)
//		if err != nil {
//			log.Error(err, fmt.Sprintf("[ReconcileLVMVG] unable to CreateVGComplex for LVMVolumeGroup: %s", lvg.Name))
//			if err = updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, lvg.Name, lvg.Namespace, err.Error(), NonOperational); err != nil {
//				log.Error(err, fmt.Sprintf("[ReconcileLVMVG] unable to update Status for LVMVolumeGroup: %s", lvg.Name))
//			}
//
//			return true, err
//		}
//
//		if len(lvg.Spec.ThinPools) != 0 {
//			for _, thinPool := range lvg.Spec.ThinPools {
//				start := time.Now()
//				command, err := utils.CreateThinPool(thinPool, lvg.Spec.ActualVGNameOnTheNode)
//				metrics.UtilsCommandsDuration(LVMVolumeGroupWatcherCtrlName, "lvcreate").Observe(metrics.GetEstimatedTimeInSeconds(start))
//				metrics.UtilsCommandsExecutionCount(LVMVolumeGroupWatcherCtrlName, "lvcreate").Inc()
//				log.Debug(command)
//				if err != nil {
//					metrics.UtilsCommandsErrorsCount(LVMVolumeGroupWatcherCtrlName, "lvcreate").Inc()
//					log.Error(err, fmt.Sprintf("[ReconcileLVMVG] unable to create a thin pool %s for LVMVolumeGroup %s", thinPool.Name, lvg.Name))
//					if err = updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, lvg.Name, lvg.Namespace, err.Error(), NonOperational); err != nil {
//						log.Error(err, fmt.Sprintf("[ReconcileLVMVG] unable to update Status for LVMVolumeGroup %s", lvg.Name))
//					}
//					return true, err
//				}
//			}
//		}
//	}
//
//	log.Info("[ReconcileLVMVG] reconcile loop end")
//	err = updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, lvg.Name, lvg.Namespace, "", Operational)
//	if err != nil {
//		log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error update LVMVolumeGroup %s", lvg.Name))
//		return true, err
//	}
//
//	log.Info(fmt.Sprintf("[ReconcileLVMVG] create event: %s", EventActionReady))
//	err = CreateEventLVMVolumeGroup(ctx, cl, metrics, EventReasonReady, EventActionReady, nodeName, lvg)
//	if err != nil {
//		log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error CreateEventLVMVolumeGroup, resource name: %s", lvg.Name))
//	}
//
//	return false, nil
//}
