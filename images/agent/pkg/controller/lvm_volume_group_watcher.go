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
	"sds-node-configurator/pkg/logger"
	"sds-node-configurator/pkg/monitoring"
	"sds-node-configurator/pkg/utils"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sds-node-configurator/config"
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
) (controller.Controller, error) {
	cl := mgr.GetClient()
	cache := mgr.GetCache()

	c, err := controller.New(LVMVolumeGroupWatcherCtrlName, mgr, controller.Options{
		Reconciler: reconcile.Func(func(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
			log.Info(fmt.Sprintf(`Reconcile of RunLVMVolumeGroupWatcherController on request, name: "%s" starts`, request.Name))

			shouldRequeue, err := ReconcileLVMVG(ctx, metrics, request.Name, request.Namespace, cfg.NodeName, log, cl)
			if shouldRequeue {
				log.Error(err, fmt.Sprintf("error in ReconcileEvent. Add to retry after %d seconds.", cfg.VolumeGroupScanInterval))
				log.Warning(fmt.Sprintf(`Added request, namespace: "%s" name: "%s", to requeue`, request.Namespace, request.Name))
				return reconcile.Result{
					RequeueAfter: cfg.VolumeGroupScanInterval * time.Second,
				}, nil
			}
			log.Info(fmt.Sprintf(`Reconcile of RunLVMVolumeGroupWatcherController on request, name: "%s" ends`, request.Name))

			return reconcile.Result{}, nil
		}),
	})

	if err != nil {
		log.Error(err, "[RunLVMVolumeGroupWatcherController] Unable to create controller RunLVMVolumeGroupWatcherController")
		return nil, err
	}

	createFunc := func(ctx context.Context, e event.CreateEvent, q workqueue.RateLimitingInterface) {
		log.Info(fmt.Sprintf("[RunLVMVolumeGroupController] event create LVMVolumeGroup, name: %s", e.Object.GetName()))

		request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: e.Object.GetNamespace(), Name: e.Object.GetName()}}
		shouldRequeue, err := ReconcileLVMVG(ctx, metrics, e.Object.GetName(), e.Object.GetNamespace(), cfg.NodeName, log, cl)
		if shouldRequeue {
			log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] An error has occured in ReconcileLVMVG. Adds to retry after %d seconds.", cfg.VolumeGroupScanInterval))
			q.AddAfter(request, cfg.VolumeGroupScanInterval*time.Second)
			log.Warning(fmt.Sprintf(`Added request, namespace: "%s" name: "%s", to requeue`, request.Namespace, request.Name))
		}
	}
	updateFunc := func(ctx context.Context, e event.UpdateEvent, q workqueue.RateLimitingInterface) {
		log.Info(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] update LVMVolumeGroupn, name: %s", e.ObjectNew.GetName()))

		newLVG, ok := e.ObjectNew.(*v1alpha1.LvmVolumeGroup)
		if !ok {
			log.Error(err, "[RunLVMVolumeGroupWatcherController] error get  ObjectNew LinstorStoragePool")
		}

		oldLVG, ok := e.ObjectOld.(*v1alpha1.LvmVolumeGroup)
		if !ok {
			log.Error(err, "[RunLVMVolumeGroupWatcherController] error get  ObjectOld LinstorStoragePool")
		}

		if !reflect.DeepEqual(oldLVG.Annotations, newLVG.Annotations) {
			log.Info("[RunLVMVolumeGroupWatcherController] annotations update")

			request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: e.ObjectNew.GetNamespace(), Name: e.ObjectNew.GetName()}}
			shouldRequeue, err := ReconcileLVMVG(ctx, metrics, e.ObjectNew.GetName(), e.ObjectNew.GetNamespace(), cfg.NodeName, log, cl)
			if shouldRequeue {
				log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] An error has occured in ReconcileLVMVG. Adds to retry after %d seconds.", cfg.VolumeGroupScanInterval))
				q.AddAfter(request, cfg.VolumeGroupScanInterval*time.Second)
				log.Warning(fmt.Sprintf(`Added request, namespace: "%s" name: "%s", to requeue`, request.Namespace, request.Name))
			}
			return
		}

		if !reflect.DeepEqual(oldLVG.Spec, newLVG.Spec) {
			log.Info("[RunLVMVolumeGroupWatcherController] lvg spec changed")

			request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: e.ObjectNew.GetNamespace(), Name: e.ObjectNew.GetName()}}
			shouldRequeue, err := ReconcileLVMVG(ctx, metrics, e.ObjectNew.GetName(), e.ObjectNew.GetNamespace(), cfg.NodeName, log, cl)
			if shouldRequeue {
				log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] An error has occured in ReconcileLVMVG. Adds to retry after %d seconds.", cfg.VolumeGroupScanInterval))
				q.AddAfter(request, cfg.VolumeGroupScanInterval*time.Second)
				log.Warning(fmt.Sprintf(`Added request, namespace: "%s" name: "%s", to requeue`, request.Namespace, request.Name))
			}
			return
		}

		if !reflect.DeepEqual(oldLVG.Status.Nodes, newLVG.Status.Nodes) {
			request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: e.ObjectNew.GetNamespace(), Name: e.ObjectNew.GetName()}}
			shouldRequeue, err := ReconcileLVMVG(ctx, metrics, e.ObjectNew.GetName(), e.ObjectNew.GetNamespace(), cfg.NodeName, log, cl)
			if shouldRequeue {
				log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] An error has occured in ReconcileLVMVG. Adds to retry after %d seconds.", cfg.VolumeGroupScanInterval))
				q.AddAfter(request, cfg.VolumeGroupScanInterval*time.Second)
				log.Warning(fmt.Sprintf(`Added request, namespace: "%s" name: "%s", to requeue`, request.Namespace, request.Name))
			}
		} else {
			log.Info("[RunLVMVolumeGroupWatcherController] lvg check dev size")
			for _, node := range newLVG.Status.Nodes {
				for _, device := range node.Devices {

					if device.DevSize.Value() == 0 {
						log.Warning(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] check dev size device.DevSize = %s", device.DevSize.String()))
						return
					}

					log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] update spec check resize device.PVSize = %s", device.PVSize))
					dPVSizeTmp := resource.MustParse(device.PVSize)

					if dPVSizeTmp.Value() == 0 {
						log.Warning(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] check dev PV size device.PVSize = %s", device.PVSize))
						return
					}

					delta, _ := utils.QuantityToBytes(internal.ResizeDelta)

					log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] resize flag = %t", device.DevSize.Value()-dPVSizeTmp.Value() > delta))

					if device.DevSize.Value()-dPVSizeTmp.Value() > delta {
						log.Info("[RunLVMVolumeGroupWatcherController] lvg status device and PV changed")
						request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: e.ObjectNew.GetNamespace(), Name: e.ObjectNew.GetName()}}
						shouldRequeue, err := ReconcileLVMVG(ctx, metrics, e.ObjectNew.GetName(), e.ObjectNew.GetNamespace(), cfg.NodeName, log, cl)
						if shouldRequeue {
							log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] An error has occured in ReconcileLVMVG. Adds to retry after %d seconds.", cfg.VolumeGroupScanInterval))
							q.AddAfter(request, cfg.VolumeGroupScanInterval*time.Second)
							log.Warning(fmt.Sprintf(`Added request, namespace: "%s" name: "%s", to requeue`, request.Namespace, request.Name))
						}
					}
				}
			}
		}
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

func ReconcileLVMVG(
	ctx context.Context,
	metrics monitoring.Metrics,
	objectName, objectNameSpace, nodeName string,
	log logger.Logger,
	cl client.Client,
) (bool, error) {
	log.Info("[ReconcileLVMVG] reconcile loop start")
	group, err := getLVMVolumeGroup(ctx, cl, metrics, objectNameSpace, objectName)
	if err != nil {
		log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error getLVMVolumeGroup, objectname: %s", objectName))
		return true, err
	}
	validation, status, err := ValidateLVMGroup(ctx, cl, metrics, group, objectNameSpace, nodeName)

	if group == nil {
		err = errors.New("nil pointer detected")
		log.Error(err, "[ReconcileLVMVG] requested LVMVG group in nil")
		return true, err
	}

	if status.Health == NonOperational {
		health := status.Health
		var message string
		if err != nil {
			message = err.Error()
		}

		log.Error(err, fmt.Sprintf("[ReconcileLVMVG] ValidateLVMGroup, resource name: %s, message: %s", group.Name, message))
		err = updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, group.Name, group.Namespace, message, health)
		if err != nil {
			log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error update LVMVolumeGroup %s", group.Name))
			return true, err
		}
	}

	if err != nil {
		log.Error(err, fmt.Sprintf("[ReconcileLVMVG] validationLVMGroup failed, resource name: %s", group.Name))
		return false, err
	}

	if validation == false {
		err = errors.New("resource validation failed")
		log.Error(err, fmt.Sprintf("[ReconcileLVMVG] validation failed for resource, name: %s", group.Name))
		log.Error(err, fmt.Sprintf("[ReconcileLVMVG] status.Message = %s", status.Message))
		return false, err
	}

	log.Info("[ReconcileLVMVG] validation passed")

	annotationMark := 0
	for k := range group.Annotations {
		if strings.Contains(k, delAnnotation) {
			annotationMark++
		}
	}

	if annotationMark != 0 {
		// lock
		log.Info(fmt.Sprintf("[ReconcileLVMVG] create event: %s", EventActionDeleting))
		err = CreateEventLVMVolumeGroup(ctx, cl, metrics, EventReasonDeleting, EventActionDeleting, nodeName, group)
		if err != nil {
			log.Error(err, "[ReconcileLVMVG] error CreateEventLVMVolumeGroup")
			return true, err
		}

		err := DeleteVG(group.Spec.ActualVGNameOnTheNode, log, metrics)
		if err != nil {
			log.Error(err, fmt.Sprintf("[ReconcileLVMVG] delete VG, name: %s", group.Spec.ActualVGNameOnTheNode))
			return true, err
		}

		log.Info(fmt.Sprintf("[ReconcileLVMVG] VG deleted, name: %s", group.Spec.ActualVGNameOnTheNode))
		return false, nil
	}

	log.Info("[ReconcileLVMVG] start reconciliation VG process")
	log.Info(fmt.Sprintf("[ReconcileLVMVG] create event: %s", EventActionProvisioning))
	err = CreateEventLVMVolumeGroup(ctx, cl, metrics, EventReasonProvisioning, EventActionProvisioning, nodeName, group)
	if err != nil {
		log.Error(err, "[ReconcileLVMVG] error CreateEventLVMVolumeGroup")
		return true, err
	}
	log.Info(fmt.Sprintf(`[ReconcileLVMVG] event was created for resource, name: %s`, group.Name))

	existVG, err := ExistVG(group.Spec.ActualVGNameOnTheNode, log, metrics)
	if err != nil {
		log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error ExistVG, name: %s", group.Spec.ActualVGNameOnTheNode))
		return true, err
	}
	if existVG {
		log.Debug("[ReconcileLVMVG] tries to update ")
		updated, err := UpdateLVMVolumeGroupTagsName(log, metrics, group)
		if err != nil {
			log.Error(err, fmt.Sprintf("[ReconcileLVMVG] unable to update VG tags on VG, name: %s", group.Spec.ActualVGNameOnTheNode))
			return true, err
		}

		if updated {
			log.Info(fmt.Sprintf("[ReconcileLVMVG] tag storage.deckhouse.io/lvmVolumeGroupName was updated on VG, name: %s", group.Spec.ActualVGNameOnTheNode))
		} else {
			log.Debug(fmt.Sprintf("[ReconcileLVMVG] no need to update tag storage.deckhouse.io/lvmVolumeGroupName on VG, name: %s", group.Spec.ActualVGNameOnTheNode))
		}

		log.Info("[ReconcileLVMVG] validation and choosing the type of operation")
		extendPVs, shrinkPVs, err := ValidateTypeLVMGroup(ctx, cl, metrics, group, log)
		if err != nil {
			log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error ValidateTypeLVMGroup, name: %s", group.Name))
			return true, err
		}

		if err == nil && extendPVs == nil && shrinkPVs == nil {
			log.Warning("[ReconcileLVMVG] ValidateTypeLVMGroup FAIL")
			//todo retry and send message
		}

		log.Debug("----- extendPVs list -----")
		for _, pvExt := range extendPVs {
			log.Debug(pvExt)
		}
		log.Debug("----- extendPVs list -----")
		log.Debug("----- shrinkPVs list -----")
		for _, pvShr := range shrinkPVs {
			log.Debug(pvShr)
		}
		log.Debug("----- shrinkPVs list -----")

		if len(extendPVs) != 0 {
			log.Info(fmt.Sprintf("[ReconcileLVMVG] CREATE event: %s", EventActionExtending))
			err = CreateEventLVMVolumeGroup(ctx, cl, metrics, EventReasonExtending, EventActionExtending, nodeName, group)
			if err != nil {
				log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error CreateEventLVMVolumeGroup, name: %s", group.Name))
				return true, err
			}
			err := ExtendVGComplex(metrics, extendPVs, group.Spec.ActualVGNameOnTheNode, log)
			if err != nil {
				log.Error(err, fmt.Sprintf("[ReconcileLVMVG] unable to ExtendVGComplex for resource, name: %s", group.Name))
				return true, err
			}
		}

		// Check Resize
		for _, n := range group.Status.Nodes {
			for _, d := range n.Devices {

				if d.DevSize.Value() == 0 {
					log.Error(errors.New("check resize d.DevSize = "), d.DevSize.String())
					log.Error(errors.New(fmt.Sprintf("check resize for device, name: %s", d.BlockDevice)),
						fmt.Sprintf("[ReconcileLVMVG] block device's size equals zero, block device name: %s, size: %s", d.BlockDevice, d.DevSize.String()))
					return false, err
				}

				log.Debug(fmt.Sprintf("[ReconcileLVMVG] check Resize d.PVSize = %s", d.PVSize))

				dPVSizeTmp := resource.MustParse(d.PVSize)

				if dPVSizeTmp.Value() == 0 {
					log.Warning(fmt.Sprintf("[ReconcileLVMVG] check dev PV size device.PVSize = %s", d.PVSize))
					return false, nil
				}

				delta, _ := utils.QuantityToBytes(internal.ResizeDelta)

				log.Debug("---------- Reconcile ---------------")
				log.Debug(fmt.Sprintf("[ReconcileLVMVG] PVSize = %s", d.PVSize))
				log.Debug(fmt.Sprintf("[ReconcileLVMVG] DevSize = %s %s", d.DevSize.String(), d.PVSize))
				log.Debug(fmt.Sprintf("[ReconcileLVMVG] Resize flag = %t", d.DevSize.Value()-dPVSizeTmp.Value() > delta))
				log.Debug("---------- Reconcile ---------------")

				if d.DevSize.Value() < dPVSizeTmp.Value() {
					return false, nil
				}

				log.Info(fmt.Sprintf("[ReconcileLVMVG] devSize changed, block device name: %s", d.BlockDevice))
				log.Info(fmt.Sprintf("[ReconcileLVMVG] devSize %s ", d.DevSize.String()))
				log.Info(fmt.Sprintf("[ReconcileLVMVG] pvSize %s ", d.DevSize.String()))

				if d.DevSize.Value()-dPVSizeTmp.Value() > delta {
					log.Info(fmt.Sprintf("[ReconcileLVMVG] create event: %s", EventActionResizing))
					err = CreateEventLVMVolumeGroup(ctx, cl, metrics, EventReasonResizing, EventActionResizing, nodeName, group)
					if err != nil {
						log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error CreateEventLVMVolumeGroup, resource name: %s", group.Name))
					}
					command, err := utils.ResizePV(d.Path)
					log.Debug(fmt.Sprintf("[ReconcileLVMVG] exec command: %s", command))
					if err != nil {
						log.Error(errors.New("check size error"), fmt.Sprintf("[ReconcileLVMVG] devSize <= pvSize, block device: %s", d.BlockDevice))
						err = updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, group.Name, group.Namespace, "devSize <= pvSize", NonOperational)
						if err != nil {
							log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error update LVMVolumeGroup %s", group.Name))
						}
						return true, err
					}
				}
			}
		}

		if group.Spec.ThinPools != nil {
			log.Info("[ReconcileLVMVG] reconcile thin pool")

			statusThinPoolMap := make(map[string]resource.Quantity)
			for _, statusThinPool := range group.Status.ThinPools {
				statusThinPoolMap[statusThinPool.Name] = statusThinPool.ActualSize
			}

			for _, pool := range group.Spec.ThinPools {
				log.Debug(fmt.Sprintf("[ReconcileLVMVG] Start reconcile thin pool: %s", pool.Name))
				log.Debug(fmt.Sprintf("[ReconcileLVMVG] thin pool size: %d", pool.Size.Value()))

				statusPoolActualSize, ok := statusThinPoolMap[pool.Name]

				if !ok {
					log.Info(fmt.Sprintf("[ReconcileLVMVG] create event: %s", EventActionCreating))
					err = CreateEventLVMVolumeGroup(ctx, cl, metrics, EventReasonCreating, EventActionCreating, nodeName, group)
					if err != nil {
						log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error CreateEventLVMVolumeGroup, resource name: %s", group.Name))
					}
					start := time.Now()
					command, err := utils.CreateThinPool(pool, group.Spec.ActualVGNameOnTheNode)
					metrics.UtilsCommandsDuration(LVMVolumeGroupWatcherCtrlName, "lvcreate").Observe(metrics.GetEstimatedTimeInSeconds(start))
					metrics.UtilsCommandsExecutionCount(LVMVolumeGroupWatcherCtrlName, "lvcreate").Inc()
					log.Debug(command)
					if err != nil {
						log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error CreateThinPool, thin pool: %s", pool.Name))
						metrics.UtilsCommandsErrorsCount(LVMVolumeGroupWatcherCtrlName, "lvcreate").Inc()
						if err = updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, group.Name, group.Namespace, err.Error(), NonOperational); err != nil {
							log.Error(err, fmt.Sprintf("[ReconcileLVMVG] unable to update LVMVolumeGroupStatus, resource name: %s", group.Name))
						}
						return true, err
					}
					continue
				}

				groupStatusVGSizeTmp := resource.MustParse(group.Status.VGSize)
				groupStatusAllocatedSizeTmp := resource.MustParse(group.Status.AllocatedSize)

				freeSpace := groupStatusVGSizeTmp.Value() - groupStatusAllocatedSizeTmp.Value()
				addSize := pool.Size.Value() - statusPoolActualSize.Value()
				if addSize <= 0 {
					err = errors.New("resize thin pool")
					log.Error(err, "[ReconcileLVMVG] add size value <= 0")
					return false, err
				}

				log.Debug(fmt.Sprintf("[ReconcileLVMVG] vgSizeGb = %s", group.Status.VGSize))
				log.Debug(fmt.Sprintf("[ReconcileLVMVG] allocatedSizeGb = %s", group.Status.AllocatedSize))

				log.Debug(fmt.Sprintf("[ReconcileLVMVG] specPoolSize = %s", pool.Size.String()))
				log.Debug(fmt.Sprintf("[ReconcileLVMVG] statusPoolActualSize = %s", statusPoolActualSize.String()))

				log.Debug(fmt.Sprintf("[ReconcileLVMVG] VG freeSpace = %s", strconv.FormatInt(freeSpace, 10)))
				log.Debug(fmt.Sprintf("[ReconcileLVMVG] thinpool addSize = %s", strconv.FormatInt(addSize, 10)))

				if addSize < 0 {
					err = errors.New("resize thin pool")
					log.Error(err, "[ReconcileLVMVG] add size value < 0")
					return false, err
				}

				if addSize > 0 {
					log.Debug(fmt.Sprintf("[ReconcileLVMVG] Identified a thin pool requiring resize: %s", pool.Name))
					if freeSpace > addSize {
						log.Info(fmt.Sprintf("[ReconcileLVMVG] create event: %s", EventActionResizing))
						err = CreateEventLVMVolumeGroup(ctx, cl, metrics, EventReasonResizing, EventActionResizing, nodeName, group)
						if err != nil {
							log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error CreateEventLVMVolumeGroup, resource name: %s", group.Name))
						}
						newLVSizeStr := strconv.FormatInt(pool.Size.Value()/1024, 10)
						start := time.Now()
						cmd, err := utils.ExtendLV(newLVSizeStr+"K", group.Spec.ActualVGNameOnTheNode, pool.Name)
						metrics.UtilsCommandsDuration(LVMVolumeGroupWatcherCtrlName, "lvextend").Observe(metrics.GetEstimatedTimeInSeconds(start))
						metrics.UtilsCommandsExecutionCount(LVMVolumeGroupWatcherCtrlName, "lvextend").Inc()
						log.Debug(cmd)
						if err != nil {
							metrics.UtilsCommandsErrorsCount(LVMVolumeGroupWatcherCtrlName, "lvextend").Inc()
							log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error ExtendLV, pool name: %s", pool.Name))
							return true, err
						}
					} else {
						log.Error(errors.New("ThinPools resize"), fmt.Sprintf("[ReconcileLVMVG] addSize > freeSize, pool name: %s", pool.Name))
					}
				} else {
					log.Debug(fmt.Sprintf("[ReconcileLVMVG] No need to resize thin pool: %s", pool.Name))

				}
				log.Debug(fmt.Sprintf("[ReconcileLVMVG] END reconcile thin pool: %s", pool.Name))
			}

		}

	} else {
		log.Info(fmt.Sprintf("[ReconcileLVMVG] create event: %s", EventActionCreating))
		err = CreateEventLVMVolumeGroup(ctx, cl, metrics, EventReasonCreating, EventActionCreating, nodeName, group)
		if err != nil {
			log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error CreateEventLVMVolumeGroup, resource name: %s", group.Name))
		}

		err := CreateVGComplex(ctx, cl, metrics, group, log)
		if err != nil {
			log.Error(err, fmt.Sprintf("[ReconcileLVMVG] unable to CreateVGComplex for resource, name: %s", group.Name))
			if err = updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, group.Name, group.Namespace, err.Error(), NonOperational); err != nil {
				log.Error(err, fmt.Sprintf("[ReconcileLVMVG] unable to update LVMVolumeGroupStatus, resource name: %s", group.Name))
			}

			return true, err
		}

		if len(group.Spec.ThinPools) != 0 {
			for _, v := range group.Spec.ThinPools {
				start := time.Now()
				command, err := utils.CreateThinPool(v, group.Spec.ActualVGNameOnTheNode)
				metrics.UtilsCommandsDuration(LVMVolumeGroupWatcherCtrlName, "lvcreate").Observe(metrics.GetEstimatedTimeInSeconds(start))
				metrics.UtilsCommandsExecutionCount(LVMVolumeGroupWatcherCtrlName, "lvcreate").Inc()
				log.Debug(command)
				if err != nil {
					metrics.UtilsCommandsErrorsCount(LVMVolumeGroupWatcherCtrlName, "lvcreate").Inc()
					log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error CreateThinPool, thin pool: %s", v.Name))
					if err = updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, group.Name, group.Namespace, err.Error(), NonOperational); err != nil {
						log.Error(err, fmt.Sprintf("[ReconcileLVMVG] unable to update LVMVolumeGroupStatus, resource name: %s", group.Name))
					}
					return true, err
				}
			}
		}
	}

	log.Info("[ReconcileLVMVG] reconcile loop end")
	err = updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, group.Name, group.Namespace, "", Operational)
	if err != nil {
		log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error update LVMVolumeGroup %s", group.Name))
		return true, err
	}

	log.Info(fmt.Sprintf("[ReconcileLVMVG] create event: %s", EventActionReady))
	err = CreateEventLVMVolumeGroup(ctx, cl, metrics, EventReasonReady, EventActionReady, nodeName, group)
	if err != nil {
		log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error CreateEventLVMVolumeGroup, resource name: %s", group.Name))
	}

	return false, nil
}
