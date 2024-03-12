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
		log.Info(fmt.Sprintf("[RunLVMVolumeGroupController] Get event CREATE for resource LVMVolumeGroup, name: %s", e.Object.GetName()))

		request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: e.Object.GetNamespace(), Name: e.Object.GetName()}}
		shouldRequeue, err := ReconcileLVMVG(ctx, metrics, e.Object.GetName(), e.Object.GetNamespace(), cfg.NodeName, log, cl)
		if shouldRequeue {
			log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] An error has occured in ReconcileLVMVG. Adds to retry after %d seconds.", cfg.VolumeGroupScanInterval))
			q.AddAfter(request, cfg.VolumeGroupScanInterval*time.Second)
			log.Warning(fmt.Sprintf(`Added request, namespace: "%s" name: "%s", to requeue`, request.Namespace, request.Name))
		}
	}

	updateFunc := func(ctx context.Context, e event.UpdateEvent, q workqueue.RateLimitingInterface) {
		log.Info(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] Get event UPDATE for resource LVMVolumeGroup, name: %s", e.ObjectNew.GetName()))

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
	lvg, err := getLVMVolumeGroup(ctx, cl, metrics, objectNameSpace, objectName)
	if err != nil {
		log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error getLVMVolumeGroup, objectname: %s", objectName))
		return false, err
	}
	if lvg == nil {
		err = errors.New("nil pointer detected")
		log.Error(err, "[ReconcileLVMVG] requested LVMVG group in nil")
		return true, err
	}

	isOwnedByNode, status, err := CheckLVMVGNodeOwnership(ctx, cl, metrics, lvg, objectNameSpace, nodeName)

	if err != nil {
		log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error CheckLVMVGNodeOwnership, resource name: %s", lvg.Name))
		if status.Health == NonOperational {
			health := status.Health
			message := status.Message
			log.Error(err, fmt.Sprintf("[ReconcileLVMVG] ValidateLVMGroup, resource name: %s, health: %s, phase: %s, message: %s", lvg.Name, health, status.Phase, message))
			err = updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, lvg.Name, lvg.Namespace, message, health)
			if err != nil {
				log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error update LVMVolumeGroup %s", lvg.Name))
				return true, err
			}
		}
		return true, err
	}

	if !isOwnedByNode {
		log.Debug(fmt.Sprintf("[ReconcileLVMVG] resource is not owned by node, name: %s, skip it", lvg.Name))
		return false, nil
	}

	log.Info("[ReconcileLVMVG] validation passed")

	annotationMark := 0
	for k := range lvg.Annotations {
		if strings.Contains(k, delAnnotation) {
			annotationMark++
		}
	}

	if annotationMark != 0 {
		// lock
		log.Info(fmt.Sprintf("[ReconcileLVMVG] create event: %s", EventActionDeleting))
		err = CreateEventLVMVolumeGroup(ctx, cl, metrics, EventReasonDeleting, EventActionDeleting, nodeName, lvg)
		if err != nil {
			log.Error(err, "[ReconcileLVMVG] error CreateEventLVMVolumeGroup")
			return true, err
		}

		err := DeleteVG(lvg.Spec.ActualVGNameOnTheNode, log, metrics)
		if err != nil {
			log.Error(err, fmt.Sprintf("[ReconcileLVMVG] delete VG, name: %s", lvg.Spec.ActualVGNameOnTheNode))
			return true, err
		}

		log.Info(fmt.Sprintf("[ReconcileLVMVG] VG deleted, name: %s", lvg.Spec.ActualVGNameOnTheNode))
		return false, nil
	}

	log.Info("[ReconcileLVMVG] start reconciliation VG process")
	log.Info(fmt.Sprintf("[ReconcileLVMVG] create event: %s", EventActionProvisioning))
	err = CreateEventLVMVolumeGroup(ctx, cl, metrics, EventReasonProvisioning, EventActionProvisioning, nodeName, lvg)
	if err != nil {
		log.Error(err, "[ReconcileLVMVG] error CreateEventLVMVolumeGroup")
		return true, err
	}
	log.Info(fmt.Sprintf(`[ReconcileLVMVG] event was created for resource, name: %s`, lvg.Name))

	isVgExist, vg, err := GetVGFromNode(lvg.Spec.ActualVGNameOnTheNode, log, metrics)
	if err != nil {
		log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error ExistVG, name: %s", lvg.Spec.ActualVGNameOnTheNode))
		return true, err
	}
	if isVgExist {
		log.Debug("[ReconcileLVMVG] start UpdateLVMVolumeGroupTagsName for r " + lvg.Name)
		updated, err := UpdateLVMVolumeGroupTagsName(log, metrics, vg, lvg)
		if err != nil {
			log.Error(err, fmt.Sprintf("[ReconcileLVMVG] unable to update VG tags on VG, name: %s", lvg.Spec.ActualVGNameOnTheNode))
			return true, err
		}

		if updated {
			log.Info(fmt.Sprintf("[ReconcileLVMVG] tag storage.deckhouse.io/lvmVolumeGroupName was updated on VG, name: %s", lvg.Spec.ActualVGNameOnTheNode))
		} else {
			log.Debug(fmt.Sprintf("[ReconcileLVMVG] no need to update tag storage.deckhouse.io/lvmVolumeGroupName on VG, name: %s", lvg.Spec.ActualVGNameOnTheNode))
		}

		log.Info("[ReconcileLVMVG] validation and choosing the type of operation")
		extendPVs, shrinkPVs, err := ValidateOperationTypeLVMGroup(ctx, cl, metrics, lvg, log)
		if err != nil {
			log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error ValidateOperationTypeLVMGroup, name: %s", lvg.Name))
			return true, err
		}

		// if err == nil && extendPVs == nil && shrinkPVs == nil {
		// 	log.Warning(fmt.Sprintf("[ReconcileLVMVG] ValidateOperationTypeLVMGroup FAIL for resource %s", lvg.Name))
		// 	//todo retry and send message
		// }

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
			err = CreateEventLVMVolumeGroup(ctx, cl, metrics, EventReasonExtending, EventActionExtending, nodeName, lvg)
			if err != nil {
				log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error CreateEventLVMVolumeGroup, name: %s", lvg.Name))
				return true, err
			}
			err := ExtendVGComplex(metrics, extendPVs, lvg.Spec.ActualVGNameOnTheNode, log)
			if err != nil {
				log.Error(err, fmt.Sprintf("[ReconcileLVMVG] unable to ExtendVGComplex for resource, name: %s", lvg.Name))
				return true, err
			}
		}

		// Check Resize
		for _, n := range lvg.Status.Nodes {
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
					err = CreateEventLVMVolumeGroup(ctx, cl, metrics, EventReasonResizing, EventActionResizing, nodeName, lvg)
					if err != nil {
						log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error CreateEventLVMVolumeGroup, resource name: %s", lvg.Name))
					}
					command, err := utils.ResizePV(d.Path)
					log.Debug(fmt.Sprintf("[ReconcileLVMVG] exec command: %s", command))
					if err != nil {
						log.Error(errors.New("check size error"), fmt.Sprintf("[ReconcileLVMVG] devSize <= pvSize, block device: %s", d.BlockDevice))
						err = updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, lvg.Name, lvg.Namespace, "devSize <= pvSize", NonOperational)
						if err != nil {
							log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error update LVMVolumeGroup %s", lvg.Name))
						}
						return true, err
					}
				}
			}
		}

		if lvg.Spec.ThinPools != nil {
			log.Info("[ReconcileLVMVG] reconcile thin pool")

			statusThinPoolMap := make(map[string]v1alpha1.StatusThinPool)
			for _, statusThinPool := range lvg.Status.ThinPools {
				statusThinPoolMap[statusThinPool.Name] = statusThinPool
			}

			for _, specThinPool := range lvg.Spec.ThinPools {
				log.Debug(fmt.Sprintf("[ReconcileLVMVG] Start reconcile thin pool: %s", specThinPool.Name))
				log.Debug(fmt.Sprintf("[ReconcileLVMVG] thin pool size: %d", specThinPool.Size.Value()))

				statusThinPool, ok := statusThinPoolMap[specThinPool.Name]

				if !ok {
					log.Info(fmt.Sprintf("[ReconcileLVMVG] create event: %s", EventActionCreating))
					err = CreateEventLVMVolumeGroup(ctx, cl, metrics, EventReasonCreating, EventActionCreating, nodeName, lvg)
					if err != nil {
						log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error CreateEventLVMVolumeGroup, resource name: %s", lvg.Name))
					}
					start := time.Now()
					command, err := utils.CreateThinPool(specThinPool, lvg.Spec.ActualVGNameOnTheNode)
					metrics.UtilsCommandsDuration(LVMVolumeGroupWatcherCtrlName, "lvcreate").Observe(metrics.GetEstimatedTimeInSeconds(start))
					metrics.UtilsCommandsExecutionCount(LVMVolumeGroupWatcherCtrlName, "lvcreate").Inc()
					log.Debug(command)
					if err != nil {
						log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error CreateThinPool, thin pool: %s", specThinPool.Name))
						metrics.UtilsCommandsErrorsCount(LVMVolumeGroupWatcherCtrlName, "lvcreate").Inc()
						if err = updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, lvg.Name, lvg.Namespace, err.Error(), NonOperational); err != nil {
							log.Error(err, fmt.Sprintf("[ReconcileLVMVG] unable to update LVMVolumeGroupStatus, resource name: %s", lvg.Name))
						}
						return true, err
					}
					continue
				}

				resizeDelta, err := resource.ParseQuantity(internal.ResizeDelta)
				if err != nil {
					log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error ParseQuantity, resource name: %s", lvg.Name))
					return false, err
				}

				if utils.AreSizesEqualWithinDelta(specThinPool.Size, statusThinPool.ActualSize, resizeDelta) {
					log.Debug(fmt.Sprintf("[ReconcileLVMVG] No need to resize thin pool: %s; specThinPool.Size = %s, statusThinPool.ActualSize = %s", specThinPool.Name, specThinPool.Size.String(), statusThinPool.ActualSize.String()))
					continue
				}

				shouldRequeue, err := ResizeThinPool(ctx, cl, log, metrics, lvg, specThinPool, statusThinPool, nodeName, resizeDelta)
				if err != nil {
					log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error ResizeThinPool, thin pool: %s", specThinPool.Name))
					return shouldRequeue, err
				}

				log.Debug(fmt.Sprintf("[ReconcileLVMVG] END reconcile thin pool: %s", specThinPool.Name))
			}

		}

	} else {
		log.Info(fmt.Sprintf("[ReconcileLVMVG] create event: %s", EventActionCreating))
		err = CreateEventLVMVolumeGroup(ctx, cl, metrics, EventReasonCreating, EventActionCreating, nodeName, lvg)
		if err != nil {
			log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error CreateEventLVMVolumeGroup, resource name: %s", lvg.Name))
		}

		log.Debug("[ReconcileLVMVG] Start CreateVGComplex function for resource " + lvg.Name)
		err := CreateVGComplex(ctx, cl, metrics, lvg, log)
		if err != nil {
			log.Error(err, fmt.Sprintf("[ReconcileLVMVG] unable to CreateVGComplex for resource, name: %s", lvg.Name))
			if err = updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, lvg.Name, lvg.Namespace, err.Error(), NonOperational); err != nil {
				log.Error(err, fmt.Sprintf("[ReconcileLVMVG] unable to update LVMVolumeGroupStatus, resource name: %s", lvg.Name))
			}

			return true, err
		}

		if len(lvg.Spec.ThinPools) != 0 {
			for _, v := range lvg.Spec.ThinPools {
				start := time.Now()
				command, err := utils.CreateThinPool(v, lvg.Spec.ActualVGNameOnTheNode)
				metrics.UtilsCommandsDuration(LVMVolumeGroupWatcherCtrlName, "lvcreate").Observe(metrics.GetEstimatedTimeInSeconds(start))
				metrics.UtilsCommandsExecutionCount(LVMVolumeGroupWatcherCtrlName, "lvcreate").Inc()
				log.Debug(command)
				if err != nil {
					metrics.UtilsCommandsErrorsCount(LVMVolumeGroupWatcherCtrlName, "lvcreate").Inc()
					log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error CreateThinPool, thin pool: %s", v.Name))
					if err = updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, lvg.Name, lvg.Namespace, err.Error(), NonOperational); err != nil {
						log.Error(err, fmt.Sprintf("[ReconcileLVMVG] unable to update LVMVolumeGroupStatus, resource name: %s", lvg.Name))
					}
					return true, err
				}
			}
		}
	}

	log.Info("[ReconcileLVMVG] reconcile loop end")
	err = updateLVMVolumeGroupHealthStatus(ctx, cl, metrics, lvg.Name, lvg.Namespace, "", Operational)
	if err != nil {
		log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error update LVMVolumeGroup %s", lvg.Name))
		return true, err
	}

	log.Info(fmt.Sprintf("[ReconcileLVMVG] create event: %s", EventActionReady))
	err = CreateEventLVMVolumeGroup(ctx, cl, metrics, EventReasonReady, EventActionReady, nodeName, lvg)
	if err != nil {
		log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error CreateEventLVMVolumeGroup, resource name: %s", lvg.Name))
	}

	return false, nil
}
