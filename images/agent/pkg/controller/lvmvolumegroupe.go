package controller

import (
	"context"
	"errors"
	"fmt"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/util/workqueue"
	"reflect"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"storage-configurator/api/v1alpha1"
	"storage-configurator/internal"
	"storage-configurator/pkg/logger"
	"storage-configurator/pkg/utils"
	"strconv"
	"strings"
)

const (
	lvmVolumeGroupName = "lvm-volume-group-controller"
)

func RunLVMVolumeGroupController(
	ctx context.Context,
	mgr manager.Manager,
	nodeName string,
	log logger.Logger,
) (controller.Controller, error) {
	cl := mgr.GetClient()
	cache := mgr.GetCache()

	c, err := controller.New(lvmVolumeGroupName, mgr, controller.Options{
		Reconciler: reconcile.Func(func(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
			return reconcile.Result{}, nil
		}),
	})
	if err != nil {
		log.Error(err, "[RunLVMVolumeGroupController] Unable to create controller RunLVMVolumeGroupController")
		return nil, err
	}

	createFunc := func(ctx context.Context, e event.CreateEvent, q workqueue.RateLimitingInterface) {
		log.Info(fmt.Sprintf("[RunLVMVolumeGroupController] event create LVMVolumeGroup, name: %s", e.Object.GetName()))
		ReconcileLVMVG(ctx, e.Object.GetName(), e.Object.GetNamespace(), nodeName, log, cl)
	}
	updateFunc := func(ctx context.Context, updateEvent event.UpdateEvent, limitingInterface workqueue.RateLimitingInterface) {
		log.Info(fmt.Sprintf("[RunLVMVolumeGroupController] update LVMVolumeGroupn, name: %s", updateEvent.ObjectNew.GetName()))

		newLVG, ok := updateEvent.ObjectNew.(*v1alpha1.LvmVolumeGroup)
		if !ok {
			log.Error(err, "[RunLVMVolumeGroupController] error get  ObjectNew LinstorStoragePool")
		}

		oldLVG, ok := updateEvent.ObjectOld.(*v1alpha1.LvmVolumeGroup)
		if !ok {
			log.Error(err, "[RunLVMVolumeGroupController] error get  ObjectOld LinstorStoragePool")
		}

		if !reflect.DeepEqual(oldLVG.Annotations, newLVG.Annotations) {
			log.Info("[RunLVMVolumeGroupController] annotations update")
			ReconcileLVMVG(ctx, updateEvent.ObjectNew.GetName(), updateEvent.ObjectNew.GetNamespace(), nodeName, log, cl)
			return
		}

		if !reflect.DeepEqual(oldLVG.Spec, newLVG.Spec) {
			log.Info("[RunLVMVolumeGroupController] lvg spec changed")
			ReconcileLVMVG(ctx, updateEvent.ObjectNew.GetName(), updateEvent.ObjectNew.GetNamespace(), nodeName, log, cl)
			return
		}

		if !reflect.DeepEqual(oldLVG.Status.Nodes, newLVG.Status.Nodes) {
			ReconcileLVMVG(ctx, updateEvent.ObjectNew.GetName(), updateEvent.ObjectNew.GetNamespace(), nodeName, log, cl)
		} else {
			log.Info("[RunLVMVolumeGroupController] lvg check dev size")
			for _, node := range newLVG.Status.Nodes {
				for _, device := range node.Devices {

					if device.DevSize.Value() == 0 {
						log.Warning(fmt.Sprintf("[RunLVMVolumeGroupController] check dev size device.DevSize = %s", device.DevSize.String()))
						return
					}

					log.Debug(fmt.Sprintf("[RunLVMVolumeGroupController] update spec check resize device.PVSize = %s", device.PVSize.String()))

					if device.PVSize.Value() == 0 {
						log.Warning(fmt.Sprintf("[RunLVMVolumeGroupController] check dev PV size device.PVSize = %s", device.PVSize.String()))
						return
					}

					delta, _ := utils.QuantityToBytes(internal.ResizeDelta)

					log.Debug(fmt.Sprintf("[RunLVMVolumeGroupController] resize flag = %t", device.DevSize.Value()-device.PVSize.Value() > delta))

					if device.DevSize.Value()-device.PVSize.Value() > delta {
						log.Info("[RunLVMVolumeGroupController] lvg status device and PV changed")
						ReconcileLVMVG(ctx, updateEvent.ObjectNew.GetName(), updateEvent.ObjectNew.GetNamespace(), nodeName, log, cl)
					}
				}
			}
		}
	}

	err = c.Watch(source.Kind(cache, &v1alpha1.LvmVolumeGroup{}), &handler.Funcs{
		CreateFunc: createFunc,
		UpdateFunc: updateFunc,
	})

	if err != nil {
		log.Error(err, "[RunLVMVolumeGroupController] error Watch controller RunLVMVolumeGroupController")
		return nil, err
	}
	return c, err
}

func ReconcileLVMVG(ctx context.Context, objectName, objectNameSpace, nodeName string, log logger.Logger, cl client.Client) {
	log.Info("[ReconcileLVMVG] reconcile loop start")
	group, err := getLVMVolumeGroup(ctx, cl, objectNameSpace, objectName)
	if err != nil {
		log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error getLVMVolumeGroup, objectname: %s", objectName))
		return
	}
	validation, status, err := ValidationLVMGroup(ctx, cl, group, objectNameSpace, nodeName)

	if group == nil {
		log.Error(nil, "[ReconcileLVMVG] requested LVMVG group in nil")
		return
	}

	if status.Health == NoOperational {
		health := status.Health
		var message string
		if err != nil {
			message = err.Error()
		}

		log.Error(err, fmt.Sprintf("[ReconcileLVMVG] ValidationLVMGroup, resource name: %s", group.Name))
		err = updateLVMVolumeGroupStatus(ctx, cl, group.Name, group.Namespace, message, health)
		if err != nil {
			log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error update LVMVolumeGroup %s", group.Name))
			return
		}
	}

	if err != nil {
		log.Error(err, fmt.Sprintf("[ReconcileLVMVG] validationLVMGroup failed, resource name: %s", group.Name))
		return
	}

	if validation == false {
		log.Info(fmt.Sprintf("[ReconcileLVMVG] validation failed for resource, name: %s", group.Name))
		log.Info(fmt.Sprintf("[ReconcileLVMVG] status.Message = %s", status.Message))
		return
	}

	log.Info("[ReconcileLVMVG] validation passed")

	annotationMark := 0
	for k, _ := range group.Annotations {
		if strings.Contains(k, delAnnotation) {
			annotationMark++
		}
	}

	if annotationMark != 0 {
		// lock
		log.Info(fmt.Sprintf("[ReconcileLVMVG] create event: %s", EventActionDeleting))
		err = CreateEventLVMVolumeGroup(ctx, cl, EventReasonDeleting, EventActionDeleting, nodeName, group)
		if err != nil {
			log.Error(err, "[ReconcileLVMVG] error CreateEventLVMVolumeGroup")
		}

		err := DeleteVG(group.Spec.ActualVGNameOnTheNode, log)
		if err != nil {
			log.Error(err, fmt.Sprintf("[ReconcileLVMVG] delete VG, name: %s", group.Spec.ActualVGNameOnTheNode))
			return
		}

		log.Info(fmt.Sprintf("[ReconcileLVMVG] VG deleted, name: %s", group.Spec.ActualVGNameOnTheNode))
		return
	}

	log.Info("[ReconcileLVMVG] start reconciliation VG process")
	log.Info(fmt.Sprintf("[ReconcileLVMVG] create event: %s", EventActionProvisioning))
	err = CreateEventLVMVolumeGroup(ctx, cl, EventReasonProvisioning, EventActionProvisioning, nodeName, group)
	if err != nil {
		log.Error(err, "[ReconcileLVMVG] error CreateEventLVMVolumeGroup")
	}

	existVG, err := ExistVG(group.Spec.ActualVGNameOnTheNode, log)
	if err != nil {
		log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error ExistVG, name: %s", group.Spec.ActualVGNameOnTheNode))
	}
	if existVG {
		log.Info("[ReconcileLVMVG] validation and choosing the type of operation")
		extendPVs, _, err := ValidationTypeLVMGroup(ctx, cl, group, log)
		if err != nil {
			log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error ValidationTypeLVMGroup, name: %s", group.Name))
		}

		if len(extendPVs) != 0 {
			log.Info(fmt.Sprintf("[ReconcileLVMVG] CREATE event: %s", EventActionExtending))
			err = CreateEventLVMVolumeGroup(ctx, cl, EventReasonExtending, EventActionExtending, nodeName, group)
			if err != nil {
				log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error CreateEventLVMVolumeGroup, name: %s", group.Name))
			}
			err := ExtendVGComplex(extendPVs, group.Spec.ActualVGNameOnTheNode, log)
			if err != nil {
				log.Error(err, fmt.Sprintf("[ReconcileLVMVG] unable to ExtendVGComplex for resource, name: %s", group.Name))
			}
		}

		//ToDo shrinkPVs > 0  _test_case_4  ???
		//if len(shrinkPVs) != 0 {
		//	log.Info("create event " + EventActionShrinking)
		//	err = CreateEventLVMVolumeGroup(ctx, cl, EventReasonShrinking, EventActionShrinking, nodeName, group)
		//	if err != nil {
		//		log.Error(err, " error CreateEventLVMVolumeGroup")
		//	}
		//
		//	group.Status.Health = NoOperational
		//	err = updateLVMVolumeGroup(ctx, cl, group)
		//	if err != nil {
		//		log.Error(err, fmt.Sprintf("error update LVMVolumeGroup %s", group.Name))
		//		return
		//	}
		//
		//	// Deleting PV from VG
		//	for _, pvPath := range shrinkPVs {
		//		command, err := utils.RemovePVFromVG(pvPath, group.Spec.ActualVGNameOnTheNode)
		//		log.Debug(command)
		//		if err != nil {
		//			log.Error(err, "RemovePVFromVG")
		//		}
		//	}
		//
		//	group.Status.Health = Operational
		//	err = updateLVMVolumeGroup(ctx, cl, group)
		//	if err != nil {
		//		log.Error(err, fmt.Sprintf("error update LVMVolumeGroup %s", group.Name))
		//		return
		//	}
		//}

		// Check Resize
		for _, n := range group.Status.Nodes {
			for _, d := range n.Devices {

				if d.DevSize.Value() == 0 {
					log.Error(errors.New("check resize d.DevSize = "), d.DevSize.String())
					log.Error(errors.New(fmt.Sprintf("check resize for device, name: %s", d.BlockDevice)),
						fmt.Sprintf("[ReconcileLVMVG] device, name: %s equals zero", d.DevSize.String()))
					return
				}

				//devSize, err := utils.QuantityToBytes(d.DevSize)
				//if err != nil {
				//	log.Error(err, "device.DevSize")
				//	return
				//}

				log.Debug(fmt.Sprintf("[ReconcileLVMVG] check Resize d.PVSize = %s", d.PVSize.String()))

				if d.PVSize.Value() == 0 {
					log.Warning(fmt.Sprintf("[ReconcileLVMVG] check dev PV size device.PVSize = %s", d.PVSize.String()))
					return
				}

				//pvSize, err := utils.QuantityToBytes(d.PVSize)
				//if err != nil {
				//	log.Error(err, "device.PVSize")
				//	return
				//}

				delta, _ := utils.QuantityToBytes(internal.ResizeDelta)

				log.Debug("---------- Reconcile ---------------")
				log.Debug(fmt.Sprintf("[ReconcileLVMVG] PVSize = %s %d", d.PVSize.String(), d.PVSize.Value()))
				log.Debug(fmt.Sprintf("[ReconcileLVMVG] DevSize = %s %d", d.DevSize.String(), d.PVSize.Value()))
				log.Debug(fmt.Sprintf("[ReconcileLVMVG] Resize flag = %t", d.DevSize.Value()-d.PVSize.Value() > delta))
				log.Debug("---------- Reconcile ---------------")

				if d.DevSize.Value() < d.PVSize.Value() {
					return
				}

				log.Info(fmt.Sprintf("[ReconcileLVMVG] devSize changed, block device name: %s", d.BlockDevice))
				log.Info(fmt.Sprintf("[ReconcileLVMVG] devSize %s ", d.DevSize.String()))
				log.Info(fmt.Sprintf("[ReconcileLVMVG] pvSize %s ", d.DevSize.String()))

				if d.DevSize.Value()-d.PVSize.Value() > delta {
					log.Info(fmt.Sprintf("[ReconcileLVMVG] create event: %s", EventActionResizing))
					err = CreateEventLVMVolumeGroup(ctx, cl, EventReasonResizing, EventActionResizing, nodeName, group)
					if err != nil {
						log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error CreateEventLVMVolumeGroup, resource name: %s", group.Name))
					}
					command, err := utils.ResizePV(d.Path)
					log.Debug(fmt.Sprintf("[ReconcileLVMVG] exec command: %s", command))
					if err != nil {
						log.Error(errors.New("check size error"), fmt.Sprintf("[ReconcileLVMVG] devSize <= pvSize, block device: %s", d.BlockDevice))
						err = updateLVMVolumeGroupStatus(ctx, cl, group.Name, group.Namespace, "devSize <= pvSize", NoOperational)
						if err != nil {
							log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error update LVMVolumeGroup %s", group.Name))
						}
						return
					}
				}
			}
		}

		if group.Spec.ThinPools != nil {
			log.Info("[ReconcileLVMVG] reconcile thin pool")
			log.Info(fmt.Sprintf("[ReconcileLVMVG] create event: %s", EventActionResizing))
			err = CreateEventLVMVolumeGroup(ctx, cl, EventReasonResizing, EventActionResizing, nodeName, group)
			if err != nil {
				log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error CreateEventLVMVolumeGroup, resource name: %s", group.Name))
			}

			statusThinPoolMap := make(map[string]resource.Quantity)
			for _, statusThinPool := range group.Status.ThinPools {
				statusThinPoolMap[statusThinPool.Name] = statusThinPool.ActualSize
			}

			// VG allocatedSize
			//allocatedSizeGb, err := utils.QuantityToBytes(group.Status.AllocatedSize)
			//if err != nil {
			//	log.Error(err, "parse spec AllocatedSize")
			//}

			for _, pool := range group.Spec.ThinPools {
				//specPoolSize, err := utils.QuantityToBytes(pool.Size)
				//if err != nil {
				//	log.Error(err, "parse spec size thin pool")
				//	return
				//}

				//statusPoolActualSize, err := utils.QuantityToBytes(statusThinPoolMap[pool.Name])
				//if err != nil {
				//	log.Error(err, "parse status size actual thin pool")
				//	return
				//}

				statusPoolActualSize := statusThinPoolMap[pool.Name]

				freeSpace := group.Status.VGSize.Value() - group.Status.AllocatedSize.Value()
				addSize := pool.Size.Value() - statusPoolActualSize.Value()
				if addSize <= 0 {
					log.Error(errors.New("resize thin pool"), "[ReconcileLVMVG] add size value <= 0")
					return
				}

				log.Debug(fmt.Sprintf("[ReconcileLVMVG] vgSizeGb = %s", group.Status.VGSize.String()))
				log.Debug(fmt.Sprintf("[ReconcileLVMVG] allocatedSizeGb = %s", group.Status.AllocatedSize.String()))

				log.Debug(fmt.Sprintf("[ReconcileLVMVG] specPoolSize = %s", group.Status.VGSize.String()))
				log.Debug(fmt.Sprintf("[ReconcileLVMVG] statusPoolActualSize = %s", statusPoolActualSize.String()))

				log.Debug(fmt.Sprintf("[ReconcileLVMVG] freeSpace = %s", strconv.FormatInt(freeSpace, 10)))
				log.Debug(fmt.Sprintf("[ReconcileLVMVG] addSize = %s", strconv.FormatInt(addSize, 10)))

				if freeSpace > addSize {
					newLVSizeStr := strconv.FormatInt(pool.Size.Value()/1024, 10)
					cmd, err := utils.ExtendLV(newLVSizeStr+"K", group.Spec.ActualVGNameOnTheNode, pool.Name)
					log.Debug(cmd)
					if err != nil {
						log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error ExtendLV, pool name: %s", pool.Name))
						return
					}
				} else {
					log.Error(errors.New("ThinPools resize"), fmt.Sprintf("[ReconcileLVMVG] addSize > freeSize, pool name: %s", pool.Name))
				}
			}
		}

	} else {
		log.Info(fmt.Sprintf("[ReconcileLVMVG] create event: %s", EventActionCreating))
		err = CreateEventLVMVolumeGroup(ctx, cl, EventReasonCreating, EventActionCreating, nodeName, group)
		if err != nil {
			log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error CreateEventLVMVolumeGroup, resource name: %s", group.Name))
		}

		err := CreateVGComplex(ctx, cl, group, log)
		if err != nil {
			log.Error(err, fmt.Sprintf("[ReconcileLVMVG] unable to CreateVGComplex for resource, name: %s", group.Name))
			if err = updateLVMVolumeGroupStatus(ctx, cl, group.Name, group.Namespace, err.Error(), NoOperational); err != nil {
				log.Error(err, fmt.Sprintf("[ReconcileLVMVG] unable to update LVMVolumeGroupStatus, resource name: %s", group.Name))
			}

			return
		}

		if len(group.Spec.ThinPools) != 0 {
			for _, v := range group.Spec.ThinPools {
				command, err := utils.CreateLV(v, group.Spec.ActualVGNameOnTheNode)
				log.Debug(command)
				if err != nil {
					log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error CreateLV, thin pool: %s", v.Name))
					if err = updateLVMVolumeGroupStatus(ctx, cl, group.Name, group.Namespace, err.Error(), NoOperational); err != nil {
						log.Error(err, fmt.Sprintf("[ReconcileLVMVG] unable to update LVMVolumeGroupStatus, resource name: %s", group.Name))
					}
					return
				}
			}
		}
	}

	log.Info("[ReconcileLVMVG] reconcile loop end")
	err = updateLVMVolumeGroupStatus(ctx, cl, group.Name, group.Namespace, "", Operational)
	if err != nil {
		log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error update LVMVolumeGroup %s", group.Name))
		return
	}

	log.Info(fmt.Sprintf("[ReconcileLVMVG] create event: %s", EventActionReady))
	err = CreateEventLVMVolumeGroup(ctx, cl, EventReasonReady, EventActionReady, nodeName, group)
	if err != nil {
		log.Error(err, fmt.Sprintf("[ReconcileLVMVG] error CreateEventLVMVolumeGroup, resource name: %s", group.Name))
	}
}
