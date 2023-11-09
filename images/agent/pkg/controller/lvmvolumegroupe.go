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
	"storage-configurator/pkg/log"
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
	log log.Logger,
) (controller.Controller, error) {
	cl := mgr.GetClient()
	cache := mgr.GetCache()

	c, err := controller.New(lvmVolumeGroupName, mgr, controller.Options{
		Reconciler: reconcile.Func(func(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {

			return reconcile.Result{}, nil
		}),
	})
	if err != nil {
		log.Error(err, "error controller RunLVMVolumeGroupController")
		return nil, err
	}

	createFunc := func(ctx context.Context, e event.CreateEvent, q workqueue.RateLimitingInterface) {
		log.Info("event create LVMVolumeGroup " + e.Object.GetName())
		ReconcileLVMVG(ctx, e.Object.GetName(), e.Object.GetNamespace(), nodeName, log, cl)
	}
	updateFunc := func(ctx context.Context, updateEvent event.UpdateEvent, limitingInterface workqueue.RateLimitingInterface) {
		log.Info("update LVMVolumeGroup " + updateEvent.ObjectNew.GetName())

		newLVG, ok := updateEvent.ObjectNew.(*v1alpha1.LvmVolumeGroup)
		if !ok {
			log.Error(err, "error get  ObjectNew LinstorStoragePool")
		}

		oldLVG, ok := updateEvent.ObjectOld.(*v1alpha1.LvmVolumeGroup)
		if !ok {
			log.Error(err, "error get  ObjectOld LinstorStoragePool")
		}

		if !reflect.DeepEqual(oldLVG.Annotations, newLVG.Annotations) {
			log.Info("annotations update")
			ReconcileLVMVG(ctx, updateEvent.ObjectNew.GetName(), updateEvent.ObjectNew.GetNamespace(), nodeName, log, cl)
			return
		}

		if !reflect.DeepEqual(oldLVG.Spec, newLVG.Spec) {
			log.Info("lvg spec changed")
			ReconcileLVMVG(ctx, updateEvent.ObjectNew.GetName(), updateEvent.ObjectNew.GetNamespace(), nodeName, log, cl)
			return
		}

		if !reflect.DeepEqual(oldLVG.Status.Nodes, newLVG.Status.Nodes) {
			ReconcileLVMVG(ctx, updateEvent.ObjectNew.GetName(), updateEvent.ObjectNew.GetNamespace(), nodeName, log, cl)
		} else {
			log.Info("lvg check dev size")
			for _, node := range newLVG.Status.Nodes {
				for _, device := range node.Devices {

					if device.DevSize.Value() == 0 {
						log.Warning("check dev size device.DevSize = " + device.DevSize.String())
						return
					}

					log.Debug("Update spec check resize device.PVSize = " + device.PVSize.String())

					if device.PVSize.Value() == 0 {
						log.Warning("check dev PV size device.PVSize = " + device.PVSize.String())
						return
					}

					delta, _ := utils.QuantityToBytes(internal.ResizeDelta)

					log.Debug("---------- Update LVG event ---------------")
					log.Debug(fmt.Sprintf("Resize flag = %t", device.DevSize.Value()-device.PVSize.Value() > delta))

					if device.DevSize.Value()-device.PVSize.Value() > delta {
						log.Info("lvg status device and PV changed")
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
		log.Error(err, "error Watch controller RunLVMVolumeGroupController")
		return nil, err
	}
	return c, err
}

func ReconcileLVMVG(ctx context.Context, objectName, objectNameSpace, nodeName string, log log.Logger, cl client.Client) {

	log.Info("reconcile loop start")
	group, err := getLVMVolumeGroup(ctx, cl, objectNameSpace, objectName)
	if err != nil {
		log.Error(err, "error getLVMVolumeGroup")
		return
	}
	validation, status, err := ValidationLVMGroup(ctx, cl, group, objectNameSpace, nodeName)

	if group == nil {
		log.Error(nil, "requested LVMVG group in nil")
		return
	}

	// todo check NonOperation
	if len(status.Health) != 0 {
		health := status.Health
		var message string
		if err != nil {
			message = err.Error()
		}

		log.Error(err, "ValidationLVMGroup")
		err = updateLVMVolumeGroupStatus(ctx, cl, group.Name, group.Namespace, message, health)
		if err != nil {
			log.Error(err, fmt.Sprintf("error update LVMVolumeGroup %s", group.Name))
			return
		}
	}

	if err != nil {
		log.Error(err, "ValidationLVMGroup failed")
		return
	}

	if validation == false {
		log.Info("Validation failed")
		log.Info("status.Message = " + status.Message)
		return
	}

	log.Info("Validation passed")

	annotationMark := 0
	for k, _ := range group.Annotations {
		if strings.Contains(k, delAnnotation) {
			annotationMark++
		}
	}

	if annotationMark != 0 {
		// lock
		log.Info("create event " + EventActionDeleting)
		err = CreateEventLVMVolumeGroup(ctx, cl, EventReasonDeleting, EventActionDeleting, nodeName, group)
		if err != nil {
			log.Error(err, " error CreateEventLVMVolumeGroup")
		}

		err := DeleteVG(group.Spec.ActualVGNameOnTheNode, log)
		if err != nil {
			log.Error(err, "DeleteVG "+group.Spec.ActualVGNameOnTheNode)
			return
		}

		log.Info("VG deleted" + group.Spec.ActualVGNameOnTheNode)
		return
	}

	log.Info("start reconciliation VG process")
	log.Info("create event " + EventActionProvisioning)
	err = CreateEventLVMVolumeGroup(ctx, cl, EventReasonProvisioning, EventActionProvisioning, nodeName, group)
	if err != nil {
		log.Error(err, " error CreateEventLVMVolumeGroup")
	}

	existVG, err := ExistVG(group.Spec.ActualVGNameOnTheNode, log)
	if err != nil {
		log.Error(err, " error ExistVG "+group.Spec.ActualVGNameOnTheNode)
	}
	if existVG {
		log.Info("validation and choosing the type of operation")
		extendPVs, _, err := ValidationTypeLVMGroup(ctx, cl, group, log)
		if err != nil {
			log.Error(err, " error ValidationTypeLVMGroup")
		}

		if len(extendPVs) != 0 {
			log.Info("CREATE event " + EventActionExtending)
			err = CreateEventLVMVolumeGroup(ctx, cl, EventReasonExtending, EventActionExtending, nodeName, group)
			if err != nil {
				log.Error(err, " error CreateEventLVMVolumeGroup")
			}
			err := ExtendVGComplex(extendPVs, group.Spec.ActualVGNameOnTheNode, log)
			if err != nil {
				log.Error(err, "ExtendVGComplex")
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
					return
				}

				//devSize, err := utils.QuantityToBytes(d.DevSize)
				//if err != nil {
				//	log.Error(err, "device.DevSize")
				//	return
				//}

				log.Debug("Check Resize d.PVSize = " + d.PVSize.String())

				if d.PVSize.Value() == 0 {
					log.Warning("check dev PV size device.PVSize = " + d.PVSize.String())
					return
				}

				//pvSize, err := utils.QuantityToBytes(d.PVSize)
				//if err != nil {
				//	log.Error(err, "device.PVSize")
				//	return
				//}

				delta, _ := utils.QuantityToBytes(internal.ResizeDelta)

				log.Debug("---------- Reconcile ---------------")
				log.Debug(fmt.Sprintf("PVSize = %s %d", d.PVSize.String(), d.PVSize.Value()))
				log.Debug(fmt.Sprintf("DevSize = %s %d", d.DevSize.String(), d.PVSize.Value()))
				log.Debug(fmt.Sprintf("Resize flag = %t", d.DevSize.Value()-d.PVSize.Value() > delta))
				log.Debug("---------- Reconcile ---------------")

				if d.DevSize.Value() < d.PVSize.Value() {
					return
				}

				log.Info("devSize changed")
				log.Info(fmt.Sprintf("devSize %s ", d.DevSize.String()))
				log.Info(fmt.Sprintf("pvSize %s ", d.DevSize.String()))

				if d.DevSize.Value()-d.PVSize.Value() > delta {
					log.Info("create event " + EventActionResizing)
					err = CreateEventLVMVolumeGroup(ctx, cl, EventReasonResizing, EventActionResizing, nodeName, group)
					if err != nil {
						log.Error(err, " error CreateEventLVMVolumeGroup")
					}
					command, err := utils.ResizePV(d.Path)
					log.Debug(command)
					if err != nil {
						log.Error(errors.New("check size error"), "devSize <= pvSize")
						err = updateLVMVolumeGroupStatus(ctx, cl, group.Name, group.Namespace, "devSize <= pvSize", NoOperational)
						if err != nil {
							log.Error(err, fmt.Sprintf("error update LVMVolumeGroup %s", group.Name))
						}
						return
					}
				}
			}
		}

		if group.Spec.ThinPools != nil {

			log.Info("reconcile thin pool")

			log.Info("create event " + EventActionResizing)
			err = CreateEventLVMVolumeGroup(ctx, cl, EventReasonResizing, EventActionResizing, nodeName, group)
			if err != nil {
				log.Error(err, " error CreateEventLVMVolumeGroup")
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
					log.Error(errors.New("resize thin pool"), "add size value <= 0")
					return
				}

				log.Debug("-----------------------------------")
				log.Debug("vgSizeGb " + group.Status.VGSize.String())
				log.Debug("allocatedSizeGb " + group.Status.AllocatedSize.String())

				log.Debug("specPoolSize " + group.Status.VGSize.String())
				log.Debug("statusPoolActualSize " + statusPoolActualSize.String())

				log.Debug("freeSpace " + strconv.FormatInt(freeSpace, 10))
				log.Debug("addSize " + strconv.FormatInt(addSize, 10))
				log.Debug("------------------------------------")

				if freeSpace > addSize {
					newLVSizeStr := strconv.FormatInt(pool.Size.Value()/1024, 10)
					cmd, err := utils.ExtendLV(newLVSizeStr+"K", group.Spec.ActualVGNameOnTheNode, pool.Name)
					log.Debug(cmd)
					if err != nil {
						log.Error(err, "error ExtendLV")
						return
					}
				} else {
					log.Error(errors.New("ThinPools resize"), "addSize > freeSize")
				}
			}
		}

	} else {
		log.Info("create event " + EventActionCreating)
		err = CreateEventLVMVolumeGroup(ctx, cl, EventReasonCreating, EventActionCreating, nodeName, group)
		if err != nil {
			log.Error(err, " error CreateEventLVMVolumeGroup")
		}

		err := CreateVGComplex(ctx, cl, group, log)
		if err != nil {
			log.Error(err, "CreateVGComplex")
			err = updateLVMVolumeGroupStatus(ctx, cl, group.Name, group.Namespace, err.Error(), NoOperational)
			return
		}

		if len(group.Spec.ThinPools) != 0 {
			for _, v := range group.Spec.ThinPools {
				command, err := utils.CreateLV(v, group.Spec.ActualVGNameOnTheNode)
				log.Debug(command)
				if err != nil {
					log.Error(err, "error CreateLV")
					err = updateLVMVolumeGroupStatus(ctx, cl, group.Name, group.Namespace, err.Error(), NoOperational)
					return
				}
			}
		}
	}

	log.Info("reconcile loop end")
	err = updateLVMVolumeGroupStatus(ctx, cl, group.Name, group.Namespace, "", Operational)
	if err != nil {
		log.Error(err, fmt.Sprintf("error update LVMVolumeGroup %s", group.Name))
		return
	}

	log.Info("create event " + EventActionReady)
	err = CreateEventLVMVolumeGroup(ctx, cl, EventReasonReady, EventActionReady, nodeName, group)
	if err != nil {
		log.Error(err, " error CreateEventLVMVolumeGroup")
	}
}
