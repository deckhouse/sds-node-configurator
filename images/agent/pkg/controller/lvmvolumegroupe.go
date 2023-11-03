package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"storage-configurator/api/v1alpha1"
	"storage-configurator/pkg/log"
	"storage-configurator/pkg/utils"
	"strconv"
	"strings"

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

					if len(device.DevSize) == 0 {
						log.Error(errors.New("check dev size device.DevSize = "), device.DevSize)
						return
					}

					devSize, err := strconv.ParseFloat(device.DevSize[:len(device.DevSize)-1], 64)
					if err != nil {
						log.Error(err, "parse devise size")
						return
					}

					if len(device.PVSize) == 0 {
						log.Error(errors.New("check dev PV size device.PVSize = "), device.PVSize)
						return
					}

					if strings.HasPrefix(device.PVSize, "<") || strings.HasPrefix(device.PVSize, ">") {
						device.PVSize = device.PVSize[1:]
					}

					pvSize, err := strconv.ParseFloat(device.PVSize[:len(device.PVSize)-1], 64)
					if err != nil {
						log.Error(err, "parse pv size")
						return
					}

					if devSize != pvSize {
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

	if len(status.Health) != 0 {
		health := status.Health
		var message string
		if err != nil {
			message = err.Error()
		}

		log.Error(err, "ValidationLVMGroup")
		err = updateLVMVolumeGroupStatus(ctx, cl, group.Name, group.Namespace, message, health)
		//err = updateLVMVolumeGroup(ctx, cl, group)
		if err != nil {
			log.Error(err, fmt.Sprintf("error update LVMVolumeGroup %s", group.Name))
		}
	}

	if err != nil {
		log.Error(err, "ValidationLVMGroup failed")
		return
	}

	if validation == false {
		log.Info("Validation failed")
		return
	}

	log.Info("Validation passed")

	annotationMark := 0
	for k, _ := range group.Annotations {
		if strings.Contains(k, delAnnotation) {
			annotationMark++
		}
	}

	//ok := group.Annotations[delAnnotation]

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

				if len(d.DevSize) == 0 {
					log.Error(errors.New("check resize d.DevSize = "), d.DevSize)
					return
				}
				devSize, err := strconv.ParseFloat(d.DevSize[:len(d.DevSize)-1], 64)
				if err != nil {
					log.Error(err, "parse devise size")
					return
				}

				d.PVSize = d.PVSize[1:]
				pvSize, err := strconv.ParseFloat(d.PVSize[:len(d.PVSize)-1], 64)
				if err != nil {
					log.Error(err, "parse pv size")
					return
				}

				if d.PVSize == d.DevSize {
					return
				}

				log.Info("devSize changed")
				log.Info(fmt.Sprintf("devSize %f", devSize))
				log.Info(fmt.Sprintf("pvSize %f", pvSize))

				if devSize > pvSize {
					log.Info("create event " + EventActionResizing)
					err = CreateEventLVMVolumeGroup(ctx, cl, EventReasonResizing, EventActionResizing, nodeName, group)
					if err != nil {
						log.Error(err, " error CreateEventLVMVolumeGroup")
					}
					command, err := utils.ResizePV(d.Path)
					log.Debug(command)
					if err != nil {
						log.Error(err, "ResizePV")
					}
				} else {
					//group.Status.Health = NoOperational
					log.Error(errors.New("check size error"), "devSize < pvSize")
					err = updateLVMVolumeGroupStatus(ctx, cl, group.Name, group.Namespace, "devSize < pvSize", NoOperational)

					//err = updateLVMVolumeGroup(ctx, cl, group)
					if err != nil {
						log.Error(err, fmt.Sprintf("error update LVMVolumeGroup %s", group.Name))
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

			statusThinPoolMap := make(map[string]string)
			for _, statusThinPool := range group.Status.ThinPools {
				statusThinPoolMap[statusThinPool.Name] = statusThinPool.ActualSize
			}

			// VG size
			vgSizeGb, err := strconv.ParseFloat(group.Status.VGSize[:len(group.Status.VGSize)-1], 64)
			if err != nil {
				log.Error(err, "parse spec VGSize Kb => Gb")
				return
			}
			vgSizeGb = vgSizeGb / 1048576

			// VG allocatedSize
			allocatedSizeGb, err := strconv.ParseFloat(group.Status.AllocatedSize[:len(group.Status.AllocatedSize)-1], 64)
			if err != nil {
				log.Error(err, "parse spec AllocatedSize M => Gb")
				return
			}
			allocatedSizeGb = allocatedSizeGb / 1024

			for _, pool := range group.Spec.ThinPools {

				specPoolSize, err := strconv.ParseFloat(pool.Size[:len(pool.Size)-1], 64)
				if err != nil {
					log.Error(err, "parse spec size thin pool Gb")
					return
				}

				statusPoolActualSize, err := strconv.ParseFloat(statusThinPoolMap[pool.Name][:len(statusThinPoolMap[pool.Name])-1], 64)
				if err != nil {
					log.Error(err, "parse status size actual thin pool Kb => Gb")
					return
				}

				statusPoolActualSize = statusPoolActualSize / 1048576

				freeSpace := vgSizeGb - allocatedSizeGb
				addSize := specPoolSize - statusPoolActualSize
				if addSize < 0.0 {
					log.Error(errors.New("resize thin pool"), "add size value < 0")
					return
				}

				if freeSpace > addSize {
					newLVSizeStr := fmt.Sprintf("%fg", specPoolSize)
					cmd, err := utils.ExtendLV(newLVSizeStr, group.Spec.ActualVGNameOnTheNode, pool.Name)
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
