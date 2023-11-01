package controller

import (
	"context"
	"fmt"
	"reflect"
	"storage-configurator/api/v1alpha1"
	"storage-configurator/pkg/log"
	"storage-configurator/pkg/utils"
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
		ReconcileLVMVG(ctx, e.Object.GetName(), e.Object.GetNamespace(), nodeName, log, cl)
	}
	updateFunc := func(ctx context.Context, updateEvent event.UpdateEvent, limitingInterface workqueue.RateLimitingInterface) {

		newLVG, ok := updateEvent.ObjectNew.(*v1alpha1.LvmVolumeGroup)
		if !ok {
			log.Error(err, "error get  ObjectNew LinstorStoragePool")
		}

		oldLVG, ok := updateEvent.ObjectOld.(*v1alpha1.LvmVolumeGroup)
		if !ok {
			log.Error(err, "error get  ObjectOld LinstorStoragePool")
		}

		if !reflect.DeepEqual(oldLVG.Status, newLVG.Status) {
			return
		}

		ReconcileLVMVG(ctx, updateEvent.ObjectNew.GetName(), updateEvent.ObjectNew.GetNamespace(), nodeName, log, cl)
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
	log.Info("event create LVMVolumeGroup " + objectName)
	group, err := getLVMVolumeGroup(ctx, cl, objectNameSpace, objectName)
	if err != nil {
		log.Error(err, "error getLVMVolumeGroup")
		return
	}
	validation, status, err := ValidationLVMGroup(ctx, cl, group, objectNameSpace, nodeName)

	if len(status.Health) != 0 {
		group.Status.Health = status.Health
		group.Status.Message = err.Error()
		err = updateLVMVolumeGroup(ctx, cl, group)
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

	///-----------------

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
		log.Info("validation operation and ")
		extendPVs, shrinkPVs, err := ValidationTypeLVMGroup(ctx, cl, group, log)
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

		if len(shrinkPVs) != 0 {
			log.Info("create event " + EventActionShrinking)
			err = CreateEventLVMVolumeGroup(ctx, cl, EventReasonShrinking, EventActionShrinking, nodeName, group)
			if err != nil {
				log.Error(err, " error CreateEventLVMVolumeGroup")
			}

			group.Status.Health = NoOperational
			err = updateLVMVolumeGroup(ctx, cl, group)
			if err != nil {
				log.Error(err, fmt.Sprintf("error update LVMVolumeGroup %s", group.Name))
				return
			}

			// Deleting PV from VG
			for _, pvPath := range shrinkPVs {
				command, err := utils.RemovePVFromVG(pvPath, group.Spec.ActualVGNameOnTheNode)
				log.Debug(command)
				if err != nil {
					log.Error(err, "RemovePVFromVG")
				}
			}

			group.Status.Health = Operational
			err = updateLVMVolumeGroup(ctx, cl, group)
			if err != nil {
				log.Error(err, fmt.Sprintf("error update LVMVolumeGroup %s", group.Name))
				return
			}
		}

		// Check Resize
		for _, n := range group.Status.Nodes {
			for _, d := range n.Devices {
				if d.PVSize < d.DevSize {
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
					group.Status.Health = NoOperational
					err = updateLVMVolumeGroup(ctx, cl, group)
					if err != nil {
						log.Error(err, fmt.Sprintf("error update LVMVolumeGroup %s", group.Name))
						return
					}
				}
			}
		}

		if group.Spec.ThinPools != nil {
			//TODO after BlockDevice controller update
			//for _, pool := range group.Spec.ThinPools {
			//pool.Size >    && pool.Size < group.Status.VGSize

			//lvs, command, err := utils.GetAllLVs()
			//for _, lv := range lvs {
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
					group.Status.Health = NoOperational
					err = updateLVMVolumeGroup(ctx, cl, group)
					log.Error(err, "error CreateLV")
					return
				}
			}
		}
	}

	group.Status.Health = Operational
	err = updateLVMVolumeGroup(ctx, cl, group)
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
