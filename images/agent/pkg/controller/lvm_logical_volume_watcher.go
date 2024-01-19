package controller

import (
	"context"
	"fmt"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sds-node-configurator/api/v1alpha1"
	"sds-node-configurator/config"
	"sds-node-configurator/pkg/logger"
	"sds-node-configurator/pkg/monitoring"
	"sds-node-configurator/pkg/utils"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const (
	LVMLogicalVolumeWatcherCtrlName            = "lvm-logical-volume-watcher-controller"
	CreatedStatusPhase                         = "Created"
	PendingStatusPhase                         = "Pending"
	ResizingStatusPhase                        = "Resizing"
	FailedStatusPhase                          = "Failed"
	Thick                           DeviceType = "Thick"
	Thin                            DeviceType = "Thin"
)

type DeviceType string

func RunLVMLogicalVolumeWatcherController(
	mgr manager.Manager,
	cfg config.Options,
	log logger.Logger,
	metrics monitoring.Metrics,
) (controller.Controller, error) {
	cl := mgr.GetClient()
	cache := mgr.GetCache()

	c, err := controller.New(LVMLogicalVolumeWatcherCtrlName, mgr, controller.Options{
		Reconciler: reconcile.Func(func(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
			return reconcile.Result{}, nil
		}),
	})
	if err != nil {
		log.Error(err, "[RunLVMLogicalVolumeWatcherController] unable to create controller")
		return nil, err
	}

	err = c.Watch(source.Kind(cache, &v1alpha1.LvmLogicalVolume{}), handler.Funcs{
		CreateFunc: func(ctx context.Context, e event.CreateEvent, q workqueue.RateLimitingInterface) {
			log.Info("[RunLVMLogicalVolumeWatcherController] Watcher.CreateFunc starts reconciliation")
			req := reconcile.Request{NamespacedName: types.NamespacedName{
				Namespace: e.Object.GetNamespace(),
				Name:      e.Object.GetName(),
			}}

			reconcileFunc(ctx, cl, log, metrics, req, cfg.NodeName)
			log.Info("[RunLVMLogicalVolumeWatcherController] Watcher.CreateFunc ends reconciliation")
		},
	})
	if err != nil {
		log.Error(err, "[RunLVMLogicalVolumeWatcherController] the controller is unable to watch")
		return nil, err
	}

	return c, err
}

func reconcileFunc(ctx context.Context, cl client.Client, log logger.Logger, metrics monitoring.Metrics, req reconcile.Request, nodeName string) {
	// Получаю ресурс
	log.Info("[reconcileFunc] starts reconciliation")
	log.Debug(fmt.Sprintf("[reconcileFunc] reconciles LVMLogicalVolume, name: %s", req.Name))
	llv, err := getLVMLogicalVolume(ctx, metrics, cl, req)
	if err != nil {
		log.Error(err, "[reconcileFunc] unable to getLVMLogicalVolume")
		return
	}

	lvg, err := getLVMVolumeGroup(ctx, cl, metrics, req.Namespace, llv.Spec.LvmVolumeGroup)
	if err != nil {
		log.Error(err, "[reconcileFunc] unable to getLVMVolumeGroup")
		return
	}

	if !belongsToNode(lvg, nodeName) {
		log.Debug(fmt.Sprintf("[reconcileFunc] the LVMVolumeGroup %s does not belongs to the current node: %s", lvg.Name, nodeName))
		return
	}
	log.Debug(fmt.Sprintf("[reconcileFunc] the LVMVolumeGroup %s belongs to the current node: %s", lvg.Name, nodeName))

	err = updateLVMLogicalVolumePhase(ctx, metrics, cl, llv, PendingStatusPhase, "")
	if err != nil {
		log.Error(err, "[reconcileFunc] unable to updateLVMLogicalVolumePhase")
	}
	log.Debug(fmt.Sprintf("[reconcileFunc] updated LVMLogicaVolume %s status.phase to %s", llv.Name, PendingStatusPhase))

	log.Debug(fmt.Sprintf("[reconcileFunc] the LVMLogicalVolume %s spec.thin.poolname: \"%s\"", llv.Name, llv.Spec.Thin.PoolName))
	// добавить проверки на наличие нужного кол-ва места
	switch getLVMLogicalVolumeType(llv) {
	case Thick:
		freeSpace, err := getFreeVGSpace(log, lvg)
		if err != nil {
			log.Error(err, "[reconcileFunc] unable to getFreeVGSpace")
		}

		if freeSpace.Value() < llv.Spec.Size.Value() {
			log.Warning(fmt.Sprintf("[reconcileFunc] not enough space left on the VG %s for the LVMLogicalVolume %s", lvg.Spec.ActualVGNameOnTheNode, llv.Name))
			return
		}

		cmd, err := utils.CreateThickLogicalVolume(lvg.Spec.ActualVGNameOnTheNode, llv.Name, llv.Spec.Size.String())
		log.Debug(fmt.Sprintf("[reconcileFunc] runs cmd: %s", cmd))
		if err != nil {
			log.Error(err, "[reconcileFunc] unable to CreateThickLogicalVolume")
			err = updateLVMLogicalVolumePhase(ctx, metrics, cl, llv, FailedStatusPhase, "Unable to create Thick LV")
			if err != nil {
				log.Error(err, "[reconcileFunc] unable to updateLVMLogicalVolumePhase")
			}
			return
		}
	case Thin:
		cmd, err := utils.CreateThinLogicalVolume(lvg.Spec.ActualVGNameOnTheNode, llv.Spec.Thin.PoolName, llv.Name, llv.Spec.Size.String())
		log.Debug(fmt.Sprintf("[reconcileFunc] runs cmd: %s", cmd))
		if err != nil {
			log.Error(err, "[reconcileFunc] unable to CreateThickLogicalVolume")
			err = updateLVMLogicalVolumePhase(ctx, metrics, cl, llv, FailedStatusPhase, "Unable to create Thin LV")
			if err != nil {
				log.Error(err, "[reconcileFunc] unable to updateLVMLogicalVolumePhase")
			}
			return
		}
	}

	//if llv.Spec.Thin.PoolName == "" {
	//	cmd, err := utils.CreateThickLogicalVolume(lvg.Spec.ActualVGNameOnTheNode, llv.Name, llv.Spec.Size.String())
	//	log.Debug(fmt.Sprintf("[reconcileFunc] runs cmd: %s", cmd))
	//	if err != nil {
	//		log.Error(err, "[reconcileFunc] unable to CreateThickLogicalVolume")
	//		llv.Status.Phase = FailedStatusPhase
	//		llv.Status.Reason = err.Error()
	//		return
	//	}
	//} else {
	//	cmd, err := utils.CreateThinLogicalVolume(lvg.Spec.ActualVGNameOnTheNode, llv.Spec.Thin.PoolName, llv.Name, llv.Spec.Size.String())
	//	log.Debug(fmt.Sprintf("[reconcileFunc] runs cmd: %s", cmd))
	//	if err != nil {
	//		log.Error(err, "[reconcileFunc] unable to CreateThickLogicalVolume")
	//		llv.Status.Phase = FailedStatusPhase
	//		llv.Status.Reason = err.Error()
	//		return
	//	}
	//}
	log.Debug(fmt.Sprintf("[reconcileFunc] successfully created Logical Volume for LVMLogicalVolume, name: %s", llv.Name))

	llv.Status.Phase = CreatedStatusPhase
	llv.Status.ActualSize = llv.Spec.Size
	err = updateLVMLogicalVolume(ctx, metrics, cl, llv)
	if err != nil {
		log.Error(err, "[reconcileFunc] unable to updateLVMLogicalVolume")
		return
	}

	log.Info("[reconcileFunc] ends reconciliation")
}

func getFreeVGSpace(log logger.Logger, lvg *v1alpha1.LvmVolumeGroup) (resource.Quantity, error) {
	vgs, cmd, _, err := utils.GetAllVGs()
	log.Debug(fmt.Sprintf("[getFreeVGSpace] runs cmd: %s", cmd))
	if err != nil {
		log.Error(err, "[getFreeVGSpace] unable to run cmd")
		return resource.Quantity{}, err
	}

	for _, vg := range vgs {
		if vg.VGName == lvg.Spec.ActualVGNameOnTheNode {
			return vg.VGFree, nil
		}
	}

	return resource.Quantity{}, nil
}

func getLVMLogicalVolumeType(llv *v1alpha1.LvmLogicalVolume) DeviceType {
	if llv.Spec.Thin.PoolName == "" {
		return Thick
	}

	return Thin
}

func belongsToNode(lvg *v1alpha1.LvmVolumeGroup, nodeName string) bool {
	var belongs bool
	for _, node := range lvg.Status.Nodes {
		if node.Name == nodeName {
			belongs = true
		}
	}

	return belongs
}

func updateLVMLogicalVolumePhase(ctx context.Context, metrics monitoring.Metrics, cl client.Client, llv *v1alpha1.LvmLogicalVolume, phase, reason string) error {
	llv.Status.Phase = phase
	llv.Status.Reason = reason

	err := updateLVMLogicalVolume(ctx, metrics, cl, llv)
	if err != nil {
		return err
	}

	return nil
}

func updateLVMLogicalVolume(ctx context.Context, metrics monitoring.Metrics, cl client.Client, llv *v1alpha1.LvmLogicalVolume) error {
	err := cl.Update(ctx, llv)
	if err != nil {
		return err
	}

	return nil
}

func getLVMLogicalVolume(ctx context.Context, metrics monitoring.Metrics, cl client.Client, req reconcile.Request) (*v1alpha1.LvmLogicalVolume, error) {
	llv := &v1alpha1.LvmLogicalVolume{}

	err := cl.Get(ctx, client.ObjectKey{
		Namespace: req.Namespace,
		Name:      req.Name,
	}, llv)
	if err != nil {
		return nil, err
	}

	return llv, err
}

func updateFunc() {
	// Если обновляется спека llv, я
	// - проверяю текущий статус ресурса
	// - если статус соответствует спеке, ничего не делаю
	// - если статус не соответствует спеке, тогда
	// - я проверяю возможность создания PV в указанном месте
	// - пытаюсь создать PV нужного размера
	// - если создаю, то обновляю статус ресурса успехом, проставляю актуальные данные
	// - если нет, кидаю ошибку и сообщение
}

func deleteFunc() {
	// Если удаляется ресурс llv, я
	// - удаляю llv
}
