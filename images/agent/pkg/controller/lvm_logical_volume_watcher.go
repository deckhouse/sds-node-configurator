package controller

import (
	"context"
	"sds-node-configurator/api/v1alpha1"
	"sds-node-configurator/config"
	"sds-node-configurator/pkg/logger"
	"sds-node-configurator/pkg/monitoring"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const (
	LVMLogicalVolumeWatcherCtrlName = "lvm-logical-volume-watcher-controller"
)

func RunLVMLogicalVolumeController(
	mgr manager.Manager,
	cfg config.Options,
	log logger.Logger,
	metrics monitoring.Metrics,
) (controller.Controller, error) {
	//cl := mgr.GetClient()
	cache := mgr.GetCache()

	c, err := controller.New(LVMLogicalVolumeWatcherCtrlName, mgr, controller.Options{
		Reconciler: reconcile.Func(func(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
			return reconcile.Result{}, nil
		}),
	})
	if err != nil {
		log.Error(err, "[RunLVMLogicalVolumeController] unable to create controller")
		return nil, err
	}

	err = c.Watch(source.Kind(cache, &v1alpha1.LvmLogicalVolume{}), handler.Funcs{
		CreateFunc:  nil,
		UpdateFunc:  nil,
		DeleteFunc:  nil,
		GenericFunc: nil,
	})
	if err != nil {
		log.Error(err, "[RunLVMLogicalVolumeController] the controller is unable to watch")
		return nil, err
	}

	return c, err
}
