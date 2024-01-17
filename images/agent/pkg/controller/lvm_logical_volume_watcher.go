package controller

import (
	"context"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sds-node-configurator/api/v1alpha1"
	"sds-node-configurator/config"
	"sds-node-configurator/pkg/logger"
	"sds-node-configurator/pkg/monitoring"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"time"
)

const (
	LVMLogicalVolumeWatcherCtrlName = "lvm-logical-volume-watcher-controller"
)

func RunLVMLogicalVolumeWatcherController(
	mgr manager.Manager,
	cfg config.Options,
	log logger.Logger,
	metrics monitoring.Metrics,
) (controller.Controller, error) {
	//cl := mgr.GetClient()
	cache := mgr.GetCache()

	c, err := controller.New(LVMLogicalVolumeWatcherCtrlName, mgr, controller.Options{
		Reconciler: reconcile.Func(func(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
			log.Info("[RunLVMLogicalVolumeWatcherController] HELLO FROM RECONCILER")
			return reconcile.Result{
				RequeueAfter: 5 * time.Second,
			}, nil
		}),
	})
	if err != nil {
		log.Error(err, "[RunLVMLogicalVolumeWatcherController] unable to create controller")
		return nil, err
	}

	// lvremove
	createFunc := func(ctx context.Context, e event.CreateEvent, q workqueue.RateLimitingInterface) {
		log.Info("[RunLVMLogicalVolumeWatcherController] Hello from CreateFunc")
		request := reconcile.Request{NamespacedName: types.NamespacedName{
			Namespace: e.Object.GetNamespace(),
			Name:      e.Object.GetName(),
		}}
		log.Info("NAME:" + request.Name)
		log.Info("NAMESPACE:" + request.Namespace)
		// Если создается ресурс llv, я
		// - должен получить информацию о том, где я должен создавать PV
		// - я проверяю возможность создания PV в указанном месте
		// - пытаюсь создать PV нужного размера
		// - если создаю, то обновляю статус ресурса успехом, проставляю актуальные данные
		// - если нет, кидаю ошибку и сообщение
	}

	err = c.Watch(source.Kind(cache, &v1alpha1.LvmLogicalVolume{}), handler.Funcs{
		CreateFunc: createFunc,
	})
	if err != nil {
		log.Error(err, "[RunLVMLogicalVolumeWatcherController] the controller is unable to watch")
		return nil, err
	}

	return c, err
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
	// - получаю данные для удаления из спеки ресурса
	// - выполняю удаления
	// - если удачно, то
}
