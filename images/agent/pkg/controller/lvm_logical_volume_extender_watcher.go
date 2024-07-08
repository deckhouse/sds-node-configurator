package controller

import (
	"agent/pkg/logger"
	"context"
	"fmt"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"strconv"
	"strings"
)

const (
	LVMLogicalVolumeExtenderCtrlName = "lvm-logical-volume-extender-controller"
)

func RunLVMLogicalVolumeExtenderWatcherController(
	mgr manager.Manager,
	log logger.Logger,
) error {
	cl := mgr.GetClient()
	mgrCache := mgr.GetCache()

	c, err := controller.New(LVMLogicalVolumeExtenderCtrlName, mgr, controller.Options{
		Reconciler: reconcile.Func(func(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
			// Reconcile

			return reconcile.Result{}, nil
		}),
	})
	if err != nil {
		log.Error(err, "[RunLVMLogicalVolumeExtenderWatcherController] unable to create a controller")
		return err
	}

	err = c.Watch(source.Kind(mgrCache, &v1alpha1.LvmVolumeGroup{}), handler.Funcs{
		// мы можем упасть и не пошириться, поднимемся и будем шириться
		CreateFunc: nil,
		UpdateFunc: nil,
	})
	if err != nil {
		log.Error(err, "[RunLVMLogicalVolumeExtenderWatcherController] unable to watch the events")
		return err
	}

	return nil
}

func ReconcileLVMLogicalVolumeExtension(ctx context.Context, cl client.Client, log logger.Logger, lvg *v1alpha1.LVMLogicalVolume) error {
	llvs, err := getAllPercentLLVForLVG(ctx, cl, lvg.Name)
	if err != nil {
		log.Error(err, fmt.Sprintf("[ReconcileLVMLogicalVolumeExtension] unable to get LLV resources"))
		return err
	}

}

func resizeLVSpecSizeIfNeeded(llvs []v1alpha1.LVMLogicalVolume, lvg *v1alpha1.LvmVolumeGroup) error {
	for _, llv := range llvs {

		var newSize int64
		switch llv.Spec.Type {
		case Thin:
			for _, tp := range lvg.Status.ThinPools {
				if tp.Name == llv.Spec.Thin.PoolName {
					//"переводим проценты в байты"
					strPercent := strings.Split(llv.Spec.Size.String(), "%")[0]
					percent, err := strconv.Atoi(strPercent)
					if err != nil {
						return err
					}

					newSize = tp.ActualSize.Value() * int64(percent) / 100
					break
				}
			}
		case Thick:
			//"переводим проценты в байты"
			strPercent := strings.Split(llv.Spec.Size.String(), "%")[0]
			percent, err := strconv.Atoi(strPercent)
			if err != nil {
				return err
			}

			newSize = lvg.Status.VGSize.Value() * int64(percent) / 100
			break
		}

		if llv.Status.ActualSize.Value() < newSize {
			// extendLV

		}

	}
}

func getAllPercentLLVForLVG(ctx context.Context, cl client.Client, lvgName string) ([]v1alpha1.LVMLogicalVolume, error) {
	llvList := &v1alpha1.LVMLogicalVolumeList{}
	err := cl.List(ctx, llvList)
	if err != nil {
		return nil, err
	}

	result := make([]v1alpha1.LVMLogicalVolume, len(llvList.Items))
	for _, llv := range llvList.Items {
		if llv.Spec.LvmVolumeGroupName == lvgName && hasLLVPercentSize(&llv) {
			result = append(result, llv)
		}
	}

	return result, nil
}

func hasLLVPercentSize(llv *v1alpha1.LVMLogicalVolume) bool {
	return strings.Contains(llv.Spec.Size, "%")
}
