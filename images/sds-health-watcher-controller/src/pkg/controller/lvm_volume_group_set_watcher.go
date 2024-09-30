package controller

import (
	"context"
	"fmt"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"reflect"
	"sds-health-watcher-controller/config"
	"sds-health-watcher-controller/internal"
	"sds-health-watcher-controller/pkg/logger"
	"sds-health-watcher-controller/pkg/monitoring"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"time"
)

const (
	LVMVolumeGroupSetCtrlName = "lvm-volume-group-set-watcher-controller"

	phaseCreated    = "Created"
	phaseNotCreated = "NotCreated"

	reasonWorkInProgress = "WorkInProgress"
	strategyPerNode      = "PerNode"
)

func RunLVMVolumeGroupSetWatcher(
	mgr manager.Manager,
	log logger.Logger,
	cfg config.Options,
	metrics monitoring.Metrics,
) error {
	cl := mgr.GetClient()

	c, err := controller.New(LVMVolumeGroupSetCtrlName, mgr, controller.Options{
		Reconciler: reconcile.Func(func(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
			log.Info(fmt.Sprintf("[RunLVMVolumeGroupSetWatcher] tries to reconcile the request of the LVMVolumeGroupSet %s", request.Name))
			lvgSet := &v1alpha1.LVMVolumeGroupSet{}
			err := cl.Get(ctx, request.NamespacedName, lvgSet)
			if err != nil {
				if errors.IsNotFound(err) {
					log.Warning(fmt.Sprintf("[RunLVMVolumeGroupSetWatcher] seems like the LVMVolumeGroupSet %s has been deleted. Stop the reconcile", lvgSet.Name))
					return reconcile.Result{}, nil
				}

				log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupSetWatcher] unable to get the LVMVolumeGroupSet %s", request.Name))
				return reconcile.Result{}, err
			}

			shouldRequeue, err := reconcileLVMVolumeGroupSet(ctx, cl, log, metrics, lvgSet)
			if err != nil {
				log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupSetWatcher] unable to reconcile the LVMVolumeGroupSet %s", lvgSet.Name))
				return reconcile.Result{}, err
			}

			if shouldRequeue {
				log.Warning(fmt.Sprintf("[RunLVMVolumeGroupSetWatcher] the LVMVolumeGroupSet %s request should be requeued in %s", lvgSet.Name, cfg.ScanIntervalSec.String()))
				return reconcile.Result{RequeueAfter: cfg.ScanIntervalSec}, nil
			}

			log.Info(fmt.Sprintf("[RunLVMVolumeGroupSetWatcher] successfully reconciled the request of the LVMVolumeGroupSet %s", request.Name))
			return reconcile.Result{}, nil
		}),
	})
	if err != nil {
		log.Error(err, "[RunLVMVolumeGroupSetWatcher] unable to create the controller")
		return err
	}

	err = c.Watch(source.Kind(mgr.GetCache(), &v1alpha1.LVMVolumeGroupSet{}, handler.TypedFuncs[*v1alpha1.LVMVolumeGroupSet, reconcile.Request]{
		CreateFunc: func(ctx context.Context, e event.TypedCreateEvent[*v1alpha1.LVMVolumeGroupSet], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			log.Info(fmt.Sprintf("[RunLVMVolumeGroupSetWatcher] createFunc got a create event for the LVMVolumeGroupSet, name: %s", e.Object.GetName()))
			request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: e.Object.GetNamespace(), Name: e.Object.GetName()}}
			q.Add(request)
			log.Info(fmt.Sprintf("[RunLVMVolumeGroupSetWatcher] createFunc added a request for the LVMVolumeGroupSet %s to the Reconcilers queue", e.Object.GetName()))
		},
		UpdateFunc: func(ctx context.Context, e event.TypedUpdateEvent[*v1alpha1.LVMVolumeGroupSet], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			log.Info(fmt.Sprintf("[RunLVMVolumeGroupSetWatcher] UpdateFunc got a update event for the LVMVolumeGroupSet %s", e.ObjectNew.GetName()))
			if !shouldLVGSetWatcherReconcileUpdateEvent(e.ObjectOld, e.ObjectNew) {
				log.Info(fmt.Sprintf("[RunLVMVolumeGroupSetWatcher] update event for the LVMVolumeGroupSet %s should not be reconciled as not target changed were made", e.ObjectNew.Name))
				return
			}

			request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: e.ObjectNew.GetNamespace(), Name: e.ObjectNew.GetName()}}
			q.Add(request)

			log.Info(fmt.Sprintf("[RunLVMVolumeGroupSetWatcher] updateFunc added a request for the LVMVolumeGroupSet %s to the Reconcilers queue", e.ObjectNew.Name))
		},
	}))
	if err != nil {
		log.Error(err, "[RunLVMVolumeGroupSetWatcher] the controller is unable to watch the LVMVolumeGroupSet resources")
		return err
	}

	return nil
}

func shouldLVGSetWatcherReconcileUpdateEvent(old, new *v1alpha1.LVMVolumeGroupSet) bool {
	return !reflect.DeepEqual(old.Spec, new.Spec)
}

func reconcileLVMVolumeGroupSet(ctx context.Context, cl client.Client, log logger.Logger, metrics monitoring.Metrics, lvgSet *v1alpha1.LVMVolumeGroupSet) (bool, error) {
	log.Debug(fmt.Sprintf("[reconcileLVMVolumeGroupSet] starts the reconciliation of the LVMVolumeGroupSet %s", lvgSet.Name))
	err := updateLVMVolumeGroupSetPhaseIfNeeded(ctx, cl, log, lvgSet, internal.PhasePending, reasonWorkInProgress)
	if err != nil {
		return false, err
	}

	nodes, err := GetNodes(ctx, cl, metrics, lvgSet.Spec.NodeSelector)
	if err != nil {
		return false, err
	}

	valid, reason := validateLVMVolumeGroupSetNodes(nodes)
	if !valid {
		err = updateLVMVolumeGroupSetPhaseIfNeeded(ctx, cl, log, lvgSet, phaseNotCreated, reason)
		if err != nil {
			return false, err
		}

		return true, nil
	}

	log.Debug(fmt.Sprintf("[reconcileLVMVolumeGroupSet] tries to provide LVMVolumeGroups by the LVMVolumeGroupSet %s", lvgSet.Name))
	err = provideLVMVolumeGroupsBySet(ctx, cl, log, metrics, lvgSet, nodes)
	if err != nil {
		log.Error(err, fmt.Sprintf("[reconcileLVMVolumeGroupSet] unable to provide LVMVolumeGroups by LVMVolumeGroupSet %s", lvgSet.Name))
		updErr := updateLVMVolumeGroupSetPhaseIfNeeded(ctx, cl, log, lvgSet, phaseNotCreated, reason)
		if updErr != nil {
			return false, updErr
		}
		return false, err
	}

	log.Debug(fmt.Sprintf("[reconcileLVMVolumeGroupSet] successfully provided LVMVolumeGroups by the LVMVolumeGroupSet %s", lvgSet.Name))
	err = updateLVMVolumeGroupSetPhaseIfNeeded(ctx, cl, log, lvgSet, phaseCreated, "")
	if err != nil {
		return false, err
	}

	log.Debug(fmt.Sprintf("[reconcileLVMVolumeGroupSet] tries to update the LVMVolumeGroupSet %s status"))
	return false, nil
}

func provideLVMVolumeGroupsBySet(ctx context.Context, cl client.Client, log logger.Logger, metrics monitoring.Metrics, lvgSet *v1alpha1.LVMVolumeGroupSet, nodes map[string]v1.Node) error {
	switch lvgSet.Spec.Strategy {
	case strategyPerNode:
		log.Debug(fmt.Sprintf("[provideLVMVolumeGroupsBySet] the LVMVolumeGroupSet %s has strategy %s, tries to provide the LVMVolumeGroups", lvgSet.Name, strategyPerNode))
		err := provideLVMVolumeGroupsBySetPerNode(ctx, cl, log, metrics, lvgSet, nodes)
		if err != nil {
			log.Error(err, fmt.Sprintf("[provideLVMVolumeGroupsBySet] unable to provide LVMVolumeGroups by the LVMVolumeGroupSet %s with strategy %s", lvgSet.Name, strategyPerNode))
			return err
		}
		log.Debug(fmt.Sprintf("[provideLVMVolumeGroupsBySet] successfully provided LVMVolumeGroups by the LVMVolumeGroupSet %s with strategy %s", lvgSet.Name, strategyPerNode))
	}

	return nil
}

func provideLVMVolumeGroupsBySetPerNode(ctx context.Context, cl client.Client, log logger.Logger, metrics monitoring.Metrics, lvgSet *v1alpha1.LVMVolumeGroupSet, nodes map[string]v1.Node) error {
	currentLVGs, err := GetLVMVolumeGroups(ctx, cl, metrics)
	if err != nil {
		log.Error(err, "[provideLVMVolumeGroupsBySetPerNode] unable to get LVMVolumeGroups")
		return err
	}
	for _, n := range nodes {
		lvg := configureLVGBySet(lvgSet, n)
		log.Debug(fmt.Sprintf("[provideLVMVolumeGroupsBySetPerNode] tries to create the LVMVolumeGroup %s", lvg.Name))
		created, err := createLVMVolumeGroupIfNeeded(ctx, cl, log, lvg, currentLVGs)
		if err != nil {
			log.Error(err, fmt.Sprintf("[provideLVMVolumeGroupsBySetPerNode] unable to create the LVMVolumeGroup %s", lvg.Name))
			return err
		}

		if !created {
			continue
		}

		log.Debug(fmt.Sprintf("[provideLVMVolumeGroupsBySetPerNode] the LVMVolumeGroup %s has been created", lvg.Name))
		log.Debug(fmt.Sprintf("[provideLVMVolumeGroupsBySetPerNode] tries to update the LVMVolumeGroupSet %s status by the created LVMVolumeGroup %s", lvgSet.Name, lvg.Name))
		err = updateLVMVolumeGroupSetStatusWithLVGIfNeeded(ctx, cl, log, lvgSet, lvg, nodes)
		if err != nil {
			log.Error(err, fmt.Sprintf("[provideLVMVolumeGroupsBySetPerNode] unable to update the LVMVolumeGroupSet %s", lvgSet.Name))
			return err
		}
		log.Debug(fmt.Sprintf("[provideLVMVolumeGroupsBySetPerNode] successfully updated the LVMVolumeGroupSet %s status by the created LVMVolumeGroup %s", lvgSet.Name, lvg.Name))
	}

	return nil
}

func updateLVMVolumeGroupSetStatusWithLVGIfNeeded(ctx context.Context, cl client.Client, log logger.Logger, lvgSet *v1alpha1.LVMVolumeGroupSet, lvg *v1alpha1.LVMVolumeGroup, nodes map[string]v1.Node) error {
	for _, createdLVG := range lvgSet.Status.CreatedLVGs {
		if createdLVG.LVMVolumeGroupName == lvg.Name {
			log.Debug(fmt.Sprintf("[updateLVMVolumeGroupSetStatusWithLVGIfNeeded] no need to update the LVMVolumeGroupSet status %s with the LVMVolumeGroup %s", lvgSet.Name, lvg.Name))
			return nil
		}
	}

	if cap(lvgSet.Status.CreatedLVGs) == 0 {
		lvgSet.Status.CreatedLVGs = make([]v1alpha1.LVMVolumeGroupSetStatusLVG, 0, len(nodes))
	}

	lvgSet.Status.CreatedLVGs = append(lvgSet.Status.CreatedLVGs, v1alpha1.LVMVolumeGroupSetStatusLVG{
		LVMVolumeGroupName: lvg.Name,
		NodeName:           lvg.Spec.Local.NodeName,
	})

	lvgSet.Status.LVGCount = len(lvgSet.Status.CreatedLVGs)

	return cl.Status().Update(ctx, lvgSet)
}

func createLVMVolumeGroupIfNeeded(ctx context.Context, cl client.Client, log logger.Logger, lvg *v1alpha1.LVMVolumeGroup, lvgs map[string]v1alpha1.LVMVolumeGroup) (bool, error) {
	if _, exist := lvgs[lvg.Name]; exist {
		log.Warning(fmt.Sprintf("[createLVMVolumeGroupIfNeeded] the LVMVolumeGroup %s has been already created", lvg.Name))
		return false, nil
	}

	for _, l := range lvgs {
		if l.Spec.Local.NodeName == lvg.Spec.Local.NodeName && l.Spec.ActualVGNameOnTheNode == lvg.Spec.ActualVGNameOnTheNode {
			log.Warning(fmt.Sprintf("[createLVMVolumeGroupIfNeeded] the LVMVolumeGroup %s has been already created", lvg.Name))
			return false, nil
		}
	}

	return true, cl.Create(ctx, lvg)
}

func configureLVGBySet(lvgSet *v1alpha1.LVMVolumeGroupSet, node v1.Node) *v1alpha1.LVMVolumeGroup {
	return &v1alpha1.LVMVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:   configureLVGNameFromSet(lvgSet),
			Labels: lvgSet.Spec.LVGTemplate.Metadata.Labels,
		},
		Spec: v1alpha1.LVMVolumeGroupSpec{
			ActualVGNameOnTheNode: lvgSet.Spec.LVGTemplate.ActualVGNameOnTheNode,
			BlockDeviceSelector:   lvgSet.Spec.LVGTemplate.BlockDeviceSelector,
			ThinPools:             lvgSet.Spec.LVGTemplate.ThinPools,
			Type:                  lvgSet.Spec.LVGTemplate.Type,
			Local: v1alpha1.LVMVolumeGroupLocalSpec{
				NodeName: node.Name,
			},
		},
	}
}

func configureLVGNameFromSet(lvgSet *v1alpha1.LVMVolumeGroupSet) string {
	return fmt.Sprintf("%s-%d", lvgSet.Name, len(lvgSet.Status.CreatedLVGs)+1)
}

func GetNodes(ctx context.Context, cl client.Client, metrics monitoring.Metrics, selector *metav1.LabelSelector) (map[string]v1.Node, error) {
	list := &v1.NodeList{}
	s, err := metav1.LabelSelectorAsSelector(selector)
	if err != nil {
		return nil, err
	}
	if s == labels.Nothing() {
		s = nil
	}
	start := time.Now()
	err = cl.List(ctx, list, &client.ListOptions{LabelSelector: s})
	metrics.APIMethodsDuration(LVMVolumeGroupSetCtrlName, "list").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.APIMethodsExecutionCount(LVMVolumeGroupSetCtrlName, "list").Inc()
	if err != nil {
		metrics.APIMethodsErrors(LVMVolumeGroupSetCtrlName, "list").Inc()
		return nil, err
	}

	result := make(map[string]v1.Node, len(list.Items))
	for _, item := range list.Items {
		result[item.Name] = item
	}

	return result, nil
}

func updateLVMVolumeGroupSetPhaseIfNeeded(ctx context.Context, cl client.Client, log logger.Logger, lvgSet *v1alpha1.LVMVolumeGroupSet, phase, reason string) error {
	log.Debug(fmt.Sprintf("[updateLVMVolumeGroupSetPhaseIfNeeded] tries to update the LVMVolumeGroupSet %s status phase to %s and reason to %s", lvgSet, phase, reason))
	if lvgSet.Status.Phase == phase && lvgSet.Status.Reason == reason {
		log.Debug(fmt.Sprintf("[updateLVMVolumeGroupSetPhaseIfNeeded] no need to update phase or reason of the LVMVolumeGroupSet %s as they are same", lvgSet.Name))
		return nil
	}

	log.Debug(fmt.Sprintf("[updateLVMVolumeGroupSetPhaseIfNeeded] the LVMVolumeGroupSet %s status phase %s and reason %s should be updated to the phase %s and reason %s", lvgSet.Name, lvgSet.Status.Phase, lvgSet.Status.Reason, phase, reason))
	lvgSet.Status.Phase = phase
	lvgSet.Status.Reason = reason
	err := cl.Status().Update(ctx, lvgSet)
	if err != nil {
		log.Error(err, fmt.Sprintf("[updateLVMVolumeGroupSetPhaseIfNeeded] unable to update the LVMVolumeGroupSet %s", lvgSet.Name))
		return err
	}

	log.Debug(fmt.Sprintf("[updateLVMVolumeGroupSetPhaseIfNeeded] successfully updated the LVMVolumeGroupSet %s to phase %s and reason %s", lvgSet.Name, phase, reason))
	return nil
}

func validateLVMVolumeGroupSetNodes(nodes map[string]v1.Node) (bool, string) {
	if len(nodes) == 0 {
		return false, "no nodes found by specified nodeSelector"
	}

	return true, ""
}
