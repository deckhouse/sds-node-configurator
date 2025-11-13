/*
Copyright 2025 Flant JSC

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
	"fmt"
	"reflect"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/sds-health-watcher-controller/config"
	"github.com/deckhouse/sds-node-configurator/images/sds-health-watcher-controller/pkg/logger"
	"github.com/deckhouse/sds-node-configurator/images/sds-health-watcher-controller/pkg/monitoring"
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
	log = log.WithName("RunLVMVolumeGroupSetWatcher")
	cl := mgr.GetClient()

	c, err := controller.New(LVMVolumeGroupSetCtrlName, mgr, controller.Options{
		Reconciler: reconcile.Func(func(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
			log := log.WithName("Reconcile").WithValues("lvgSetName", request.Name)
			log.Info("tries to reconcile the request of the LVMVolumeGroupSet")
			lvgSet := &v1alpha1.LVMVolumeGroupSet{}
			err := cl.Get(ctx, request.NamespacedName, lvgSet)
			if err != nil {
				if errors.IsNotFound(err) {
					log.Warning("seems like the LVMVolumeGroupSet has been deleted. Stop the reconcile")
					return reconcile.Result{}, nil
				}

				log.Error(err, "unable to get the LVMVolumeGroupSet")
				return reconcile.Result{}, err
			}

			log = log.WithValues("lvgSetName", lvgSet.Name)
			shouldRequeue, err := reconcileLVMVolumeGroupSet(ctx, cl, log, metrics, lvgSet)
			if err != nil {
				log.Error(err, "unable to reconcile the LVMVolumeGroupSet")
				return reconcile.Result{}, err
			}

			if shouldRequeue {
				log.Warning("the LVMVolumeGroupSet request should be requeued", "requeueIn", cfg.ScanIntervalSec)
				return reconcile.Result{RequeueAfter: cfg.ScanIntervalSec}, nil
			}

			log.Info("successfully reconciled the request of the LVMVolumeGroupSet")
			return reconcile.Result{}, nil
		}),
	})
	if err != nil {
		log.Error(err, "unable to create the controller")
		return err
	}

	err = c.Watch(source.Kind(mgr.GetCache(), &v1alpha1.LVMVolumeGroupSet{}, handler.TypedFuncs[*v1alpha1.LVMVolumeGroupSet, reconcile.Request]{
		CreateFunc: func(_ context.Context, e event.TypedCreateEvent[*v1alpha1.LVMVolumeGroupSet], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			log := log.WithName("CreateFunc").WithValues("lvgSetName", e.Object.GetName())
			log.Info("createFunc got a create event for the LVMVolumeGroupSet")
			request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: e.Object.GetNamespace(), Name: e.Object.GetName()}}
			q.Add(request)
			log.Info("createFunc added a request for the LVMVolumeGroupSet to the Reconcilers queue")
		},
		UpdateFunc: func(_ context.Context, e event.TypedUpdateEvent[*v1alpha1.LVMVolumeGroupSet], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			log := log.WithName("UpdateFunc").WithValues("lvgSetName", e.ObjectNew.GetName())
			log.Info("UpdateFunc got an update event for the LVMVolumeGroupSet")
			if !shouldLVGSetWatcherReconcileUpdateEvent(e.ObjectOld, e.ObjectNew) {
				log.Info("update event for the LVMVolumeGroupSet should not be reconciled as not target changed were made")
				return
			}

			request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: e.ObjectNew.GetNamespace(), Name: e.ObjectNew.GetName()}}
			q.Add(request)
			log.Info("updateFunc added a request for the LVMVolumeGroupSet to the Reconcilers queue")
		},
	}))
	if err != nil {
		log.Error(err, "the controller is unable to watch the LVMVolumeGroupSet resources")
		return err
	}

	return nil
}

func shouldLVGSetWatcherReconcileUpdateEvent(oldLVG, newLVG *v1alpha1.LVMVolumeGroupSet) bool {
	return !reflect.DeepEqual(oldLVG.Spec, newLVG.Spec)
}

func reconcileLVMVolumeGroupSet(ctx context.Context, cl client.Client, log logger.Logger, metrics monitoring.Metrics, lvgSet *v1alpha1.LVMVolumeGroupSet) (bool, error) {
	log = log.WithName("reconcileLVMVolumeGroupSet")
	log.Debug("starts the reconciliation of the LVMVolumeGroupSet")
	err := updateLVMVolumeGroupSetPhaseIfNeeded(ctx, cl, log, lvgSet, v1alpha1.PhasePending, reasonWorkInProgress)
	if err != nil {
		return false, err
	}

	log.Debug("tries to get nodes by the LVMVolumeGroupSet nodeSelector")
	nodes, err := GetNodes(ctx, cl, metrics, lvgSet.Spec.NodeSelector)
	if err != nil {
		return false, err
	}
	log.Debug("successfully got nodes by the LVMVolumeGroupSet nodeSelector")
	log.Trace("nodes", "nodes", nodes)

	log.Debug("starts to validate the LVMVolumeGroupSet nodes")
	valid, reason := validateLVMVolumeGroupSetNodes(nodes)
	if !valid {
		log.Warning("the LVMVolumeGroupSet nodes are invalid", "reason", reason)
		err = updateLVMVolumeGroupSetPhaseIfNeeded(ctx, cl, log, lvgSet, phaseNotCreated, reason)
		if err != nil {
			return false, err
		}

		return true, nil
	}
	log.Debug("the LVMVolumeGroupSet nodes are valid")

	log.Debug("tries to provide LVMVolumeGroups by the LVMVolumeGroupSet")
	err = provideLVMVolumeGroupsBySet(ctx, cl, log, metrics, lvgSet, nodes)
	if err != nil {
		log.Error(err, "unable to provide LVMVolumeGroups by LVMVolumeGroupSet")
		updErr := updateLVMVolumeGroupSetPhaseIfNeeded(ctx, cl, log, lvgSet, phaseNotCreated, err.Error())
		if updErr != nil {
			return false, updErr
		}
		return false, err
	}
	log.Debug("successfully provided LVMVolumeGroups by the LVMVolumeGroupSet")

	err = updateLVMVolumeGroupSetPhaseIfNeeded(ctx, cl, log, lvgSet, phaseCreated, "")
	if err != nil {
		return false, err
	}

	return false, nil
}

func provideLVMVolumeGroupsBySet(ctx context.Context, cl client.Client, log logger.Logger, metrics monitoring.Metrics, lvgSet *v1alpha1.LVMVolumeGroupSet, nodes map[string]v1.Node) error {
	log = log.
		WithName("provideLVMVolumeGroupsBySet").
		WithValues(
			"lvgSetName", lvgSet.Name,
			"strategy", lvgSet.Spec.Strategy)
	//nolint:gocritic
	switch lvgSet.Spec.Strategy {
	case strategyPerNode:
		log.Debug("the LVMVolumeGroupSet has strategy, tries to provide the LVMVolumeGroups",
			"strategy", strategyPerNode)
		err := provideLVMVolumeGroupsPerNode(ctx, cl, log, metrics, lvgSet, nodes)
		if err != nil {
			log.Error(err, "unable to provide LVMVolumeGroups by the LVMVolumeGroupSet with strategy")
			return err
		}
		log.Debug("successfully provided LVMVolumeGroups by the LVMVolumeGroupSet with strategy",
			"strategy", strategyPerNode)
	default:
		return fmt.Errorf("LVMVolumeGroupSet %s strategy %s is not implemented", lvgSet.Name, lvgSet.Spec.Strategy)
	}

	return nil
}

func provideLVMVolumeGroupsPerNode(ctx context.Context, cl client.Client, log logger.Logger, metrics monitoring.Metrics, lvgSet *v1alpha1.LVMVolumeGroupSet, nodes map[string]v1.Node) error {
	log = log.WithName("provideLVMVolumeGroupsPerNode")
	log.Debug("tries to get LVMVolumeGroups")
	currentLVGs, err := GetLVMVolumeGroups(ctx, cl, metrics)
	if err != nil {
		log.Error(err, "unable to get LVMVolumeGroups")
		return err
	}
	log.Debug("successfully got LVMVolumeGroups")
	log.Trace("current LVMVolumeGroups", "currentLVGs", currentLVGs)

	for _, node := range nodes {
		configuredLVG := configureLVGBySet(lvgSet, node)
		log := log.WithValues("lvgName", configuredLVG.Name, "nodeName", node.Name)
		log.Trace("configurated LVMVolumeGroup", "configuredLVG", configuredLVG)

		currentLVG := matchConfiguredLVGWithExistingOne(configuredLVG, currentLVGs)
		if currentLVG != nil {
			log.Debug("tries to update the LVMVolumeGroup")
			err = updateLVMVolumeGroupByConfiguredFromSet(ctx, cl, currentLVG, configuredLVG)
			if err != nil {
				log.Error(err, "unable to update the LVMVolumeGroup")
				return err
			}
			log.Info("LVMVolumeGroup has been successfully updated")
		} else {
			log.Debug("tries to create the LVMVolumeGroup")
			err = createLVMVolumeGroup(ctx, cl, configuredLVG)
			if err != nil {
				log.Error(err, "unable to create the LVMVolumeGroup")
				return err
			}

			log.Info("the LVMVolumeGroup has been created")
			log.Debug("tries to update the LVMVolumeGroupSet status by the created LVMVolumeGroup")
			err = updateLVMVolumeGroupSetStatusByLVGIfNeeded(ctx, cl, log, lvgSet, configuredLVG, nodes)
			if err != nil {
				log.Error(err, "unable to update the LVMVolumeGroupSet")
				return err
			}
			log.Debug("successfully updated the LVMVolumeGroupSet status by the created LVMVolumeGroup")
		}
	}

	return nil
}

func matchConfiguredLVGWithExistingOne(lvg *v1alpha1.LVMVolumeGroup, lvgs map[string]v1alpha1.LVMVolumeGroup) *v1alpha1.LVMVolumeGroup {
	for _, l := range lvgs {
		if l.Spec.Local.NodeName == lvg.Spec.Local.NodeName && l.Spec.ActualVGNameOnTheNode == lvg.Spec.ActualVGNameOnTheNode {
			return &l
		}
	}

	return nil
}

func updateLVMVolumeGroupByConfiguredFromSet(ctx context.Context, cl client.Client, existing, configured *v1alpha1.LVMVolumeGroup) error {
	existing.Spec.ThinPools = configured.Spec.ThinPools
	existing.Spec.BlockDeviceSelector = configured.Spec.BlockDeviceSelector

	return cl.Update(ctx, existing)
}

func updateLVMVolumeGroupSetStatusByLVGIfNeeded(ctx context.Context, cl client.Client, log logger.Logger, lvgSet *v1alpha1.LVMVolumeGroupSet, lvg *v1alpha1.LVMVolumeGroup, nodes map[string]v1.Node) error {
	log = log.WithName("updateLVMVolumeGroupSetStatusByLVGIfNeeded").WithValues("lvgSetName", lvgSet.Name, "lvgName", lvg.Name)
	for _, createdLVG := range lvgSet.Status.CreatedLVGs {
		if createdLVG.LVMVolumeGroupName == lvg.Name {
			log.Debug("no need to update the LVMVolumeGroupSet status by the LVMVolumeGroup")
			return nil
		}
	}

	log.Debug("the LVMVolumeGroupSet status should be updated by the LVMVolumeGroup")
	if cap(lvgSet.Status.CreatedLVGs) == 0 {
		lvgSet.Status.CreatedLVGs = make([]v1alpha1.LVMVolumeGroupSetStatusLVG, 0, len(nodes))
	}

	lvgSet.Status.CreatedLVGs = append(lvgSet.Status.CreatedLVGs, v1alpha1.LVMVolumeGroupSetStatusLVG{
		LVMVolumeGroupName: lvg.Name,
		NodeName:           lvg.Spec.Local.NodeName,
	})

	lvgSet.Status.CurrentLVMVolumeGroupsCount = len(lvgSet.Status.CreatedLVGs)
	lvgSet.Status.DesiredLVMVolumeGroupsCount = len(nodes)

	return cl.Status().Update(ctx, lvgSet)
}

func createLVMVolumeGroup(ctx context.Context, cl client.Client, lvg *v1alpha1.LVMVolumeGroup) error {
	return cl.Create(ctx, lvg)
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
	return fmt.Sprintf("%s-%d", lvgSet.Name, len(lvgSet.Status.CreatedLVGs))
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
	log = log.
		WithName("updateLVMVolumeGroupSetPhaseIfNeeded").
		WithValues(
			"lvgSetName", lvgSet.Name,
			"phase", phase,
			"reason", reason)
	log.Debug("tries to update the LVMVolumeGroupSet status phase and reason")
	if lvgSet.Status.Phase == phase && lvgSet.Status.Reason == reason {
		log.Debug("no need to update phase or reason of the LVMVolumeGroupSet as they are same")
		return nil
	}

	log.Debug("the LVMVolumeGroupSet status phase and reason should be updated",
		"currentPhase", lvgSet.Status.Phase,
		"currentReason", lvgSet.Status.Reason)
	lvgSet.Status.Phase = phase
	lvgSet.Status.Reason = reason
	err := cl.Status().Update(ctx, lvgSet)
	if err != nil {
		log.Error(err, "unable to update the LVMVolumeGroupSet")
		return err
	}

	log.Debug("successfully updated the LVMVolumeGroupSet to phase and reason")
	return nil
}

func validateLVMVolumeGroupSetNodes(nodes map[string]v1.Node) (bool, string) {
	if len(nodes) == 0 {
		return false, "no nodes found by specified nodeSelector"
	}

	return true, ""
}
