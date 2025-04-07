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

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/sds-health-watcher-controller/config"
	"github.com/deckhouse/sds-node-configurator/images/sds-health-watcher-controller/internal"
	"github.com/deckhouse/sds-node-configurator/images/sds-health-watcher-controller/pkg/logger"
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
)

const (
	BlockDeviceLabelsWatcherCtrlName = "block-device-labels-watcher-controller"

	// LVGUpdateTriggerLabel if you change this value, you must change its value in agent/internal/const.go as well
	LVGUpdateTriggerLabel = "storage.deckhouse.io/update-trigger"
)

func RunBlockDeviceLabelsWatcher(
	mgr manager.Manager,
	log logger.Logger,
	cfg config.Options,
) error {
	cl := mgr.GetClient()

	c, err := controller.New(BlockDeviceLabelsWatcherCtrlName, mgr, controller.Options{
		Reconciler: reconcile.Func(func(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
			log.Info(fmt.Sprintf("[RunBlockDeviceLabelsWatcher] starts to reconcile the BlockDevice %s", request.Name))

			bd := &v1alpha1.BlockDevice{}
			err := cl.Get(ctx, request.NamespacedName, bd)
			if err != nil {
				if errors.IsNotFound(err) {
					log.Warning(fmt.Sprintf("[RunBlockDeviceLabelsWatcher] seems like the BlockDevice %s was removed as it was not found. Stop the reconcile", request.Name))
					return reconcile.Result{}, nil
				}

				log.Error(err, fmt.Sprintf("[RunBlockDeviceLabelsWatcher] unable to get the BlockDevice %s", request.Name))
				return reconcile.Result{}, err
			}

			shouldRequeue, err := reconcileBlockDeviceLabels(ctx, cl, log, bd)
			if err != nil {
				log.Error(err, fmt.Sprintf("[RunBlockDeviceLabelsWatcher] unable to reconcile the BlockDevice %s", bd.Name))
				return reconcile.Result{}, err
			}

			if shouldRequeue {
				log.Warning(fmt.Sprintf("[RunBlockDeviceLabelsWatcher] the request for the BlockDevice %s should be requeued in %s", bd.Name, cfg.ScanIntervalSec.String()))
				return reconcile.Result{RequeueAfter: cfg.ScanIntervalSec}, nil
			}

			log.Info(fmt.Sprintf("[RunBlockDeviceLabelsWatcher] the BlockDevice %s was successfully reconciled", bd.Name))
			return reconcile.Result{}, nil
		}),
	})
	if err != nil {
		log.Error(err, "[RunBlockDeviceLabelsWatcher] unable to create the controller")
		return err
	}

	err = c.Watch(source.Kind(mgr.GetCache(), &v1alpha1.BlockDevice{}, handler.TypedFuncs[*v1alpha1.BlockDevice, reconcile.Request]{
		CreateFunc: func(_ context.Context, e event.TypedCreateEvent[*v1alpha1.BlockDevice], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			log.Debug(fmt.Sprintf("[RunBlockDeviceLabelsWatcher] got a Create event for the BlockDevice %s", e.Object.Name))
			q.Add(reconcile.Request{NamespacedName: types.NamespacedName{Namespace: e.Object.GetNamespace(), Name: e.Object.GetName()}})
			log.Debug(fmt.Sprintf("[RunBlockDeviceLabelsWatcher] the BlockDevice %s was added to the Reconciler's queue", e.Object.Name))
		},
		UpdateFunc: func(_ context.Context, e event.TypedUpdateEvent[*v1alpha1.BlockDevice], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			log.Debug(fmt.Sprintf("[RunBlockDeviceLabelsWatcher] got an Update event for the BlockDevice %s", e.ObjectNew.Name))

			if reflect.DeepEqual(e.ObjectOld.Labels, e.ObjectNew.Labels) {
				log.Debug(fmt.Sprintf("[RunBlockDeviceLabelsWatcher] no need to reconcile the BlockDevice %s as its labels are the same", e.ObjectNew.Name))
				return
			}

			q.Add(reconcile.Request{NamespacedName: types.NamespacedName{Namespace: e.ObjectNew.GetNamespace(), Name: e.ObjectNew.GetName()}})
			log.Debug(fmt.Sprintf("[RunBlockDeviceLabelsWatcher] the BlockDevice %s was added to the Reconciler's queue", e.ObjectNew.Name))
		},
	}))
	if err != nil {
		log.Error(err, "[RunBlockDeviceLabelsWatcher] unable to controller.Watch")
		return err
	}

	return nil
}

func reconcileBlockDeviceLabels(ctx context.Context, cl client.Client, log logger.Logger, blockDevice *v1alpha1.BlockDevice) (bool, error) {
	log.Info(fmt.Sprintf("[reconcileBlockDeviceLabels] starts the reconciliation for the BlockDevice %s", blockDevice.Name))
	shouldRetry := false

	log.Debug("[reconcileBlockDeviceLabels] tries to list LVMVolumeGroups")
	lvgList := &v1alpha1.LVMVolumeGroupList{}
	err := cl.List(ctx, lvgList)
	if err != nil {
		return false, err
	}
	log.Debug("[reconcileBlockDeviceLabels] successfully listed LVMVolumeGroups")

	for _, lvg := range lvgList.Items {
		if len(lvg.Status.Nodes) == 0 {
			log.Info(fmt.Sprintf("[reconcileBlockDeviceLabels] LVMVolumeGroup %s nodes are not configured yet, retry later...", lvg.Name))
			shouldRetry = true
			continue
		}

		if checkIfLVGInProgress(&lvg) {
			log.Warning(fmt.Sprintf("[reconcileBlockDeviceLabels] the LVMVolumeGroup %s is in a progress, retry later...", lvg.Name))
			shouldRetry = true
			continue
		}

		log.Debug(fmt.Sprintf("[reconcileBlockDeviceLabels] tries to configure a selector from blockDeviceSelector of the LVMVolumeGroup %s", lvg.Name))
		selector, err := metav1.LabelSelectorAsSelector(lvg.Spec.BlockDeviceSelector)
		if err != nil {
			return false, err
		}
		log.Debug(fmt.Sprintf("[reconcileBlockDeviceLabels] successfully configured a selector from blockDeviceSelector of the LVMVolumeGroup %s", lvg.Name))

		usedBdNames := make(map[string]struct{}, len(lvg.Status.Nodes[0].Devices))
		for _, n := range lvg.Status.Nodes {
			for _, d := range n.Devices {
				usedBdNames[d.BlockDevice] = struct{}{}
			}
		}

		shouldTrigger := false
		if selector.Matches(labels.Set(blockDevice.Labels)) {
			shouldTrigger = shouldTriggerLVGUpdateIfMatches(log, &lvg, blockDevice, usedBdNames)
		} else {
			shouldTrigger = shouldTriggerLVGUpdateIfNotMatches(log, &lvg, blockDevice, usedBdNames)
		}

		if shouldTrigger {
			if lvg.Labels == nil {
				lvg.Labels = make(map[string]string)
			}
			lvg.Labels[LVGUpdateTriggerLabel] = "true"
			log.Info(fmt.Sprintf("[reconcileBlockDeviceLabels] the LVMVolumeGroup %s should be triggered to update its configuration. Add the label %s to the resource", lvg.Name, LVGUpdateTriggerLabel))
			err = cl.Update(ctx, &lvg)
			if err != nil {
				return false, err
			}
			log.Info(fmt.Sprintf("[reconcileBlockDeviceLabels] successfully added the label %s to provide LVMVolumeGroup %s resource configuration update", LVGUpdateTriggerLabel, lvg.Name))
		}
	}

	return shouldRetry, nil
}

func checkIfLVGInProgress(newLVG *v1alpha1.LVMVolumeGroup) bool {
	for _, c := range newLVG.Status.Conditions {
		if c.Type == internal.TypeVGConfigurationApplied {
			if c.Reason == internal.ReasonUpdating || c.Reason == internal.ReasonCreating {
				return true
			}
		}
	}

	return false
}

func shouldTriggerLVGUpdateIfMatches(log logger.Logger, lvg *v1alpha1.LVMVolumeGroup, blockDevice *v1alpha1.BlockDevice, usedBdNames map[string]struct{}) bool {
	log.Debug(fmt.Sprintf("[reconcileBlockDeviceLabels] BlockDevice %s matches a blockDeviceSelector of the LVMVolumeGroup %s", blockDevice.Name, lvg.Name))
	if _, used := usedBdNames[blockDevice.Name]; !used {
		log.Info(fmt.Sprintf("[reconcileBlockDeviceLabels] the BlockDevice %s matches the LVMVolumeGroup %s blockDeviceSelector, but is not used yet", blockDevice.Name, lvg.Name))
		return true
	}

	// for the case when BlockDevice stopped match the LVG blockDeviceSelector and then start again
	for _, c := range lvg.Status.Conditions {
		if c.Type == internal.TypeVGConfigurationApplied && c.Status == metav1.ConditionFalse {
			log.Warning(fmt.Sprintf("[reconcileBlockDeviceLabels] the BlockDevice %s matches the LVMVolumeGroup %s blockDeviceSelector, but the LVMVolumeGroup has condition %s in status False", blockDevice.Name, lvg.Name, c.Type))
			return true
		}
	}

	log.Debug(fmt.Sprintf("[reconcileBlockDeviceLabels] the BlockDevice %s matches the LVMVolumeGroup %s blockDeviceSelector and already used by the resource", blockDevice.Name, lvg.Name))
	return false
}

func shouldTriggerLVGUpdateIfNotMatches(log logger.Logger, lvg *v1alpha1.LVMVolumeGroup, blockDevice *v1alpha1.BlockDevice, usedBdNames map[string]struct{}) bool {
	log.Debug(fmt.Sprintf("[reconcileBlockDeviceLabels] BlockDevice %s does not match a blockDeviceSelector of the LVMVolumeGroup %s", blockDevice.Name, lvg.Name))
	if _, used := usedBdNames[blockDevice.Name]; used {
		log.Warning(fmt.Sprintf("[reconcileBlockDeviceLabels] the BlockDevice %s does not match the LVMVolumeGroup %s blockDeviceSelector, but is used by the resource", blockDevice.Name, lvg.Name))
		return true
	}

	log.Debug(fmt.Sprintf("[reconcileBlockDeviceLabels] the BlockDevice %s does not match the LVMVolumeGroup %s blockDeviceSelector and is not used by the resource", blockDevice.Name, lvg.Name))
	return false
}
