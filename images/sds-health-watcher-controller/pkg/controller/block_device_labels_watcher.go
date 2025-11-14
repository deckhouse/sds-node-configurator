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
	"reflect"

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
	"github.com/deckhouse/sds-node-configurator/images/sds-health-watcher-controller/internal"
	"github.com/deckhouse/sds-node-configurator/images/sds-health-watcher-controller/pkg/logger"
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
	log = log.WithName("RunBlockDeviceLabelsWatcher")
	cl := mgr.GetClient()

	c, err := controller.New(BlockDeviceLabelsWatcherCtrlName, mgr, controller.Options{
		Reconciler: reconcile.Func(func(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
			log := log.WithName("Reconcile").WithValues("blockDeviceName", request.Name)
			log.Info("starts to reconcile the BlockDevice")

			bd := &v1alpha1.BlockDevice{}
			err := cl.Get(ctx, request.NamespacedName, bd)
			if err != nil {
				if errors.IsNotFound(err) {
					log.Warning("seems like the BlockDevice was removed as it was not found. Stop the reconcile")
					return reconcile.Result{}, nil
				}

				log.Error(err, "unable to get the BlockDevice")
				return reconcile.Result{}, err
			}

			log = log.WithValues("blockDeviceName", bd.Name)
			shouldRequeue, err := reconcileBlockDeviceLabels(ctx, cl, log, bd)
			if err != nil {
				log.Error(err, "unable to reconcile the BlockDevice")
				return reconcile.Result{}, err
			}

			if shouldRequeue {
				log.Warning("the request for the BlockDevice should be requeued", "requeueIn", cfg.ScanIntervalSec)
				return reconcile.Result{RequeueAfter: cfg.ScanIntervalSec}, nil
			}

			log.Info("the BlockDevice was successfully reconciled")
			return reconcile.Result{}, nil
		}),
	})
	if err != nil {
		log.Error(err, "unable to create the controller")
		return err
	}

	err = c.Watch(source.Kind(mgr.GetCache(), &v1alpha1.BlockDevice{}, handler.TypedFuncs[*v1alpha1.BlockDevice, reconcile.Request]{
		CreateFunc: func(_ context.Context, e event.TypedCreateEvent[*v1alpha1.BlockDevice], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			log := log.WithName("CreateFunc").WithValues("blockDeviceName", e.Object.Name)
			log.Debug("got a Create event for the BlockDevice")
			q.Add(reconcile.Request{NamespacedName: types.NamespacedName{Namespace: e.Object.GetNamespace(), Name: e.Object.GetName()}})
			log.Debug("the BlockDevice was added to the Reconciler's queue")
		},
		UpdateFunc: func(_ context.Context, e event.TypedUpdateEvent[*v1alpha1.BlockDevice], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			log := log.WithName("UpdateFunc").WithValues("blockDeviceName", e.ObjectNew.Name)
			log.Debug("got an Update event for the BlockDevice")

			if reflect.DeepEqual(e.ObjectOld.Labels, e.ObjectNew.Labels) {
				log.Debug("no need to reconcile the BlockDevice as its labels are the same")
				return
			}

			q.Add(reconcile.Request{NamespacedName: types.NamespacedName{Namespace: e.ObjectNew.GetNamespace(), Name: e.ObjectNew.GetName()}})
			log.Debug("the BlockDevice was added to the Reconciler's queue")
		},
	}))
	if err != nil {
		log.Error(err, "unable to controller.Watch")
		return err
	}

	return nil
}

func reconcileBlockDeviceLabels(ctx context.Context, cl client.Client, log logger.Logger, blockDevice *v1alpha1.BlockDevice) (bool, error) {
	log = log.WithName("reconcileBlockDeviceLabels").WithValues("blockDeviceName", blockDevice.Name)
	log.Info("starts the reconciliation for the BlockDevice")
	shouldRetry := false

	log.Debug("tries to list LVMVolumeGroups")
	lvgList := &v1alpha1.LVMVolumeGroupList{}
	err := cl.List(ctx, lvgList)
	if err != nil {
		return false, err
	}
	log.Debug("successfully listed LVMVolumeGroups")

	for _, lvg := range lvgList.Items {
		log := log.WithValues("lvgName", lvg.Name)
		if len(lvg.Status.Nodes) == 0 {
			log.Info("LVMVolumeGroup nodes are not configured yet, retry later...")
			shouldRetry = true
			continue
		}

		if checkIfLVGInProgress(&lvg) {
			log.Warning("the LVMVolumeGroup is in a progress, retry later...")
			shouldRetry = true
			continue
		}

		log.Debug("tries to configure a selector from blockDeviceSelector of the LVMVolumeGroup")
		selector, err := metav1.LabelSelectorAsSelector(lvg.Spec.BlockDeviceSelector)
		if err != nil {
			return false, err
		}
		log.Debug("successfully configured a selector from blockDeviceSelector of the LVMVolumeGroup")

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
			log.Info("the LVMVolumeGroup should be triggered to update its configuration. Add the label to the resource", "label", LVGUpdateTriggerLabel)
			err = cl.Update(ctx, &lvg)
			if err != nil {
				return false, err
			}
			log.Info("successfully added the label to provide LVMVolumeGroup resource configuration update",
				"label", LVGUpdateTriggerLabel)
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
	log = log.
		WithName("shouldTriggerLVGUpdateIfMatches").
		WithValues(
			"blockDeviceName", blockDevice.Name,
			"lvgName", lvg.Name)
	log.Debug("BlockDevice matches a blockDeviceSelector of the LVMVolumeGroup")
	if _, used := usedBdNames[blockDevice.Name]; !used {
		log.Info("the BlockDevice matches the LVMVolumeGroup blockDeviceSelector, but is not used yet")
		return true
	}

	// for the case when BlockDevice stopped match the LVG blockDeviceSelector and then start again
	for _, c := range lvg.Status.Conditions {
		if c.Type == internal.TypeVGConfigurationApplied && c.Status == metav1.ConditionFalse {
			log.Warning("the BlockDevice matches the LVMVolumeGroup blockDeviceSelector, but the LVMVolumeGroup has condition in status False",
				"conditionType", c.Type)
			return true
		}
	}

	log.Debug("the BlockDevice matches the LVMVolumeGroup blockDeviceSelector and already used by the resource")
	return false
}

func shouldTriggerLVGUpdateIfNotMatches(log logger.Logger, lvg *v1alpha1.LVMVolumeGroup, blockDevice *v1alpha1.BlockDevice, usedBdNames map[string]struct{}) bool {
	log = log.
		WithName("shouldTriggerLVGUpdateIfNotMatches").
		WithValues(
			"blockDeviceName", blockDevice.Name,
			"lvgName", lvg.Name)
	log.Debug("BlockDevice does not match a blockDeviceSelector of the LVMVolumeGroup")
	if _, used := usedBdNames[blockDevice.Name]; used {
		log.Warning("the BlockDevice does not match the LVMVolumeGroup blockDeviceSelector, but is used by the resource")
		return true
	}

	log.Debug("the BlockDevice does not match the LVMVolumeGroup blockDeviceSelector and is not used by the resource")
	return false
}
