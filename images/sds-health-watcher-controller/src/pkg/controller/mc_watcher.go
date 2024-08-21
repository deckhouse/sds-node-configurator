/*
Copyright 2024 Flant JSC

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
	"encoding/json"
	"fmt"

	"github.com/cloudflare/cfssl/log"
	mc "sds-health-watcher-controller/api"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"sds-health-watcher-controller/pkg/logger"
)

const (
	MCWatcherCtrlName             = "sds-mc-watcher-controller"
	sdsNodeConfiguratorModuleName = "sds-node-configurator"
	sdsLocalVolumeModuleName      = "sds-local-volume"
	sdsReplicatedVolumeName       = "sds-replicated-volume"
)

func RunMCWatcher(
	mgr manager.Manager,
	log logger.Logger,
) error {
	cl := mgr.GetClient()

	c, err := controller.New(MCWatcherCtrlName, mgr, controller.Options{
		Reconciler: reconcile.Func(func(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
			log.Info(fmt.Sprintf("[RunMCWatcher] Reconciler got a request %s", request.String()))
			checkMCThinPoolsEnabled(ctx, cl)
			return reconcile.Result{}, nil
		}),
	})

	if err != nil {
		log.Error(err, "[RunMCWatcher] unable to create a controller")
		return err
	}

	err = c.Watch(source.Kind(mgr.GetCache(), &mc.ModuleConfig{}, handler.TypedFuncs[*mc.ModuleConfig, reconcile.Request]{
		CreateFunc: func(_ context.Context, e event.TypedCreateEvent[*mc.ModuleConfig], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			log.Info(fmt.Sprintf("[RunMCWatcher] got a create event for the ModuleConfig %s", e.Object.GetName()))
			request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: e.Object.GetNamespace(), Name: e.Object.GetName()}}
			q.Add(request)
			log.Info(fmt.Sprintf("[RunMCWatcher] added the ModuleConfig %s to the Reconcilers queue", e.Object.GetName()))
		},
		UpdateFunc: func(_ context.Context, e event.TypedUpdateEvent[*mc.ModuleConfig], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			log.Info(fmt.Sprintf("[RunMCWatcher] got a update event for the ModuleConfig %s", e.ObjectNew.GetName()))
			request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: e.ObjectNew.GetNamespace(), Name: e.ObjectNew.GetName()}}
			q.Add(request)
			log.Info(fmt.Sprintf("[RunMCWatcher] added the ModuleConfig %s to the Reconcilers queue", e.ObjectNew.GetName()))
		},
	}))
	if err != nil {
		log.Error(err, "[RunMCWatcher] unable to watch the events")
		return err
	}

	return nil
}

func checkMCThinPoolsEnabled(ctx context.Context, cl client.Client) {
	listDevice := &mc.ModuleConfigList{}

	err := cl.List(ctx, listDevice)
	if err != nil {
		log.Fatal(err)
	}

	for _, moduleItem := range listDevice.Items {
		if moduleItem.Name != sdsLocalVolumeModuleName && moduleItem.Name != sdsReplicatedVolumeName {
			continue
		}

		if value, exists := moduleItem.Spec.Settings["enableThinProvisioning"]; exists && value == true {
			sncModuleConfig := &mc.ModuleConfig{}
			err = cl.Get(ctx, types.NamespacedName{Name: sdsNodeConfiguratorModuleName, Namespace: ""}, sncModuleConfig)
			if err != nil {
				log.Fatal(err)
			}

			if value, exists = sncModuleConfig.Spec.Settings["enableThinProvisioning"]; exists && value == true {
				log.Info("Thin pools support is enabled")
			} else {
				log.Info("Enabling thin pools support")
				patchBytes, err := json.Marshal(map[string]interface{}{
					"spec": map[string]interface{}{
						"version": 1,
						"settings": map[string]interface{}{
							"enableThinProvisioning": true,
						},
					},
				})

				if err != nil {
					log.Fatalf("Error marshalling patch: %s", err.Error())
				}

				err = cl.Patch(context.TODO(), sncModuleConfig, client.RawPatch(types.MergePatchType, patchBytes))
				if err != nil {
					log.Fatalf("Error patching object: %s", err.Error())
				}
			}
		}
	}
}
