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
	"encoding/json"

	"github.com/cloudflare/cfssl/log"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	d8commonapi "github.com/deckhouse/sds-common-lib/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/sds-health-watcher-controller/pkg/logger"
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
	log = log.WithName("RunMCWatcher")
	cl := mgr.GetClient()

	c, err := controller.New(MCWatcherCtrlName, mgr, controller.Options{
		Reconciler: reconcile.Func(func(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
			log := log.
				WithName("Reconcile").
				WithValues("moduleConfigName", request.Name)
			log.Info("Reconciler got a request")
			checkMCThinPoolsEnabled(ctx, cl)
			return reconcile.Result{}, nil
		}),
	})

	if err != nil {
		log.Error(err, "unable to create a controller")
		return err
	}

	err = c.Watch(source.Kind(mgr.GetCache(), &d8commonapi.ModuleConfig{}, handler.TypedFuncs[*d8commonapi.ModuleConfig, reconcile.Request]{
		CreateFunc: func(_ context.Context, e event.TypedCreateEvent[*d8commonapi.ModuleConfig], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			log := log.
				WithName("CreateFunc").
				WithValues("moduleConfigName", e.Object.GetName())
			log.Info("got a create event for the ModuleConfig")
			request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: e.Object.GetNamespace(), Name: e.Object.GetName()}}
			q.Add(request)
			log.Info("added the ModuleConfig to the Reconcilers queue")
		},
		UpdateFunc: func(_ context.Context, e event.TypedUpdateEvent[*d8commonapi.ModuleConfig], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			log := log.
				WithName("UpdateFunc").
				WithValues("moduleConfigName", e.ObjectNew.GetName())
			log.Info("got an update event for the ModuleConfig")
			request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: e.ObjectNew.GetNamespace(), Name: e.ObjectNew.GetName()}}
			q.Add(request)
			log.Info("added the ModuleConfig to the Reconcilers queue")
		},
	}))
	if err != nil {
		log.Error(err, "unable to watch the events")
		return err
	}

	return nil
}

func checkMCThinPoolsEnabled(ctx context.Context, cl client.Client) {
	listDevice := &d8commonapi.ModuleConfigList{}

	err := cl.List(ctx, listDevice)
	if err != nil {
		log.Fatal(err)
	}

	for _, moduleItem := range listDevice.Items {
		if moduleItem.Name != sdsLocalVolumeModuleName && moduleItem.Name != sdsReplicatedVolumeName {
			continue
		}

		if value, exists := moduleItem.Spec.Settings["enableThinProvisioning"]; exists && value == true {
			sncModuleConfig := &d8commonapi.ModuleConfig{}
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
