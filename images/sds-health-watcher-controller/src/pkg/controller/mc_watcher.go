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
	dh "github.com/deckhouse/deckhouse/deckhouse-controller/pkg/apis/deckhouse.io/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sds-health-watcher-controller/pkg/logger"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
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
		Reconciler: reconcile.Func(func(_ context.Context, request reconcile.Request) (reconcile.Result, error) {
			log.Info(fmt.Sprintf("[RunLVGConditionsWatcher] Reconciler got a request %s", request.String()))
			return reconcile.Result{}, nil
		}),
	})

	if err != nil {
		log.Error(err, "[MCWatcherCtrlName] unable to create a controller")
		return err
	}

	err = c.Watch(source.Kind(mgr.GetCache(), &dh.ModuleConfig{}), handler.Funcs{
		CreateFunc: func(ctx context.Context, e event.CreateEvent, _ workqueue.RateLimitingInterface) {
			log.Info(fmt.Sprintf("[MCWatcherCtrlName] got a create event for the ModuleConfig %s", e.Object.GetName()))
			checkMCThinPoolsEnabled(ctx, cl)
		},
		UpdateFunc: func(ctx context.Context, e event.UpdateEvent, _ workqueue.RateLimitingInterface) {
			log.Info(fmt.Sprintf("[MCWatcherCtrlName] got a update event for the ModuleConfig %s", e.ObjectNew.GetName()))
			checkMCThinPoolsEnabled(ctx, cl)
		},
	})
	if err != nil {
		log.Error(err, "[MCWatcherCtrlName] unable to watch the events")
		return err
	}

	return nil
}

func checkMCThinPoolsEnabled(ctx context.Context, cl client.Client) {
	listDevice := &dh.ModuleConfigList{}

	err := cl.List(ctx, listDevice)
	if err != nil {
		log.Fatal(err)
	}

	for _, moduleItem := range listDevice.Items {
		if moduleItem.Name != sdsLocalVolumeModuleName && moduleItem.Name != sdsReplicatedVolumeName {
			continue
		}

		if value, exists := moduleItem.Spec.Settings["enableThinProvisioning"]; exists && value == true {
			ctx := context.Background()

			sncModuleConfig := &dh.ModuleConfig{}

			err = cl.Get(ctx, types.NamespacedName{Name: sdsNodeConfiguratorModuleName, Namespace: ""}, sncModuleConfig)
			if err != nil {
				log.Fatal(err)
			}

			if value, exists := sncModuleConfig.Spec.Settings["enableThinProvisioning"]; exists && value == true {
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
