/*
Copyright YEAR Flant JSC

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

	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/deckhouse/sds-node-configurator/images/sds-common-scheduler-extender/pkg/logger"
	lapi "github.com/deckhouse/sds-replicated-volume/api/linstor"
)

const (
	LVGLayerResourceIDsWatcherName = "layer-resource-ids-watcher"
)

func RunLayerResourceIDsWatcher(
	mgr manager.Manager,
	log *logger.Logger,
) error {
	log.Info("[RunLayerResourceIDsWatcher] starts the work")

	c, err := controller.New(LVGLayerResourceIDsWatcherName, mgr, controller.Options{
		Reconciler: reconcile.Func(func(_ context.Context, _ reconcile.Request) (reconcile.Result, error) {
			return reconcile.Result{}, nil
		}),
	})
	if err != nil {
		log.Error(err, "[RunLayerResourceIDsWatcher] unable to create a controller")
		return err
	}

	err = c.Watch(source.Kind(mgr.GetCache(), &lapi.LayerResourceIds{}, handler.TypedFuncs[*lapi.LayerResourceIds, reconcile.Request]{
		CreateFunc: func(_ context.Context, e event.TypedCreateEvent[*lapi.LayerResourceIds], _ workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			log.Info(fmt.Sprintf("[RunLayerResourceIDsWatcher] res id created %s", e.Object.GetName()))
		},
	}))

	return nil
}
