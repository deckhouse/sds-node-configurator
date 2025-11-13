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
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
)

type ReconcileRequest[T client.Object] struct {
	Object T
}

type Result struct {
	RequeueAfter time.Duration
}

type Named interface {
	Name() string
}

type Reconciler[T client.Object] interface {
	Named
	MaxConcurrentReconciles() int
	ShouldReconcileCreate(objectNew T) bool
	ShouldReconcileUpdate(objectOld T, objectNew T) bool
	Reconcile(context.Context, ReconcileRequest[T]) (Result, error)
}

type Discoverer interface {
	Named
	Discover(context.Context) (Result, error)
}

func AddReconciler[T client.Object](
	mgr manager.Manager,
	log logger.Logger,
	reconciler Reconciler[T],
) error {
	t := reflect.TypeFor[T]()
	if t.Kind() != reflect.Pointer {
		panic("T is not a pointer")
	}

	if t.Elem().Kind() != reflect.Struct {
		panic("T is not a struct pointer")
	}

	typeName := t.Elem().Name()

	c, err := controller.New(
		reconciler.Name(),
		mgr,
		controller.Options{
			Reconciler:              makeReconcileDispatcher(mgr, log, reconciler),
			MaxConcurrentReconciles: reconciler.MaxConcurrentReconciles(),
		},
	)

	if err != nil {
		return err
	}

	obj := reflect.New(t.Elem()).Interface().(T)

	return c.Watch(
		source.Kind(
			mgr.GetCache(),
			obj,
			handler.TypedFuncs[T, reconcile.Request]{
				CreateFunc: func(
					_ context.Context,
					e event.TypedCreateEvent[T],
					q workqueue.TypedRateLimitingInterface[reconcile.Request],
				) {
					objLog := log.WithName("CreateFunc").WithValues("type", typeName, "name", e.Object.GetName())
					if !reconciler.ShouldReconcileCreate(e.Object) {
						objLog.Debug("createFunc skipped a request to the Reconcilers queue")
						return
					}

					objLog.Info("createFunc got a create event")

					request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: e.Object.GetNamespace(), Name: e.Object.GetName()}}
					q.Add(request)

					objLog.Info("createFunc added a request to the Reconcilers queue")
				},
				UpdateFunc: func(
					_ context.Context,
					e event.TypedUpdateEvent[T],
					q workqueue.TypedRateLimitingInterface[reconcile.Request],
				) {
					objLog := log.WithName("UpdateFunc").WithValues("type", typeName, "name", e.ObjectNew.GetName())
					objLog.Info("UpdateFunc got a update event")

					if !reconciler.ShouldReconcileUpdate(e.ObjectOld, e.ObjectNew) {
						objLog.Debug("updateFunc skipped a request to the Reconcilers queue")
						return
					}

					request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: e.ObjectNew.GetNamespace(), Name: e.ObjectNew.GetName()}}
					q.Add(request)

					objLog.Info("updateFunc added a request to the Reconcilers queue")
				},
			},
		),
	)
}

func AddDiscoverer(
	mgr manager.Manager,
	log logger.Logger,
	discoverer Discoverer,
) (discover func(context.Context) (Result, error), err error) {
	kCtrl, err := controller.New(
		discoverer.Name(),
		mgr,
		controller.Options{
			Reconciler: makeDiscovererDispatcher(log, discoverer),
		},
	)
	if err != nil {
		return nil, err
	}

	return func(ctx context.Context) (Result, error) {
		res, err := kCtrl.Reconcile(ctx, reconcile.Request{})
		return Result{RequeueAfter: res.RequeueAfter}, err
	}, nil
}

func makeDiscovererDispatcher(log logger.Logger, discoverer Discoverer) reconcile.Func {
	return reconcile.Func(func(ctx context.Context, _ reconcile.Request) (reconcile.Result, error) {
		log := log.WithName("DiscovererDispatcher").WithValues("discovererName", discoverer.Name())
		log.Info("discoverer starts")

		result, err := discoverer.Discover(ctx)

		return reconcile.Result{RequeueAfter: result.RequeueAfter}, err
	})
}

func makeReconcileDispatcher[T client.Object](
	mgr manager.Manager,
	log logger.Logger,
	reconciler Reconciler[T],
) reconcile.TypedReconciler[reconcile.Request] {
	cl := mgr.GetClient()
	return reconcile.Func(func(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
		// load object being reconciled
		log := log.WithName("ReconcileDispatcher").WithValues("namespacedName", req.NamespacedName)
		log.Info("Reconciler starts to reconcile the request")

		t := reflect.TypeFor[T]()
		obj := reflect.New(t.Elem()).Interface().(T)

		if err := cl.Get(ctx, req.NamespacedName, obj); err != nil {
			if errors.IsNotFound(err) {
				log.Warning("seems like the object was deleted as unable to get it. Stop to reconcile", "error", err)
				return reconcile.Result{}, nil
			}

			log.Error(err, "unable to get an object by NamespacedName")
			return reconcile.Result{}, err
		}

		result, err := reconciler.Reconcile(
			ctx,
			ReconcileRequest[T]{
				Object: obj,
			},
		)
		return reconcile.Result{
			RequeueAfter: result.RequeueAfter,
		}, err
	})
}
