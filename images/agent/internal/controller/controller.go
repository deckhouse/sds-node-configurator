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

// SoonestRequeue is the requeue a caller should honour when several results ask
// for one. A zero RequeueAfter means "not asking", so it never wins over a
// result that is.
func SoonestRequeue(results ...Result) Result {
	var soonest Result

	for _, res := range results {
		if res.RequeueAfter <= 0 {
			continue
		}

		if soonest.RequeueAfter <= 0 || res.RequeueAfter < soonest.RequeueAfter {
			soonest = res
		}
	}

	return soonest
}

// DiscoverInOrder makes one discovery pass out of several discoverers, run in
// the order they are given.
//
// That order is a contract rather than a convenience, and this function exists
// so the contract has one place to live instead of being re-stated by every
// caller that runs the pass. The LVMVolumeGroup discoverer matches a Volume
// Group's Physical Volumes against the actualVGNameOnTheNode/vgUuid pair the
// block-device discoverer writes onto a BlockDevice, so it has to run after it.
//
// A caller that ran only the first half is the failure this prevents, and it is
// not hypothetical. The LVMVolumeGroup discoverer bounds how long it retries a
// Physical Volume whose BlockDevice has not arrived (lvg.maxUnnamedPVPasses);
// past that bound the only other thing that runs it is a udev event, which a
// node whose devices have settled does not raise. So a path that creates a
// BlockDevice without running the LVMVolumeGroup discoverer after it leaves
// nothing to notice the device. An operator widening a BlockDeviceFilter
// produces exactly that — a new BlockDevice, no udev event — and the
// LVMVolumeGroup would go on reporting the Physical Volume as unnamed, out of
// service, until the agent restarted.
//
// The pass asks for the soonest requeue any of them asked for. A discoverer that
// already ran keeps its requeue even when a later one fails: it is a request
// about the node, not about the pass.
func DiscoverInOrder(discoverers ...func(context.Context) (Result, error)) func(context.Context) (Result, error) {
	return func(ctx context.Context) (Result, error) {
		var soonest Result

		for _, discover := range discoverers {
			res, err := discover(ctx)
			soonest = SoonestRequeue(soonest, res)
			if err != nil {
				return soonest, err
			}
		}

		return soonest, nil
	}
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

	tname := t.Elem().Name()

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
					if !reconciler.ShouldReconcileCreate(e.Object) {
						log.Debug(fmt.Sprintf("createFunc skipped a request for the %s %s to the Reconcilers queue", tname, e.Object.GetName()))
						return
					}

					log.Info(fmt.Sprintf("createFunc got a create event for the %s, name: %s", tname, e.Object.GetName()))

					request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: e.Object.GetNamespace(), Name: e.Object.GetName()}}
					q.Add(request)

					log.Info(fmt.Sprintf("createFunc added a request for the %s %s to the Reconcilers queue", tname, e.Object.GetName()))
				},
				UpdateFunc: func(
					_ context.Context,
					e event.TypedUpdateEvent[T],
					q workqueue.TypedRateLimitingInterface[reconcile.Request],
				) {
					log.Info(fmt.Sprintf("UpdateFunc got a update event for the %s %s", tname, e.ObjectNew.GetName()))

					if !reconciler.ShouldReconcileUpdate(e.ObjectOld, e.ObjectNew) {
						log.Debug(fmt.Sprintf("updateFunc skipped a request for the %s %s to the Reconcilers queue", tname, e.ObjectNew.GetName()))
						return
					}

					request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: e.ObjectNew.GetNamespace(), Name: e.ObjectNew.GetName()}}
					q.Add(request)

					log.Info(fmt.Sprintf("updateFunc added a request for the %s %s to the Reconcilers queue", tname, e.ObjectNew.GetName()))
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
		log.Info(fmt.Sprintf("[DiscovererDispatcher] %s discoverer starts", discoverer.Name()))

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
		log.Info(fmt.Sprintf("[ReconcileDispatcher] Reconciler starts to reconcile the request %s", req.String()))

		t := reflect.TypeFor[T]()
		obj := reflect.New(t.Elem()).Interface().(T)

		if err := cl.Get(ctx, req.NamespacedName, obj); err != nil {
			if errors.IsNotFound(err) {
				log.Warning(fmt.Sprintf("[ReconcileDispatcher] seems like the object was deleted as unable to get it, err: %s. Stop to reconcile", err.Error()))
				return reconcile.Result{}, nil
			}

			log.Error(err, fmt.Sprintf("[ReconcileDispatcher] unable to get an object by NamespacedName %s", req.String()))
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
