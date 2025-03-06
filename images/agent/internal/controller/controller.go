package controller

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
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
		log.Info(fmt.Sprintf("[ReconcileDispatcher] Reconciler starts to reconcile the request %s", req.NamespacedName.String()))

		t := reflect.TypeFor[T]()
		obj := reflect.New(t.Elem()).Interface().(T)

		if err := cl.Get(ctx, req.NamespacedName, obj); err != nil {
			if errors.IsNotFound(err) {
				log.Warning(fmt.Sprintf("[ReconcileDispatcher] seems like the object was deleted as unable to get it, err: %s. Stop to reconcile", err.Error()))
				return reconcile.Result{}, nil
			}

			log.Error(err, fmt.Sprintf("[ReconcileDispatcher] unable to get an object by NamespacedName %s", req.NamespacedName.String()))
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
