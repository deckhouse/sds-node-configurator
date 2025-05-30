package bdf

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/monitoring"
)

const ReconcilerName = "block-device-filter-watcher-controller"

type Reconciler struct {
	client                 client.Client
	log                    logger.Logger
	metrics                monitoring.Metrics
	runBlockDeviceDiscover func(context.Context) (controller.Result, error)
	cfg                    ReconcilerConfig
}

type ReconcilerConfig struct {
	NodeName string
	Loglevel logger.Verbosity
}

func NewReconciler(
	client client.Client,
	log logger.Logger,
	metrics monitoring.Metrics,
	runBlockDeviceDiscover func(context.Context) (controller.Result, error),
	cfg ReconcilerConfig,
) *Reconciler {
	return &Reconciler{
		client:                 client,
		log:                    log,
		metrics:                metrics,
		runBlockDeviceDiscover: runBlockDeviceDiscover,
		cfg:                    cfg,
	}
}

func (r *Reconciler) Name() string {
	return ReconcilerName
}

func (r *Reconciler) MaxConcurrentReconciles() int {
	return 1
}

func (r *Reconciler) ShouldReconcileCreate(_ *v1alpha1.BlockDeviceFilter) bool {
	return true
}

func (r *Reconciler) ShouldReconcileUpdate(_ *v1alpha1.BlockDeviceFilter, _ *v1alpha1.BlockDeviceFilter) bool {
	return true
}

func (r *Reconciler) Reconcile(ctx context.Context, _ controller.ReconcileRequest[*v1alpha1.BlockDeviceFilter]) (controller.Result, error) {
	return r.runBlockDeviceDiscover(ctx)
}
