package llvs

import (
	"agent/internal/cache"
	"agent/internal/controller"
	"agent/internal/logger"
	"agent/internal/monitoring"
	"agent/internal/utils"
	"context"
	"time"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const ReconcilerName = "lvm-logical-volume-snapshot-watcher-controller"

type Reconciler struct {
	cl       client.Client
	log      logger.Logger
	lvgCl    *utils.LVGClient
	metrics  monitoring.Metrics
	sdsCache *cache.Cache
	cfg      ReconcilerConfig
}

type ReconcilerConfig struct {
	NodeName                string
	BlockDeviceScanInterval time.Duration
	VolumeGroupScanInterval time.Duration
}

func NewReconciler(
	cl client.Client,
	log logger.Logger,
	metrics monitoring.Metrics,
	sdsCache *cache.Cache,
	cfg ReconcilerConfig,
) *Reconciler {
	return &Reconciler{
		cl:  cl,
		log: log,
		lvgCl: utils.NewLVGClient(
			cl,
			log,
			metrics,
			cfg.NodeName,
			ReconcilerName,
		),
		metrics:  metrics,
		sdsCache: sdsCache,
		cfg:      cfg,
	}
}

func (r *Reconciler) Name() string {
	return ReconcilerName
}

func (r *Reconciler) MaxConcurrentReconciles() int {
	return 1
}

// ShouldReconcileUpdate implements controller.Reconciler.
func (r *Reconciler) ShouldReconcileUpdate(_ *v1alpha1.LVMVolumeGroup, _ *v1alpha1.LVMVolumeGroup) bool {
	return true
}

// Reconcile implements controller.Reconciler.
func (r *Reconciler) Reconcile(_ context.Context, _ controller.ReconcileRequest[*v1alpha1.LVMVolumeGroup]) (controller.Result, error) {
	return controller.Result{}, nil
}
