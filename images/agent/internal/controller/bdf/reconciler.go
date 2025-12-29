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
	metrics                *monitoring.Metrics
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
	metrics *monitoring.Metrics,
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
	r.log.Trace("Reconciling BlockDeviceFilter")
	return r.runBlockDeviceDiscover(ctx)
}
