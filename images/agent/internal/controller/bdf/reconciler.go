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
	client  client.Client
	log     logger.Logger
	metrics *monitoring.Metrics
	// discover is a whole discovery pass, not the block-device discoverer alone.
	// A BlockDeviceFilter decides which devices become BlockDevices, so a change to
	// one can create a BlockDevice — and the LVMVolumeGroup discoverer is what
	// notices that a Physical Volume finally has one. It bounds how long it waits
	// (lvg.maxUnnamedPVPasses), and past that bound the only other thing that runs
	// it is a udev event, which creating a Kubernetes resource does not raise. So
	// re-running only the block-device half here left the LVMVolumeGroup reporting
	// the device as missing, out of service, until the agent restarted.
	//
	// See controller.DiscoverInOrder, which is where the order lives.
	//
	// It is expected to be wrapped in controller.WithSingleRetryChain, and this
	// reconciler is one of the two reasons that wrapper exists. Whatever Result is
	// returned from here goes back into controller-runtime's work queue for this
	// controller, which is a retry chain of its own — one the scanner's gate cannot
	// see, running full discovery passes concurrently with it and spending
	// lvg.maxUnnamedPVPasses twice as fast in wall-clock time. The wrapper answers
	// with an empty Result for exactly that reason, so this reconciler stays
	// transparent: it passes the pass's verdict on, and the verdict is "nothing to
	// requeue, the chain has it".
	discover func(context.Context) (controller.Result, error)
	cfg      ReconcilerConfig
}

type ReconcilerConfig struct {
	NodeName string
	Loglevel logger.Verbosity
}

func NewReconciler(
	client client.Client,
	log logger.Logger,
	metrics *monitoring.Metrics,
	discover func(context.Context) (controller.Result, error),
	cfg ReconcilerConfig,
) *Reconciler {
	return &Reconciler{
		client:   client,
		log:      log,
		metrics:  metrics,
		discover: discover,
		cfg:      cfg,
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
	return r.discover(ctx)
}
