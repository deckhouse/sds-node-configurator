/*
	Copyright 2026 Flant JSC

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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
)

// A BlockDeviceFilter decides which devices become BlockDevices, so widening one
// creates BlockDevices — and creating a Kubernetes resource raises no udev event,
// which is the agent's only other way of running discovery.
//
// This reconciler used to run the block-device discoverer alone. An
// LVMVolumeGroup that had spent its unnamed-PV budget (lvg.maxUnnamedPVPasses)
// waiting for one of these devices was therefore never told the device had
// arrived: it kept reporting VGReady=False/NodeNotDescribed, out of service,
// until the agent restarted.
func TestReconcile_RunsTheWholeDiscoveryPass(t *testing.T) {
	var ran []string

	record := func(name string, res controller.Result) func(context.Context) (controller.Result, error) {
		return func(context.Context) (controller.Result, error) {
			ran = append(ran, name)
			return res, nil
		}
	}

	r := NewReconciler(
		nil,
		logger.Logger{},
		nil,
		controller.DiscoverInOrder(
			record("block-devices", controller.Result{}),
			record("volume-groups", controller.Result{RequeueAfter: 5 * time.Second}),
		),
		ReconcilerConfig{NodeName: "node-1"},
	)

	res, err := r.Reconcile(context.Background(), controller.ReconcileRequest[*v1alpha1.BlockDeviceFilter]{})

	require.NoError(t, err)
	assert.Equal(t, []string{"block-devices", "volume-groups"}, ran,
		"the LVMVolumeGroup discoverer has to run after the block-device one, or nothing notices the new BlockDevice")
	assert.Equal(t, 5*time.Second, res.RequeueAfter,
		"the reconciler is transparent: it hands back whatever the pass it was given returned")
}

// In the agent the pass is wrapped in controller.WithSingleRetryChain, and that
// wrapper answers with an empty Result on purpose. This asserts the combination,
// because it is the combination that matters: a Result returned from here goes
// into controller-runtime's work queue for this controller, and a chain there runs
// full discovery passes alongside the scanner's chain — two of them spending
// lvg.maxUnnamedPVPasses in half the wall-clock time the budget is argued from.
func TestReconcile_DoesNotOpenASecondRetryChain(t *testing.T) {
	// The chain the wrapper starts sleeps for the requeue it was given; cancelling
	// ends it with the test instead of leaving a goroutine behind for an hour.
	chainCtx, cancelChain := context.WithCancel(context.Background())
	t.Cleanup(cancelChain)

	r := NewReconciler(
		nil,
		logger.Logger{},
		nil,
		controller.WithSingleRetryChain(
			chainCtx,
			logger.Logger{},
			controller.DiscoveryPassName,
			func(context.Context) (controller.Result, error) {
				return controller.Result{RequeueAfter: time.Hour}, nil
			},
		),
		ReconcilerConfig{NodeName: "node-1"},
	)

	res, err := r.Reconcile(context.Background(), controller.ReconcileRequest[*v1alpha1.BlockDeviceFilter]{})

	require.NoError(t, err)
	assert.Zero(t, res.RequeueAfter,
		"the requeue belongs to the wrapper's chain; returning it here would start a second one in controller-runtime")
}
