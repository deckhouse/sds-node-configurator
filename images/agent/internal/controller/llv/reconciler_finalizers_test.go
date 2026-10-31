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

package llv

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/deckhouse/sds-common-lib/conditions"
	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/cache"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/monitoring"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/test_utils"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

const consumerFinalizer = "storage.deckhouse.io/sds-local-volume-csi"

func TestForeignFinalizers(t *testing.T) {
	for _, tc := range []struct {
		name       string
		finalizers []string
		want       []string
	}{
		{"none", nil, nil},
		{"only the agent's", []string{internal.SdsNodeConfiguratorFinalizer}, nil},
		{"only a consumer's", []string{consumerFinalizer}, []string{consumerFinalizer}},
		{"both", []string{internal.SdsNodeConfiguratorFinalizer, consumerFinalizer}, []string{consumerFinalizer}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			llv := &v1alpha1.LVMLogicalVolume{ObjectMeta: v1.ObjectMeta{Finalizers: tc.finalizers}}
			assert.Equal(t, tc.want, foreignFinalizers(llv))
		})
	}
}

// createDeletingLLVWithConsumer stores a Created volume that carries a
// consumer's finalizer next to the agent's and has been asked to be deleted.
func createDeletingLLVWithConsumer(ctx context.Context, t *testing.T, cl client.Client) *v1alpha1.LVMLogicalVolume {
	t.Helper()

	llv := &v1alpha1.LVMLogicalVolume{
		ObjectMeta: v1.ObjectMeta{
			Name:       "test-llv",
			Finalizers: []string{internal.SdsNodeConfiguratorFinalizer, consumerFinalizer},
		},
		Spec: v1alpha1.LVMLogicalVolumeSpec{ActualLVNameOnTheNode: "test-lv", Type: internal.Thick, Size: "1Gi"},
		Status: &v1alpha1.LVMLogicalVolumeStatus{
			Phase: v1alpha1.PhaseCreated,
			Conditions: []v1.Condition{{
				Type: conditions.TypeReady, Status: v1.ConditionTrue, Reason: conditions.ReasonReconciled,
				Message: "created", LastTransitionTime: v1.Now(),
			}},
		},
	}
	require.NoError(t, cl.Create(ctx, llv))
	require.NoError(t, cl.Delete(ctx, llv))
	require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(llv), llv))
	require.NotNil(t, llv.DeletionTimestamp)

	return llv
}

// A volume whose deletion waits for its consumer's finalizer says so on the
// resource, once, and is left in place: the Logical Volume is not touched while
// another controller still holds the resource.
func TestReconcileLLVDeleteFuncReportsForeignFinalizers(t *testing.T) {
	ctx := context.Background()
	r := setupReconciler()
	llv := createDeletingLLVWithConsumer(ctx, t, r.cl)

	lvg := &v1alpha1.LVMVolumeGroup{Spec: v1alpha1.LVMVolumeGroupSpec{ActualVGNameOnTheNode: "test-vg"}}

	requeue, err := r.reconcileLLVDeleteFunc(ctx, llv, lvg)
	require.NoError(t, err)
	assert.False(t, requeue)

	got := &v1alpha1.LVMLogicalVolume{}
	require.NoError(t, r.cl.Get(ctx, client.ObjectKeyFromObject(llv), got))
	assert.Equal(t, v1alpha1.PhaseCreated, got.Status.Phase, "the phase still describes the Logical Volume, which is in place")
	// The cache holds no LV, so a pass that went on to the removal would find
	// nothing to remove and succeed: the agent's finalizer gone is what tells it.
	assert.ElementsMatch(t, []string{internal.SdsNodeConfiguratorFinalizer, consumerFinalizer}, got.Finalizers,
		"the pass must not go on to remove the Logical Volume and the agent's finalizer")

	ready := conditions.Get(got.Status.Conditions, conditions.TypeReady)
	require.NotNil(t, ready)
	assert.Equal(t, v1.ConditionFalse, ready.Status)
	assert.Equal(t, conditions.ReasonDeleting, ready.Reason)
	assert.True(t, strings.Contains(ready.Message, consumerFinalizer), "the message names the finalizer it waits for: %q", ready.Message)
	assert.False(t, strings.Contains(ready.Message, internal.SdsNodeConfiguratorFinalizer), "the agent's own finalizer is not what it waits for")

	// A second pass with nothing changed writes nothing.
	resourceVersion := got.ResourceVersion
	_, err = r.reconcileLLVDeleteFunc(ctx, got, lvg)
	require.NoError(t, err)
	again := &v1alpha1.LVMLogicalVolume{}
	require.NoError(t, r.cl.Get(ctx, client.ObjectKeyFromObject(llv), again))
	assert.Equal(t, resourceVersion, again.ResourceVersion)
}

// A failed write of the wait is retried, but not returned as an error: Reconcile
// would turn an error into the Failed phase of a volume that is in place.
func TestReconcileLLVDeleteFuncRequeuesWhenTheWaitCannotBeReported(t *testing.T) {
	ctx := context.Background()
	base := test_utils.NewFakeClient(&v1alpha1.LVMLogicalVolume{})
	llv := createDeletingLLVWithConsumer(ctx, t, base)

	cl := interceptor.NewClient(base, interceptor.Funcs{
		SubResourceUpdate: func(context.Context, client.Client, string, client.Object, ...client.SubResourceUpdateOption) error {
			return errors.New("the API server is unavailable")
		},
	})
	r := NewReconciler(cl, logger.NewNop(), monitoring.GetMetrics(""), cache.New(), utils.NewCommands(), ReconcilerConfig{})

	lvg := &v1alpha1.LVMVolumeGroup{Spec: v1alpha1.LVMVolumeGroupSpec{ActualVGNameOnTheNode: "test-vg"}}

	requeue, err := r.reconcileLLVDeleteFunc(ctx, llv, lvg)
	require.NoError(t, err)
	assert.True(t, requeue)

	got := &v1alpha1.LVMLogicalVolume{}
	require.NoError(t, base.Get(ctx, client.ObjectKeyFromObject(llv), got))
	assert.ElementsMatch(t, []string{internal.SdsNodeConfiguratorFinalizer, consumerFinalizer}, got.Finalizers)
}
