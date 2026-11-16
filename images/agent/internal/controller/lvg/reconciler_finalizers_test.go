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

package lvg

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/deckhouse/sds-common-lib/conditions"
	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/cache"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/mock_utils"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/monitoring"
)

const consumerFinalizer = "storage.deckhouse.io/some-consumer"

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
			lvg := &v1alpha1.LVMVolumeGroup{ObjectMeta: v1.ObjectMeta{Finalizers: tc.finalizers}}
			assert.Equal(t, tc.want, foreignFinalizers(lvg))
		})
	}
}

// A group whose deletion waits for another controller's finalizer touches
// nothing on the node, keeps the agent's finalizer and says what it waits for.
func TestReconcileWaitsForForeignFinalizersBeforeDeleting(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	// No host command is expected: the mock fails the test on any of them.
	mc := mock_utils.NewMockCommands(ctrl)
	r := reconcilerWithMockedCommands(t, mc)

	lvg := &v1alpha1.LVMVolumeGroup{
		ObjectMeta: v1.ObjectMeta{
			Name:       "vg-a",
			Finalizers: []string{internal.SdsNodeConfiguratorFinalizer, consumerFinalizer},
		},
		Spec: v1alpha1.LVMVolumeGroupSpec{
			ActualVGNameOnTheNode: "data-vg",
			Type:                  internal.Local,
			Local:                 v1alpha1.LVMVolumeGroupLocalSpec{NodeName: "test_node"},
		},
		Status: v1alpha1.LVMVolumeGroupStatus{
			Conditions: []v1.Condition{{
				Type: internal.TypeVGConfigurationApplied, Status: v1.ConditionTrue, Reason: internal.ReasonApplied,
				Message: "applied", LastTransitionTime: v1.Now(),
			}},
		},
	}
	require.NoError(t, r.cl.Create(ctx, lvg))
	require.NoError(t, r.cl.Delete(ctx, lvg))
	require.NoError(t, r.cl.Get(ctx, client.ObjectKeyFromObject(lvg), lvg))
	require.NotNil(t, lvg.DeletionTimestamp)

	res, err := r.Reconcile(ctx, controller.ReconcileRequest[*v1alpha1.LVMVolumeGroup]{Object: lvg})
	require.NoError(t, err)
	assert.Equal(t, controller.Result{}, res, "removing the finalizer is an update event, so there is nothing to poll for")

	got := &v1alpha1.LVMVolumeGroup{}
	require.NoError(t, r.cl.Get(ctx, client.ObjectKeyFromObject(lvg), got))
	assert.ElementsMatch(t, []string{internal.SdsNodeConfiguratorFinalizer, consumerFinalizer}, got.Finalizers)

	applied := conditions.Get(got.Status.Conditions, internal.TypeVGConfigurationApplied)
	require.NotNil(t, applied)
	assert.Equal(t, v1.ConditionFalse, applied.Status)
	assert.Equal(t, internal.ReasonTerminating, applied.Reason)
	assert.Contains(t, applied.Message, consumerFinalizer)
	assert.NotContains(t, applied.Message, internal.SdsNodeConfiguratorFinalizer)

	// The write above wakes the agent again. That pass finds the wait already
	// reported and does not go to the API server at all.
	var calls int
	counted := interceptor.NewClient(r.cl.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			calls++
			return c.Get(ctx, key, obj, opts...)
		},
		SubResourceUpdate: func(ctx context.Context, c client.Client, subResource string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			calls++
			return c.SubResource(subResource).Update(ctx, obj, opts...)
		},
	})
	again := NewReconciler(counted, UncachedReader{Reader: counted}, logger.NewNop(), monitoring.GetMetrics(""), cache.New(), mc, nil, ReconcilerConfig{NodeName: "test_node", CmdDeadlineDuration: 30 * time.Second})
	_, err = again.Reconcile(ctx, controller.ReconcileRequest[*v1alpha1.LVMVolumeGroup]{Object: got})
	require.NoError(t, err)
	assert.Zero(t, calls)
}
