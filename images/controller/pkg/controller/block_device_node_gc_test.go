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

package controller

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/deckhouse/sds-common-lib/conditions"
	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/controller/config"
)

var gcNow = time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)

func gcBlockDevice(name, node string, missing *metav1.Condition) *v1alpha1.BlockDevice {
	bd := &v1alpha1.BlockDevice{ObjectMeta: metav1.ObjectMeta{Name: name}}
	bd.Status.NodeName = node
	if missing != nil {
		bd.Status.Conditions = []metav1.Condition{*missing}
	}
	return bd
}

func gcMissing(status metav1.ConditionStatus, reason string, since time.Duration) *metav1.Condition {
	return &metav1.Condition{
		Type:               v1alpha1.BlockDeviceConditionMissing,
		Status:             status,
		Reason:             reason,
		Message:            "m",
		LastTransitionTime: metav1.NewTime(gcNow.Add(-since)),
	}
}

func TestNodeLossReason(t *testing.T) {
	nodes := map[string]struct{}{"with-agent": {}, "no-agent": {}}
	agents := map[string]struct{}{"with-agent": {}}

	assert.Equal(t, "", nodeLossReason("with-agent", nodes, agents))
	assert.Equal(t, v1alpha1.ReasonAgentNotRunning, nodeLossReason("no-agent", nodes, agents))
	assert.Equal(t, v1alpha1.ReasonNodeNotFound, nodeLossReason("deleted", nodes, agents))
	assert.Equal(t, "", nodeLossReason("", nodes, agents), "a BlockDevice without a node name is left alone")
}

func TestAgentNodeNames(t *testing.T) {
	pods := []v1.Pod{
		{Spec: v1.PodSpec{NodeName: "a"}, Status: v1.PodStatus{Phase: v1.PodRunning}},
		{Spec: v1.PodSpec{NodeName: "b"}, Status: v1.PodStatus{Phase: v1.PodFailed}},
		{Spec: v1.PodSpec{NodeName: ""}, Status: v1.PodStatus{Phase: v1.PodPending}},
	}
	assert.Equal(t, map[string]struct{}{"a": {}, "b": {}}, agentNodeNames(pods))
}

func TestNodeGCDecide(t *testing.T) {
	const retention = 24 * time.Hour
	noRefs := newLVGReferences(nil)

	tests := []struct {
		name       string
		missing    *metav1.Condition
		reason     string
		refs       lvgReferences
		lvgName    string
		retention  time.Duration
		wantAction nodeGCAction
		wantStatus metav1.ConditionStatus
		wantReason string
	}{
		{
			name:       "no condition goes Unknown",
			reason:     v1alpha1.ReasonNodeNotFound,
			wantAction: nodeGCMark, wantStatus: metav1.ConditionUnknown, wantReason: v1alpha1.ReasonNodeNotFound,
		},
		{
			name:       "Unknown within the grace period stays",
			missing:    gcMissing(metav1.ConditionUnknown, v1alpha1.ReasonAgentNotRunning, nodeMissingGracePeriod-time.Second),
			reason:     v1alpha1.ReasonAgentNotRunning,
			wantAction: nodeGCNone, wantStatus: metav1.ConditionUnknown, wantReason: v1alpha1.ReasonAgentNotRunning,
		},
		{
			name:       "Unknown past the grace period turns True",
			missing:    gcMissing(metav1.ConditionUnknown, v1alpha1.ReasonAgentNotRunning, nodeMissingGracePeriod),
			reason:     v1alpha1.ReasonAgentNotRunning,
			wantAction: nodeGCMark, wantStatus: metav1.ConditionTrue, wantReason: v1alpha1.ReasonAgentNotRunning,
		},
		{
			name:       "Unknown of another reason takes the new one and does not turn True yet",
			missing:    gcMissing(metav1.ConditionUnknown, v1alpha1.ReasonAgentNotRunning, time.Hour),
			reason:     v1alpha1.ReasonNodeNotFound,
			wantAction: nodeGCMark, wantStatus: metav1.ConditionUnknown, wantReason: v1alpha1.ReasonNodeNotFound,
		},
		{
			name:       "True younger than the retention is kept",
			missing:    gcMissing(metav1.ConditionTrue, v1alpha1.ReasonNodeNotFound, retention-time.Second),
			reason:     v1alpha1.ReasonNodeNotFound,
			wantAction: nodeGCNone, wantStatus: metav1.ConditionTrue, wantReason: v1alpha1.ReasonNodeNotFound,
		},
		{
			name:       "True older than the retention is deleted",
			missing:    gcMissing(metav1.ConditionTrue, v1alpha1.ReasonNodeNotFound, retention),
			reason:     v1alpha1.ReasonNodeNotFound,
			wantAction: nodeGCDelete, wantStatus: metav1.ConditionTrue, wantReason: v1alpha1.ReasonNodeNotFound,
		},
		{
			name:       "True set by the agent counts from its own transition",
			missing:    gcMissing(metav1.ConditionTrue, v1alpha1.ReasonDeviceNotFound, retention+time.Hour),
			reason:     v1alpha1.ReasonAgentNotRunning,
			wantAction: nodeGCDelete, wantStatus: metav1.ConditionTrue, wantReason: v1alpha1.ReasonDeviceNotFound,
		},
		{
			name:    "True older than the retention is kept while an LVMVolumeGroup lists the BlockDevice",
			missing: gcMissing(metav1.ConditionTrue, v1alpha1.ReasonNodeNotFound, retention*2),
			reason:  v1alpha1.ReasonNodeNotFound,
			refs: newLVGReferences([]v1alpha1.LVMVolumeGroup{{
				ObjectMeta: metav1.ObjectMeta{Name: "vg"},
				Status: v1alpha1.LVMVolumeGroupStatus{Nodes: []v1alpha1.LVMVolumeGroupNode{{
					Name: "n", Devices: []v1alpha1.LVMVolumeGroupDevice{{BlockDevice: "bd"}},
				}}},
			}}),
			wantAction: nodeGCNone, wantStatus: metav1.ConditionTrue, wantReason: v1alpha1.ReasonNodeNotFound,
		},
		{
			name:       "True older than the retention is kept while its lvmVolumeGroupName exists",
			missing:    gcMissing(metav1.ConditionTrue, v1alpha1.ReasonNodeNotFound, retention*2),
			reason:     v1alpha1.ReasonNodeNotFound,
			refs:       newLVGReferences([]v1alpha1.LVMVolumeGroup{{ObjectMeta: metav1.ObjectMeta{Name: "vg"}}}),
			lvgName:    "vg",
			wantAction: nodeGCNone, wantStatus: metav1.ConditionTrue, wantReason: v1alpha1.ReasonNodeNotFound,
		},
		{
			name:       "a zero retention never deletes",
			missing:    gcMissing(metav1.ConditionTrue, v1alpha1.ReasonNodeNotFound, retention*100),
			reason:     v1alpha1.ReasonNodeNotFound,
			retention:  -1,
			wantAction: nodeGCNone, wantStatus: metav1.ConditionTrue, wantReason: v1alpha1.ReasonNodeNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bd := gcBlockDevice("bd", "n", tt.missing)
			bd.Status.LVMVolumeGroupName = tt.lvgName
			refs := tt.refs
			if refs.names == nil {
				refs = noRefs
			}
			r := retention
			if tt.retention < 0 {
				r = 0
			}

			assert.Equal(t, tt.wantAction, nodeGCDecide(bd, tt.reason, refs, gcNow, r))
			cond := conditions.Get(bd.Status.Conditions, v1alpha1.BlockDeviceConditionMissing)
			require.NotNil(t, cond)
			assert.Equal(t, tt.wantStatus, cond.Status)
			assert.Equal(t, tt.wantReason, cond.Reason)
		})
	}
}

func newNodeGCTestEnv(t *testing.T, funcs interceptor.Funcs, objs ...client.Object) (*blockDeviceNodeGC, client.Client) {
	t.Helper()

	scheme := apiruntime.NewScheme()
	require.NoError(t, v1alpha1.AddToScheme(scheme))
	require.NoError(t, v1.AddToScheme(scheme))
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).WithInterceptorFuncs(funcs).Build()

	log, err := logger.NewLoggerToWriter(io.Discard, logger.ErrorLevel)
	require.NoError(t, err)
	selector, err := labels.Parse(config.AgentPodLabelSelector)
	require.NoError(t, err)

	return &blockDeviceNodeGC{
		cl:        cl,
		reader:    cl,
		log:       log,
		namespace: config.DefaultAgentNamespace,
		selector:  selector,
		retention: 24 * time.Hour,
		interval:  time.Minute,
	}, cl
}

func agentPod(node string) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "agent-" + node, Namespace: config.DefaultAgentNamespace, Labels: map[string]string{"app": "sds-node-configurator"}},
		Spec:       v1.PodSpec{NodeName: node},
	}
}

func node(name string) *v1.Node {
	return &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
}

func TestBlockDeviceNodeGCPass(t *testing.T) {
	ctx := context.Background()

	gc, cl := newNodeGCTestEnv(t, interceptor.Funcs{},
		node("with-agent"), node("no-agent"), agentPod("with-agent"),
		gcBlockDevice("on-agent-node", "with-agent", nil),
		gcBlockDevice("on-unlabeled-node", "no-agent", nil),
		gcBlockDevice("on-deleted-node", "deleted", nil),
		gcBlockDevice("expired", "deleted", gcMissing(metav1.ConditionTrue, v1alpha1.ReasonNodeNotFound, 25*time.Hour)),
		gcBlockDevice("expired-but-used", "deleted", gcMissing(metav1.ConditionTrue, v1alpha1.ReasonNodeNotFound, 25*time.Hour)),
		&v1alpha1.LVMVolumeGroup{
			ObjectMeta: metav1.ObjectMeta{Name: "vg"},
			Status: v1alpha1.LVMVolumeGroupStatus{Nodes: []v1alpha1.LVMVolumeGroupNode{{
				Name: "deleted", Devices: []v1alpha1.LVMVolumeGroupDevice{{BlockDevice: "expired-but-used"}},
			}}},
		},
	)

	require.NoError(t, gc.pass(ctx, gcNow))

	get := func(name string) *v1alpha1.BlockDevice {
		bd := &v1alpha1.BlockDevice{}
		err := cl.Get(ctx, client.ObjectKey{Name: name}, bd)
		if apierrors.IsNotFound(err) {
			return nil
		}
		require.NoError(t, err)
		return bd
	}

	bd := get("on-agent-node")
	require.NotNil(t, bd)
	assert.Nil(t, conditions.Get(bd.Status.Conditions, v1alpha1.BlockDeviceConditionMissing), "the agent of the node owns its BlockDevices")

	bd = get("on-unlabeled-node")
	require.NotNil(t, bd)
	cond := conditions.Get(bd.Status.Conditions, v1alpha1.BlockDeviceConditionMissing)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionUnknown, cond.Status)
	assert.Equal(t, v1alpha1.ReasonAgentNotRunning, cond.Reason)

	bd = get("on-deleted-node")
	require.NotNil(t, bd)
	cond = conditions.Get(bd.Status.Conditions, v1alpha1.BlockDeviceConditionMissing)
	require.NotNil(t, cond)
	assert.Equal(t, v1alpha1.ReasonNodeNotFound, cond.Reason)

	assert.Nil(t, get("expired"), "Missing=True past the retention and unreferenced is deleted")
	assert.NotNil(t, get("expired-but-used"), "a BlockDevice an LVMVolumeGroup lists is kept")

	require.NoError(t, gc.pass(ctx, gcNow.Add(nodeMissingGracePeriod)))
	cond = conditions.Get(get("on-unlabeled-node").Status.Conditions, v1alpha1.BlockDeviceConditionMissing)
	assert.Equal(t, metav1.ConditionTrue, cond.Status, "Unknown turns True after the grace period")
}

func TestBlockDeviceNodeGCPass_FailedPodListWritesNothing(t *testing.T) {
	ctx := context.Background()

	gc, cl := newNodeGCTestEnv(t, interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*v1.PodList); ok {
				return errors.New("forbidden")
			}
			return c.List(ctx, list, opts...)
		},
	}, node("n"), gcBlockDevice("bd", "n", nil))

	require.Error(t, gc.pass(ctx, gcNow))

	bd := &v1alpha1.BlockDevice{}
	require.NoError(t, cl.Get(ctx, client.ObjectKey{Name: "bd"}, bd))
	assert.Empty(t, bd.Status.Conditions, "without the agent Pods every node would look abandoned")
}

func TestBlockDeviceNodeGCPass_NoAgentPodsWritesNothing(t *testing.T) {
	ctx := context.Background()

	gc, cl := newNodeGCTestEnv(t, interceptor.Funcs{}, node("n"), gcBlockDevice("bd", "n", nil))

	require.NoError(t, gc.pass(ctx, gcNow))

	bd := &v1alpha1.BlockDevice{}
	require.NoError(t, cl.Get(ctx, client.ObjectKey{Name: "bd"}, bd))
	assert.Empty(t, bd.Status.Conditions, "no agent Pod at all is not every node losing its agent")
}
