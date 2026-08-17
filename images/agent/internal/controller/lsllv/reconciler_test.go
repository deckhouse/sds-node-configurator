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

package lsllv

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/cache"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/mock_utils"
)

const (
	testNode = "node-1"
	testVG   = "vgshared"
	testLV   = "vol1"
)

func testGroup(owner string) *v1alpha1.LVMSharedVolumeGroup {
	return &v1alpha1.LVMSharedVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "pool-1"},
		Spec: v1alpha1.LVMSharedVolumeGroupSpec{
			ActualVGNameOnTheNode: testVG,
			Nodes:                 []string{testNode},
			MetadataOwner:         owner,
			VolumeCleanup:         v1alpha1.VolumeCleanupDiscard,
		},
	}
}

func testVolume(opts ...func(*v1alpha1.LVMSharedLogicalVolume)) *v1alpha1.LVMSharedLogicalVolume {
	v := &v1alpha1.LVMSharedLogicalVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "vol-1"},
		Spec: v1alpha1.LVMSharedLogicalVolumeSpec{
			LVMSharedVolumeGroupName: "pool-1",
			ActualLVNameOnTheNode:    testLV,
			Size:                     "10Gi",
			VolumeCleanup:            v1alpha1.VolumeCleanupDiscard,
		},
	}
	for _, opt := range opts {
		opt(v)
	}
	return v
}

func deleting(v *v1alpha1.LVMSharedLogicalVolume) {
	now := metav1.Now()
	v.DeletionTimestamp = &now
	v.Finalizers = append(v.Finalizers, internal.SdsNodeConfiguratorFinalizer)
}

func noCleanup(v *v1alpha1.LVMSharedLogicalVolume) {
	v.Spec.VolumeCleanup = ""
}

func existingLV(tags string) internal.LVData {
	return internal.LVData{VGName: testVG, LVName: testLV, LVAttr: "-wi-------", LvTags: tags}
}

func testReconciler(
	t *testing.T,
	commands *mock_utils.MockCommands,
	lvs []internal.LVData,
	objects ...client.Object,
) (*Reconciler, client.Client) {
	t.Helper()

	s := scheme.Scheme
	require.NoError(t, v1alpha1.AddToScheme(s))
	cl := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(objects...).
		WithStatusSubresource(&v1alpha1.LVMSharedLogicalVolume{}).
		Build()

	sdsCache := cache.New()
	sdsCache.StoreLVs(lvs, bytes.Buffer{})

	// These tests describe what happens around the volume, not how it is looked
	// up, so lvm answers "cannot say" and the lookup falls back to the scan
	// results the test seeded. The other way round — lvm answering — is a test of
	// its own, below.
	commands.EXPECT().GetLV(gomock.Any(), gomock.Any()).
		Return(internal.LVData{}, "lvs", bytes.Buffer{}, errors.New("not asked here")).AnyTimes()

	log, err := logger.NewLogger(logger.WarningLevel)
	require.NoError(t, err)

	return NewReconciler(cl, log, sdsCache, commands, ReconcilerConfig{NodeName: testNode}), cl
}

func reconcile(t *testing.T, r *Reconciler, v *v1alpha1.LVMSharedLogicalVolume) controller.Result {
	t.Helper()
	res, err := r.Reconcile(context.Background(),
		controller.ReconcileRequest[*v1alpha1.LVMSharedLogicalVolume]{Object: v})
	require.NoError(t, err)
	return res
}

func TestOnlyTheMetadataOwnerCreates(t *testing.T) {
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	// No command: LVM metadata has one writer by construction, and two nodes
	// creating into one group is not a race that ends well.
	volume := testVolume()
	r, _ := testReconciler(t, commands, nil, testGroup("other-node"), volume)

	reconcile(t, r, volume)
}

func TestCreatedVolumeIsReleasedImmediately(t *testing.T) {
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	volume := testVolume()
	r, cl := testReconciler(t, commands, nil, testGroup(testNode), volume)

	gomock.InOrder(
		commands.EXPECT().CreateLVShared(gomock.Any(), testVG, testLV, "10Gi").Return("lvcreate", nil),
		// Creating is not attaching: an owner that kept every volume it made
		// would hold the exclusive lock of the whole pool.
		commands.EXPECT().LVDeactivateShared(gomock.Any(), testVG, []string{testLV}).Return("lvchange -an", nil),
	)

	reconcile(t, r, volume)

	got := &v1alpha1.LVMSharedLogicalVolume{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKey{Name: "vol-1"}, got))
	assert.Equal(t, PhaseCreated, got.Status.Phase)
	assert.Contains(t, got.Finalizers, internal.SdsNodeConfiguratorFinalizer)
}

func TestExistingVolumeIsNotCreatedTwice(t *testing.T) {
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	volume := testVolume()
	r, cl := testReconciler(t, commands, []internal.LVData{existingLV("")}, testGroup(testNode), volume)

	reconcile(t, r, volume)

	got := &v1alpha1.LVMSharedLogicalVolume{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKey{Name: "vol-1"}, got))
	assert.Equal(t, PhaseCreated, got.Status.Phase)
}

func TestVanishedVolumeIsJustReleased(t *testing.T) {
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	// Nothing on the node: no erase, no lvremove, and the object may go.
	volume := testVolume(deleting)
	r, cl := testReconciler(t, commands, nil, testGroup(testNode), volume)

	reconcile(t, r, volume)

	err := cl.Get(context.Background(), client.ObjectKey{Name: "vol-1"}, &v1alpha1.LVMSharedLogicalVolume{})
	assert.Error(t, err)
}

func TestGroupPolicyIsTheFloorNotTheDefault(t *testing.T) {
	// A volume that asks for nothing still gets what the pool demands: capacity
	// here goes back into the same LUN and comes out as somebody else's volume.
	assert.Equal(t, v1alpha1.VolumeCleanupDiscard,
		effectiveCleanup(testVolume(noCleanup), testGroup(testNode)))

	// And a volume asking for more than the pool gets more.
	stronger := testVolume(func(v *v1alpha1.LVMSharedLogicalVolume) {
		v.Spec.VolumeCleanup = v1alpha1.VolumeCleanupRandomFillThreePass
	})
	assert.Equal(t, v1alpha1.VolumeCleanupRandomFillThreePass, effectiveCleanup(stronger, testGroup(testNode)))
}

func TestFailedCreationIsReported(t *testing.T) {
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	volume := testVolume()
	r, cl := testReconciler(t, commands, nil, testGroup(testNode), volume)

	commands.EXPECT().CreateLVShared(gomock.Any(), testVG, testLV, "10Gi").
		Return("lvcreate", errors.New("Volume group vgshared has insufficient free space"))

	_, err := r.Reconcile(context.Background(),
		controller.ReconcileRequest[*v1alpha1.LVMSharedLogicalVolume]{Object: volume})
	require.Error(t, err)

	got := &v1alpha1.LVMSharedLogicalVolume{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKey{Name: "vol-1"}, got))
	assert.Equal(t, PhasePending, got.Status.Phase)
}

func TestConditionTimestampMovesOnlyOnChange(t *testing.T) {
	conditions := []metav1.Condition{}
	setCondition(&conditions, metav1.Condition{Type: ConditionReady, Status: metav1.ConditionFalse, Reason: "One"})
	first := conditions[0].LastTransitionTime

	setCondition(&conditions, metav1.Condition{Type: ConditionReady, Status: metav1.ConditionFalse, Reason: "Two"})
	assert.Equal(t, first, conditions[0].LastTransitionTime)

	setCondition(&conditions, metav1.Condition{Type: ConditionReady, Status: metav1.ConditionTrue, Reason: "Three"})
	assert.NotEqual(t, first, conditions[0].LastTransitionTime)
}

func TestAnExistingVolumeIsNotCreatedTwiceWhenTheScanIsStale(t *testing.T) {
	// The scanner has no schedule of its own, so a volume created a moment ago
	// may be absent from the scan results. Trusting them would run lvcreate
	// against a name that exists, and the failure would move a perfectly good
	// volume from Created back to Pending.
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	volume := testVolume()
	group := testGroup(testNode)

	s := scheme.Scheme
	require.NoError(t, v1alpha1.AddToScheme(s))
	cl := fake.NewClientBuilder().WithScheme(s).WithObjects(volume, group).
		WithStatusSubresource(&v1alpha1.LVMSharedLogicalVolume{}).Build()

	commands.EXPECT().GetLV(testVG, testLV).
		Return(internal.LVData{VGName: testVG, LVName: testLV}, "lvs", bytes.Buffer{}, nil).AnyTimes()
	// No CreateLVShared expectation: asking lvm is the whole point.

	log, err := logger.NewLogger(logger.WarningLevel)
	require.NoError(t, err)
	r := NewReconciler(cl, log, cache.New(), commands, ReconcilerConfig{NodeName: testNode})

	reconcile(t, r, volume)

	published := &v1alpha1.LVMSharedLogicalVolume{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKey{Name: volume.Name}, published))
	require.NotNil(t, published.Status)
	assert.Equal(t, PhaseCreated, published.Status.Phase)
}
