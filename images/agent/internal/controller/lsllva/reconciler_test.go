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

package lsllva

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"k8s.io/apimachinery/pkg/api/resource"
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
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

const (
	testNode = "node-1"
	testVG   = "vgshared"
	testLV   = "vol1"
)

func testGroup() *v1alpha1.LVMSharedVolumeGroup {
	return &v1alpha1.LVMSharedVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "pool-1"},
		Spec: v1alpha1.LVMSharedVolumeGroupSpec{
			ActualVGNameOnTheNode: testVG,
			Nodes:                 []string{testNode},
			VolumeCleanup:         v1alpha1.VolumeCleanupDiscard,
		},
	}
}

func testVolume() *v1alpha1.LVMSharedLogicalVolume {
	return &v1alpha1.LVMSharedLogicalVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "vol-1"},
		Spec: v1alpha1.LVMSharedLogicalVolumeSpec{
			LVMSharedVolumeGroupName: "pool-1",
			ActualLVNameOnTheNode:    testLV,
			Size:                     "10Gi",
			VolumeCleanup:            v1alpha1.VolumeCleanupDiscard,
		},
	}
}

func testAttachment(node string, opts ...func(*v1alpha1.LVMSharedLogicalVolumeAttachment)) *v1alpha1.LVMSharedLogicalVolumeAttachment {
	a := &v1alpha1.LVMSharedLogicalVolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "att-1"},
		Spec: v1alpha1.LVMSharedLogicalVolumeAttachmentSpec{
			LVMSharedLogicalVolumeName: "vol-1",
			NodeName:                   node,
			AccessMode:                 v1alpha1.LVMSharedLogicalVolumeAccessModeRWO,
		},
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

func withFinalizer(a *v1alpha1.LVMSharedLogicalVolumeAttachment) {
	a.Finalizers = append(a.Finalizers, internal.SdsNodeConfiguratorFinalizer)
}

func deleting(a *v1alpha1.LVMSharedLogicalVolumeAttachment) {
	now := metav1.Now()
	a.DeletionTimestamp = &now
	withFinalizer(a)
}

func activeLV() internal.LVData {
	return internal.LVData{VGName: testVG, LVName: testLV, LVAttr: "-wi-a-----"}
}

func inactiveLV() internal.LVData {
	return internal.LVData{VGName: testVG, LVName: testLV, LVAttr: "-wi-------"}
}

// dmView points the device-mapper view at a directory this test owns, and hands
// back the two things a test does with it. "Active on this node" is a dm device
// and nothing else, so this is the whole of the state the reconciler reads back
// after an activation.
func dmView(t *testing.T) (activate func(), deactivate func()) {
	t.Helper()
	root := t.TempDir()
	previous := utils.SysBlockRoot
	utils.SysBlockRoot = root
	t.Cleanup(func() { utils.SysBlockRoot = previous })

	dir := filepath.Join(root, "dm-0", "dm")
	return func() {
			require.NoError(t, os.MkdirAll(dir, 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(dir, "name"), []byte(testVG+"-"+testLV+"\n"), 0o644))
		}, func() {
			require.NoError(t, os.RemoveAll(filepath.Join(root, "dm-0")))
		}
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
		WithStatusSubresource(&v1alpha1.LVMSharedLogicalVolumeAttachment{}).
		Build()

	sdsCache := cache.New()
	sdsCache.StoreLVs(lvs, bytes.Buffer{})

	// Sizes come from lvm first and the scan results only as a fallback; these
	// tests describe the fallback, so lvm answers "cannot say".
	commands.EXPECT().GetLV(gomock.Any(), gomock.Any()).
		Return(internal.LVData{}, "lvs", bytes.Buffer{}, errors.New("not asked here")).AnyTimes()

	log, err := logger.NewLogger(logger.WarningLevel)
	require.NoError(t, err)

	return NewReconciler(cl, log, sdsCache, commands, ReconcilerConfig{NodeName: testNode}), cl
}

func reconcile(t *testing.T, r *Reconciler, a *v1alpha1.LVMSharedLogicalVolumeAttachment) controller.Result {
	t.Helper()
	res, err := r.Reconcile(context.Background(),
		controller.ReconcileRequest[*v1alpha1.LVMSharedLogicalVolumeAttachment]{Object: a})
	require.NoError(t, err)
	return res
}

func attachmentFromAPI(t *testing.T, cl client.Client) *v1alpha1.LVMSharedLogicalVolumeAttachment {
	t.Helper()
	a := &v1alpha1.LVMSharedLogicalVolumeAttachment{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKey{Name: "att-1"}, a))
	return a
}

func TestAttachmentsOfOtherNodesAreIgnored(t *testing.T) {
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	// No command at all: activating a volume attached to a neighbour is the one
	// thing exclusive activation exists to prevent.
	attachment := testAttachment("other-node")
	r, _ := testReconciler(t, commands, nil, testGroup(), testVolume(), attachment)

	reconcile(t, r, attachment)
	assert.False(t, r.ShouldReconcileCreate(attachment))
}

func TestAttachActivatesAndReportsThePath(t *testing.T) {
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	attachment := testAttachment(testNode)
	activate, _ := dmView(t)
	r, cl := testReconciler(t, commands, nil, testGroup(), testVolume(), attachment)

	// No device-mapper device before the command and one after it, which is what
	// the reconciler reads back instead of trusting the exit code.
	commands.EXPECT().LVActivateShared(gomock.Any(), testVG, []string{testLV}, false).DoAndReturn(
		func(_ context.Context, _ string, _ []string, _ bool) (string, error) {
			activate()
			return "lvchange -aey", nil
		})

	reconcile(t, r, attachment)

	got := attachmentFromAPI(t, cl)
	assert.Equal(t, PhaseAttached, got.Status.Phase)
	assert.Equal(t, "/dev/vgshared/vol1", got.Status.DevicePath)
	assert.Contains(t, got.Finalizers, internal.SdsNodeConfiguratorFinalizer)
}

func TestReadWriteManyUsesSharedActivation(t *testing.T) {
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	attachment := testAttachment(testNode, func(a *v1alpha1.LVMSharedLogicalVolumeAttachment) {
		a.Spec.AccessMode = v1alpha1.LVMSharedLogicalVolumeAccessModeRWX
	})
	activate, _ := dmView(t)
	r, _ := testReconciler(t, commands, nil, testGroup(), testVolume(), attachment)

	commands.EXPECT().LVActivateShared(gomock.Any(), testVG, []string{testLV}, true).DoAndReturn(
		func(_ context.Context, _ string, _ []string, _ bool) (string, error) {
			activate()
			return "lvchange -asy", nil
		})

	reconcile(t, r, attachment)
}

func TestAlreadyActiveVolumeIsNotActivatedAgain(t *testing.T) {
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	// No expectation: every lvm command against a shared group takes the group
	// lock, and the lock is what the pool's throughput is made of.
	attachment := testAttachment(testNode, withFinalizer)
	activate, _ := dmView(t)
	activate()
	r, cl := testReconciler(t, commands, []internal.LVData{activeLV()}, testGroup(), testVolume(), attachment)

	reconcile(t, r, attachment)

	assert.Equal(t, PhaseAttached, attachmentFromAPI(t, cl).Status.Phase)
}

func TestLockHeldElsewhereIsPendingNotFailed(t *testing.T) {
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	attachment := testAttachment(testNode)
	r, cl := testReconciler(t, commands, []internal.LVData{inactiveLV()}, testGroup(), testVolume(), attachment)

	commands.EXPECT().LVActivateShared(gomock.Any(), testVG, []string{testLV}, false).
		Return("lvchange -aey", errors.New("LV locked by other host"))

	res := reconcile(t, r, attachment)

	assert.NotZero(t, res.RequeueAfter)
	got := attachmentFromAPI(t, cl)
	assert.Equal(t, PhasePending, got.Status.Phase,
		"a volume still held by its previous node is a state of the pool, not a fault of this one")
	assert.Empty(t, got.Status.DevicePath)
}

func TestSilentActivationFailureIsCaught(t *testing.T) {
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	attachment := testAttachment(testNode)
	// The command returns zero and the volume is still inactive. One command
	// carries one exit code, so the set of active volumes is what decides.
	r, cl := testReconciler(t, commands, []internal.LVData{inactiveLV()}, testGroup(), testVolume(), attachment)

	commands.EXPECT().LVActivateShared(gomock.Any(), testVG, []string{testLV}, false).
		Return("lvchange -aey", nil)

	res := reconcile(t, r, attachment)

	assert.NotZero(t, res.RequeueAfter)
	assert.Equal(t, PhasePending, attachmentFromAPI(t, cl).Status.Phase)
}

func TestDetachDeactivatesBeforeReleasingTheObject(t *testing.T) {
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	attachment := testAttachment(testNode, deleting)
	activate, deactivate := dmView(t)
	activate()
	r, cl := testReconciler(t, commands, []internal.LVData{activeLV()}, testGroup(), testVolume(), attachment)

	commands.EXPECT().LVDeactivateShared(gomock.Any(), testVG, []string{testLV}).DoAndReturn(
		func(_ context.Context, _ string, _ []string) (string, error) {
			deactivate()
			return "lvchange -an", nil
		})

	reconcile(t, r, attachment)

	// The fake client removes an object once its last finalizer goes.
	err := cl.Get(context.Background(), client.ObjectKey{Name: "att-1"}, &v1alpha1.LVMSharedLogicalVolumeAttachment{})
	assert.True(t, err != nil, "the attachment is released only after the volume is")
}

func TestBusyVolumeKeepsTheAttachmentAlive(t *testing.T) {
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	attachment := testAttachment(testNode, deleting)
	activate, _ := dmView(t)
	activate()
	r, cl := testReconciler(t, commands, []internal.LVData{activeLV()}, testGroup(), testVolume(), attachment)

	commands.EXPECT().LVDeactivateShared(gomock.Any(), testVG, []string{testLV}).
		Return("lvchange -an", errors.New("Logical volume vgshared/vol1 in use"))

	res := reconcile(t, r, attachment)

	assert.NotZero(t, res.RequeueAfter)
	assert.Contains(t, attachmentFromAPI(t, cl).Finalizers, internal.SdsNodeConfiguratorFinalizer,
		"letting the object go would leave the lock held with nothing describing it")
}

func TestDetachWithoutVolumeReleasesTheObject(t *testing.T) {
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	// The volume is gone from the API, so there is nothing left to hold on to
	// and no command to run.
	attachment := testAttachment(testNode, deleting)
	r, cl := testReconciler(t, commands, nil, attachment)

	reconcile(t, r, attachment)

	err := cl.Get(context.Background(), client.ObjectKey{Name: "att-1"}, &v1alpha1.LVMSharedLogicalVolumeAttachment{})
	assert.True(t, err != nil)
}

func TestMissingVolumeIsWaitedFor(t *testing.T) {
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	// A PersistentVolumeClaim whose volume the metadata owner has not created
	// yet: wait, do not fail.
	attachment := testAttachment(testNode)
	r, _ := testReconciler(t, commands, nil, attachment)

	res := reconcile(t, r, attachment)
	assert.NotZero(t, res.RequeueAfter)
}

func TestConditionTimestampMovesOnlyOnChange(t *testing.T) {
	conditions := []metav1.Condition{}
	setCondition(&conditions, metav1.Condition{Type: ConditionReady, Status: metav1.ConditionFalse, Reason: "One"})
	first := conditions[0].LastTransitionTime

	setCondition(&conditions, metav1.Condition{Type: ConditionReady, Status: metav1.ConditionFalse, Reason: "Two"})
	assert.Equal(t, first, conditions[0].LastTransitionTime,
		"a timestamp that moves on every reconcile says nothing about when the state changed")
	assert.Equal(t, "Two", conditions[0].Reason)

	setCondition(&conditions, metav1.Condition{Type: ConditionReady, Status: metav1.ConditionTrue, Reason: "Three"})
	assert.NotEqual(t, first, conditions[0].LastTransitionTime)
}

func TestMassEventGoesInOneCommand(t *testing.T) {
	// A node coming back after a reboot has every attachment fire at once. The
	// first reconcile of that burst takes the group lock once for all of them;
	// the ones that follow find their volumes already active.
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)

	objects := []client.Object{testGroup(), testVolume(), testAttachment(testNode)}
	for i := 2; i <= 5; i++ {
		name := fmt.Sprintf("vol-%d", i)
		lv := fmt.Sprintf("vol%d", i)
		objects = append(objects, &v1alpha1.LVMSharedLogicalVolume{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec: v1alpha1.LVMSharedLogicalVolumeSpec{
				LVMSharedVolumeGroupName: "pool-1",
				ActualLVNameOnTheNode:    lv,
				Size:                     "10Gi",
				VolumeCleanup:            v1alpha1.VolumeCleanupDiscard,
			},
		})
		objects = append(objects, &v1alpha1.LVMSharedLogicalVolumeAttachment{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("att-%d", i)},
			Spec: v1alpha1.LVMSharedLogicalVolumeAttachmentSpec{
				LVMSharedLogicalVolumeName: name,
				NodeName:                   testNode,
				AccessMode:                 v1alpha1.LVMSharedLogicalVolumeAccessModeRWO,
			},
		})
	}
	// One attachment belongs to a neighbour and one asks for shared activation:
	// neither may be swept into this batch.
	objects = append(objects,
		&v1alpha1.LVMSharedLogicalVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "vol-elsewhere"},
			Spec: v1alpha1.LVMSharedLogicalVolumeSpec{
				LVMSharedVolumeGroupName: "pool-1", ActualLVNameOnTheNode: "elsewhere",
				Size: "10Gi", VolumeCleanup: v1alpha1.VolumeCleanupDiscard,
			},
		},
		&v1alpha1.LVMSharedLogicalVolumeAttachment{
			ObjectMeta: metav1.ObjectMeta{Name: "att-elsewhere"},
			Spec: v1alpha1.LVMSharedLogicalVolumeAttachmentSpec{
				LVMSharedLogicalVolumeName: "vol-elsewhere", NodeName: "other-node",
				AccessMode: v1alpha1.LVMSharedLogicalVolumeAccessModeRWO,
			},
		},
		&v1alpha1.LVMSharedLogicalVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "vol-rwx"},
			Spec: v1alpha1.LVMSharedLogicalVolumeSpec{
				LVMSharedVolumeGroupName: "pool-1", ActualLVNameOnTheNode: "rwx",
				Size: "10Gi", VolumeCleanup: v1alpha1.VolumeCleanupDiscard,
			},
		},
		&v1alpha1.LVMSharedLogicalVolumeAttachment{
			ObjectMeta: metav1.ObjectMeta{Name: "att-rwx"},
			Spec: v1alpha1.LVMSharedLogicalVolumeAttachmentSpec{
				LVMSharedLogicalVolumeName: "vol-rwx", NodeName: testNode,
				AccessMode: v1alpha1.LVMSharedLogicalVolumeAccessModeRWX,
			},
		},
	)

	attachment := testAttachment(testNode)
	r, _ := testReconciler(t, commands, nil, objects...)

	var got []string
	commands.EXPECT().LVActivateShared(gomock.Any(), testVG, gomock.Any(), false).DoAndReturn(
		func(_ context.Context, _ string, lvNames []string, _ bool) (string, error) {
			got = lvNames
			active := make([]internal.LVData, 0, len(lvNames))
			for _, name := range lvNames {
				active = append(active, internal.LVData{VGName: testVG, LVName: name, LVAttr: "-wi-a-----"})
			}
			r.sdsCache.StoreLVs(active, bytes.Buffer{})
			return "lvchange -aey", nil
		})

	reconcile(t, r, attachment)

	assert.Equal(t, testLV, got[0], "the volume of this reconcile comes first")
	assert.ElementsMatch(t, []string{testLV, "vol2", "vol3", "vol4", "vol5"}, got)
	assert.NotContains(t, got, "elsewhere", "a neighbour's volume is not ours to activate")
	assert.NotContains(t, got, "rwx", "shared and exclusive activation cannot share a command")
}

// --- growing an attached volume ---

func sizedLV(size string) internal.LVData {
	return internal.LVData{
		VGName: testVG, LVName: testLV, LVAttr: "-wi-a-----",
		LVSize: resource.MustParse(size),
	}
}

func volumeOfSize(size string) *v1alpha1.LVMSharedLogicalVolume {
	volume := testVolume()
	volume.Spec.Size = size
	return volume
}

func TestAttachedVolumeIsGrownByTheNodeThatHoldsTheLock(t *testing.T) {
	// Not by the metadata owner: lvextend takes the LV lock, and under lvmlockd
	// that lock is held exclusively by the activating node.
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	attachment := testAttachment(testNode, withFinalizer)
	// The volume is attached here, so the extension is this node's to make.
	activate, _ := dmView(t)
	activate()
	r, cl := testReconciler(t, commands, []internal.LVData{sizedLV("10Gi")},
		testGroup(), volumeOfSize("20Gi"), attachment)

	commands.EXPECT().LVExtendShared(gomock.Any(), testVG, testLV, "20Gi").DoAndReturn(
		func(_ context.Context, _, _, _ string) (string, error) {
			r.sdsCache.StoreLVs([]internal.LVData{sizedLV("20Gi")}, bytes.Buffer{})
			return "lvextend", nil
		})

	reconcile(t, r, attachment)

	got := attachmentFromAPI(t, cl)
	assert.Equal(t, PhaseAttached, got.Status.Phase)
	assert.Equal(t, "20Gi", got.Status.ObservedSize,
		"the consumer needs the new size before it grows anything on top of it")
}

func TestVolumeAtItsSizeIsNotExtended(t *testing.T) {
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	// No expectation: every lvm command against a shared group takes the group
	// lock, and the pool's throughput is made of those.
	attachment := testAttachment(testNode, withFinalizer)
	// The volume is attached here, so the extension is this node's to make.
	activate, _ := dmView(t)
	activate()
	r, cl := testReconciler(t, commands, []internal.LVData{sizedLV("10Gi")},
		testGroup(), volumeOfSize("10Gi"), attachment)

	reconcile(t, r, attachment)

	assert.Equal(t, "10Gi", attachmentFromAPI(t, cl).Status.ObservedSize)
}

func TestUnknownSizeIsNotAReasonToExtend(t *testing.T) {
	// The volume was just activated and this node's view of LVM has not caught
	// up. Extending on the strength of not knowing is the one thing that must not
	// happen: the next pass knows.
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	attachment := testAttachment(testNode, withFinalizer)
	// The volume is attached here, so the extension is this node's to make.
	activate, _ := dmView(t)
	activate()
	r, cl := testReconciler(t, commands, []internal.LVData{activeLV()},
		testGroup(), volumeOfSize("20Gi"), attachment)

	res := reconcile(t, r, attachment)

	assert.NotZero(t, res.RequeueAfter)
	assert.Equal(t, PhaseAttached, attachmentFromAPI(t, cl).Status.Phase,
		"the volume is usable at whatever size it has, so readiness does not wait for a resize")
}

func TestFailedExtensionKeepsTheVolumeUsable(t *testing.T) {
	// A group out of space is not this node's to fix, and the volume still works
	// at its current size.
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	attachment := testAttachment(testNode, withFinalizer)
	// The volume is attached here, so the extension is this node's to make.
	activate, _ := dmView(t)
	activate()
	r, cl := testReconciler(t, commands, []internal.LVData{sizedLV("10Gi")},
		testGroup(), volumeOfSize("20Gi"), attachment)

	commands.EXPECT().LVExtendShared(gomock.Any(), testVG, testLV, "20Gi").
		Return("lvextend", errors.New("Insufficient free space"))

	res := reconcile(t, r, attachment)

	assert.NotZero(t, res.RequeueAfter)
	assert.Equal(t, PhaseAttached, attachmentFromAPI(t, cl).Status.Phase)
}

func TestRoundedSizeIsReportedAsItIs(t *testing.T) {
	// lvm rounds up to whole extents, and with a large extent size the difference
	// is worth reporting rather than echoing the request back.
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	attachment := testAttachment(testNode, withFinalizer)
	// The volume is attached here, so the extension is this node's to make.
	activate, _ := dmView(t)
	activate()
	r, cl := testReconciler(t, commands, []internal.LVData{sizedLV("10Gi")},
		testGroup(), volumeOfSize("10241Mi"), attachment)

	commands.EXPECT().LVExtendShared(gomock.Any(), testVG, testLV, "10241Mi").DoAndReturn(
		func(_ context.Context, _, _, _ string) (string, error) {
			r.sdsCache.StoreLVs([]internal.LVData{sizedLV("10244Mi")}, bytes.Buffer{})
			return "lvextend", nil
		})

	reconcile(t, r, attachment)

	assert.Equal(t, "10244Mi", attachmentFromAPI(t, cl).Status.ObservedSize)
}

func TestActivationIsSeenWithoutWaitingForAScan(t *testing.T) {
	// The state is read back after the activation, and it used to be read from
	// the scan cache — which the activation does not touch. The attachment
	// therefore reported NotActiveYet forever on a volume that was active, and
	// the pod waiting for it never started. Nothing here fills a cache: the
	// device-mapper device appearing is the whole of the evidence.
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	attachment := testAttachment(testNode)
	activate, _ := dmView(t)
	r, cl := testReconciler(t, commands, nil, testGroup(), testVolume(), attachment)

	commands.EXPECT().LVActivateShared(gomock.Any(), testVG, []string{testLV}, false).DoAndReturn(
		func(_ context.Context, _ string, _ []string, _ bool) (string, error) {
			activate()
			return "lvchange -aey", nil
		})

	reconcile(t, r, attachment)

	got := attachmentFromAPI(t, cl)
	assert.Equal(t, PhaseAttached, got.Status.Phase)
	assert.Equal(t, "/dev/vgshared/vol1", got.Status.DevicePath)
}
