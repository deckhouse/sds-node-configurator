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

package lsvg

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

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
)

func testGroup(nodes ...string) *v1alpha1.LVMSharedVolumeGroup {
	return &v1alpha1.LVMSharedVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "pool-1"},
		Spec: v1alpha1.LVMSharedVolumeGroupSpec{
			ActualVGNameOnTheNode: testVG,
			Nodes:                 nodes,
			VolumeCleanup:         v1alpha1.VolumeCleanupDiscard,
		},
	}
}

func testReconciler(
	t *testing.T,
	node *corev1.Node,
	commands *mock_utils.MockCommands,
	lvs []internal.LVData,
	objects ...client.Object,
) (*Reconciler, client.Client, string) {
	t.Helper()

	s := scheme.Scheme
	require.NoError(t, v1alpha1.AddToScheme(s))
	require.NoError(t, corev1.AddToScheme(s))
	cl := fake.NewClientBuilder().WithScheme(s).
		WithObjects(append([]client.Object{node}, objects...)...).
		WithStatusSubresource(&v1alpha1.LVMSharedVolumeGroup{}).
		WithInterceptorFuncs(interceptor.Funcs{SubResourcePatch: applyNodeEntry}).
		Build()

	sdsCache := cache.New()
	sdsCache.StoreLVs(lvs, bytes.Buffer{})

	// The fallback answers: lvm cannot say anything about the group or the
	// devices. A test that needs a different answer declares it before calling
	// this helper, and gomock prefers the expectation declared first.
	commands.EXPECT().GetVG(gomock.Any()).Return(internal.VGData{}, "vgs", bytes.Buffer{}, nil).AnyTimes()
	commands.EXPECT().GetPV(gomock.Any()).Return(internal.PVData{}, "pvs", bytes.Buffer{}, nil).AnyTimes()

	log, err := logger.NewLogger(logger.WarningLevel)
	require.NoError(t, err)

	dir := t.TempDir()
	return NewReconciler(cl, log, sdsCache, commands, ReconcilerConfig{
		NodeName:  testNode,
		HostIDDir: dir,
	}), cl, dir
}

func nodeWith(annotations map[string]string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: testNode, Annotations: annotations}}
}

func activeLV(vg, lv string) internal.LVData {
	return internal.LVData{VGName: vg, LVName: lv, LVAttr: "-wi-a-----"}
}

func inactiveLV(vg, lv string) internal.LVData {
	return internal.LVData{VGName: vg, LVName: lv, LVAttr: "-wi-------"}
}

func reconcile(t *testing.T, r *Reconciler, group *v1alpha1.LVMSharedVolumeGroup) controller.Result {
	t.Helper()
	res, err := r.Reconcile(context.Background(), controller.ReconcileRequest[*v1alpha1.LVMSharedVolumeGroup]{Object: group})
	require.NoError(t, err)
	return res
}

func annotationsOf(t *testing.T, cl client.Client) map[string]string {
	t.Helper()
	node := &corev1.Node{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKey{Name: testNode}, node))
	return node.Annotations
}

func TestWaitsForTheAllocatorInsteadOfPickingAnID(t *testing.T) {
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	// No lock-start at all: an id this node chose for itself could collide with
	// a neighbour's, and two nodes on one delta lease is the failure the whole
	// design exists to prevent.
	r, cl, dir := testReconciler(t, nodeWith(nil), commands, nil)

	res := reconcile(t, r, testGroup(testNode))

	assert.NotZero(t, res.RequeueAfter)
	_, err := os.Stat(filepath.Join(dir, hostIDFileName))
	assert.True(t, os.IsNotExist(err), "no host id, no file")
	assert.NotContains(t, annotationsOf(t, cl), LockspaceStartedAnnotationPrefix+"pool-1")
}

func TestJoinWritesTheFileBeforeStartingTheLockspace(t *testing.T) {
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)

	r, cl, dir := testReconciler(t, nodeWith(map[string]string{SanlockHostIDAnnotation: "7"}), commands, nil)

	// lvmlockd reads the file when the lockspace starts, so the file has to be
	// there before the command runs, not after it.
	commands.EXPECT().VGLockStart(gomock.Any(), "vgshared", 7).DoAndReturn(
		func(_ context.Context, _ string, _ int) (string, error) {
			content, err := os.ReadFile(filepath.Join(dir, hostIDFileName))
			require.NoError(t, err)
			assert.Equal(t, "host_id = 7\n", string(content))
			return "vgchange --lock-start", nil
		})

	reconcile(t, r, testGroup(testNode))

	assert.Contains(t, annotationsOf(t, cl), LockspaceStartedAnnotationPrefix+"pool-1",
		"the value is the group's uuid, so the fact is its presence")
}

func TestJoinIsIdempotent(t *testing.T) {
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	node := nodeWith(map[string]string{
		SanlockHostIDAnnotation:                     "7",
		LockspaceStartedAnnotationPrefix + "pool-1": "true",
	})
	// Already started: no second lock-start, which on a live pool would be a
	// command that blocks for minutes for nothing.
	r, _, _ := testReconciler(t, node, commands, nil)

	reconcile(t, r, testGroup(testNode))
}

func TestFailedLockStartLeavesTheNodeNotReady(t *testing.T) {
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	commands.EXPECT().VGLockStart(gomock.Any(), "vgshared", 7).
		Return("vgchange --lock-start", errors.New("lockstart failed"))

	r, cl, _ := testReconciler(t, nodeWith(map[string]string{SanlockHostIDAnnotation: "7"}), commands, nil)

	res := reconcile(t, r, testGroup(testNode))

	assert.NotZero(t, res.RequeueAfter, "a LUN that is not visible yet is retried, not escalated")
	assert.NotContains(t, annotationsOf(t, cl), LockspaceStartedAnnotationPrefix+"pool-1",
		"readiness must not be claimed for a lockspace that did not start")
}

func TestLeaveWaitsWhileSomethingStillAsksForTheVolume(t *testing.T) {
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	// Neither VGLockStop nor a barrier: the attachment reconciler is releasing
	// the volume, and one more pass is all this needs.
	node := nodeWith(map[string]string{
		SanlockHostIDAnnotation:                     "7",
		LockspaceStartedAnnotationPrefix + "pool-1": "true",
	})
	group := testGroup("other-node")
	volume := &v1alpha1.LVMSharedLogicalVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "vol-1"},
		Spec: v1alpha1.LVMSharedLogicalVolumeSpec{
			LVMSharedVolumeGroupName: group.Name,
			ActualLVNameOnTheNode:    "vol1",
			Size:                     "1Gi",
		},
	}
	attachment := &v1alpha1.LVMSharedLogicalVolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "att-1"},
		Spec: v1alpha1.LVMSharedLogicalVolumeAttachmentSpec{
			LVMSharedLogicalVolumeName: "vol-1",
			NodeName:                   testNode,
		},
	}
	fakeSysBlockWithLUN(t, 8192)
	fakeActiveLV(t, "vol1")
	r, cl, _ := testReconciler(t, node, commands, nil, volume, attachment)

	res := reconcile(t, r, group)

	assert.NotZero(t, res.RequeueAfter)
	assert.Contains(t, annotationsOf(t, cl), LockspaceStartedAnnotationPrefix+"pool-1",
		"the node still holds the lockspace, so it must still say so")
}

func TestALiveStuckNodeRaisesTheBarrierOverItsOwnVolumes(t *testing.T) {
	// kubelet is gone, its pods were never removed, their mounts hold the
	// volumes, and sanlock renews the lease for as long as the node breathes.
	// The pool has already taken the node out. Waiting here is waiting for a
	// human, which this platform does not do.
	//
	// The order is the safety: the barrier first, the leases after. Data is not
	// damaged either way — a write fails instead of landing on a volume that by
	// then may belong to somebody else.
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	node := nodeWith(map[string]string{
		SanlockHostIDAnnotation:                     "7",
		LockspaceStartedAnnotationPrefix + "pool-1": "true",
	})
	fakeSysBlockWithLUN(t, 8192)
	fakeActiveLV(t, "vol1")
	r, cl, _ := testReconciler(t, node, commands, nil)

	gomock.InOrder(
		commands.EXPECT().WipeDMTable(gomock.Any(), utils.DMName(testVG, "vol1")).
			Return("dmsetup wipe_table", nil),
		commands.EXPECT().VGLockStop(gomock.Any(), testVG).Return("vgchange --lock-stop", nil),
	)

	reconcile(t, r, testGroup("other-node"))

	assert.NotContains(t, annotationsOf(t, cl), LockspaceStartedAnnotationPrefix+"pool-1",
		"the node holds nothing now and must not claim otherwise")
}

func TestAnIncompleteBarrierLeavesTheLeasesAlone(t *testing.T) {
	// One map short of a barrier is not a barrier: a write could still reach the
	// array through it, so the leases stay where they are and the node tries
	// again rather than letting another node have the volume.
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	node := nodeWith(map[string]string{
		SanlockHostIDAnnotation:                     "7",
		LockspaceStartedAnnotationPrefix + "pool-1": "true",
	})
	fakeSysBlockWithLUN(t, 8192)
	fakeActiveLV(t, "vol1")
	r, cl, _ := testReconciler(t, node, commands, nil)

	// No VGLockStop expectation: the leases must not go.
	commands.EXPECT().WipeDMTable(gomock.Any(), utils.DMName(testVG, "vol1")).
		Return("dmsetup wipe_table", errors.New("device busy"))

	res := reconcile(t, r, testGroup("other-node"))

	assert.NotZero(t, res.RequeueAfter)
	assert.Contains(t, annotationsOf(t, cl), LockspaceStartedAnnotationPrefix+"pool-1")
}

func TestLeaveDropsReadinessBeforeStoppingTheLockspace(t *testing.T) {
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	node := nodeWith(map[string]string{
		SanlockHostIDAnnotation:                     "7",
		LockspaceStartedAnnotationPrefix + "pool-1": "true",
	})
	r, cl, _ := testReconciler(t, node, commands, []internal.LVData{inactiveLV("vgshared", "vol1")})

	commands.EXPECT().VGLockStop(gomock.Any(), "vgshared").DoAndReturn(
		func(_ context.Context, _ string) (string, error) {
			assert.NotContains(t, annotationsOf(t, cl), LockspaceStartedAnnotationPrefix+"pool-1",
				"nothing new must be scheduled here while the lockspace is going away")
			return "vgchange --lock-stop", nil
		})

	reconcile(t, r, testGroup("other-node"))
}

func TestVolumesOfOtherGroupsDoNotBlockLeaving(t *testing.T) {
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	node := nodeWith(map[string]string{
		SanlockHostIDAnnotation:                     "7",
		LockspaceStartedAnnotationPrefix + "pool-1": "true",
	})
	r, _, _ := testReconciler(t, node, commands, []internal.LVData{activeLV("vglocal", "somelv")})

	commands.EXPECT().VGLockStop(gomock.Any(), "vgshared").Return("vgchange --lock-stop", nil)

	reconcile(t, r, testGroup("other-node"))
}

func TestLeaveIsIdempotent(t *testing.T) {
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	// Never joined, so there is nothing to stop and no command to run.
	r, _, _ := testReconciler(t, nodeWith(nil), commands, nil)

	reconcile(t, r, testGroup("other-node"))
}

func TestHostIDFileIsNotRewrittenWhenUnchanged(t *testing.T) {
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	node := nodeWith(map[string]string{
		SanlockHostIDAnnotation:                     "7",
		LockspaceStartedAnnotationPrefix + "pool-1": "true",
	})
	r, _, dir := testReconciler(t, node, commands, nil)

	path := filepath.Join(dir, hostIDFileName)
	require.NoError(t, os.WriteFile(path, []byte("host_id = 7\n"), 0o644))
	before, err := os.Stat(path)
	require.NoError(t, err)

	reconcile(t, r, testGroup(testNode))

	after, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, before.ModTime(), after.ModTime(),
		"rewriting the file under a running lockspace is a change nobody asked for")
}

func TestUnreadableHostIDIsAnError(t *testing.T) {
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	r, _, _ := testReconciler(t, nodeWith(map[string]string{SanlockHostIDAnnotation: "zero"}), commands, nil)

	_, err := r.Reconcile(context.Background(),
		controller.ReconcileRequest[*v1alpha1.LVMSharedVolumeGroup]{Object: testGroup(testNode)})
	require.Error(t, err)
}

// --- the metadata owner's half: creating the group ---

func ownedGroup(opts ...func(*v1alpha1.LVMSharedVolumeGroup)) *v1alpha1.LVMSharedVolumeGroup {
	g := testGroup(testNode)
	g.Spec.MetadataOwner = testNode
	g.Spec.Devices = []v1alpha1.LVMSharedVolumeGroupDevice{{WWID: "36c89f1a1"}}
	g.Spec.LVM = &v1alpha1.LVMSharedVolumeGroupLVMSpec{
		PhysicalExtentSize: "4Mi",
		SanlockAlignSize:   "4Mi",
	}
	for _, opt := range opts {
		opt(g)
	}
	return g
}

func fakeSysBlockWithLUN(t *testing.T, granularity int) {
	t.Helper()
	root := t.TempDir()
	base := filepath.Join(root, "dm-3")
	require.NoError(t, os.MkdirAll(filepath.Join(base, "dm"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(base, "queue"), 0o755))
	write := func(p, v string) {
		require.NoError(t, os.WriteFile(filepath.Join(base, p), []byte(v+"\n"), 0o644))
	}
	write("dm/uuid", "mpath-36c89f1a1")
	write("dm/name", "mpathi")
	write("queue/logical_block_size", "512")
	write("queue/physical_block_size", "512")
	write("queue/discard_granularity", strconv.Itoa(granularity))

	old := utils.SysBlockRoot
	utils.SysBlockRoot = root
	t.Cleanup(func() { utils.SysBlockRoot = old })
}

// fakeActiveLV adds a device-mapper device for a volume of the group, which is
// what "active on this node" means and the only thing the reconciler reads.
func fakeActiveLV(t *testing.T, lvName string) {
	t.Helper()
	root := utils.SysBlockRoot
	// A directory of its own per call: several volumes of a group can be mapped
	// at once, and a helper that reuses one slot can only ever describe one.
	entries, err := os.ReadDir(root)
	require.NoError(t, err)
	base := filepath.Join(root, "dm-"+strconv.Itoa(20+len(entries)))
	require.NoError(t, os.MkdirAll(filepath.Join(base, "dm"), 0o755))
	mangled := strings.ReplaceAll(testVG, "-", "--") + "-" + strings.ReplaceAll(lvName, "-", "--")
	require.NoError(t, os.WriteFile(filepath.Join(base, "dm", "name"), []byte(mangled+"\n"), 0o644))
}

func TestOwnerCreatesTheGroupAndItStartsItsOwnLockspace(t *testing.T) {
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	r, cl, _ := testReconciler(t, nodeWith(map[string]string{SanlockHostIDAnnotation: "7"}), commands, nil)

	// No VGLockStart expectation: vgcreate --shared starts the lockspace itself,
	// so running it afterwards would be a command that blocks for nothing.
	commands.EXPECT().CreateVGShared(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, params utils.SharedVGParams) (string, error) {
			assert.Equal(t, testVG, params.VGName)
			assert.Equal(t, []string{"/dev/mapper/mpathi"}, params.PVPaths)
			assert.Equal(t, 7, params.HostID, "the client checks host_id against the alignment ceiling itself")
			assert.Equal(t, 4, params.SanlockAlignSizeMiB)
			assert.Equal(t, "4Mi", params.PhysicalExtentSize)
			return "vgcreate --shared", nil
		})

	reconcile(t, r, ownedGroup())

	assert.Contains(t, annotationsOf(t, cl), LockspaceStartedAnnotationPrefix+"pool-1",
		"the value is the group's uuid, so the fact is its presence")
}

func TestGroupIsNotRecreatedOverAnExistingOne(t *testing.T) {
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	// The physical volume label already names a group, which is the only proof
	// that counts: under lvmlockd a skipped group looks exactly like an absent
	// one to vgs.
	// lvm says the group is there, which is the only source that can say it about
	// a group created a moment ago.
	commands.EXPECT().GetVG(testVG).Return(internal.VGData{VGName: testVG, VGUUID: "vg-uuid"}, "vgs", bytes.Buffer{}, nil).AnyTimes()
	group := ownedGroup()
	r, _, _ := testReconciler(t, nodeWith(map[string]string{SanlockHostIDAnnotation: "7"}), commands, nil, group)

	commands.EXPECT().VGLockStart(gomock.Any(), testVG, 7).Return("vgchange --lock-start", nil)

	reconcile(t, r, group)
}

func TestTheOwnerPublishesWhatItObservesAboutTheGroup(t *testing.T) {
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := ownedGroup()

	s := scheme.Scheme
	require.NoError(t, v1alpha1.AddToScheme(s))
	require.NoError(t, corev1.AddToScheme(s))
	node := nodeWith(map[string]string{
		SanlockHostIDAnnotation: "7",
		// The value is the group's identity: a name alone would let a group
		// destroyed and created again inherit this node's old claim.
		LockspaceStartedAnnotationPrefix + group.Name: "vg-uuid",
	})
	cl := fake.NewClientBuilder().WithScheme(s).
		WithObjects(node, group).WithStatusSubresource(group).Build()

	commands.EXPECT().GetVG(testVG).Return(internal.VGData{
		VGName:       testVG,
		VGUUID:       "vg-uuid",
		VGSize:       resource.MustParse("200Gi"),
		VGFree:       resource.MustParse("197Gi"),
		VGExtentSize: resource.MustParse("4Mi"),
	}, "vgs", bytes.Buffer{}, nil).AnyTimes()

	sdsCache := cache.New()
	sdsCache.StorePVs([]internal.PVData{{PVName: "/dev/mapper/mpathi", VGName: testVG}}, bytes.Buffer{})
	sdsCache.StoreLVs([]internal.LVData{
		{VGName: testVG, LVName: "lvmlock", LVSize: resource.MustParse("256Mi")},
		{VGName: testVG, LVName: "volume-1"},
	}, bytes.Buffer{})

	log, err := logger.NewLogger(logger.WarningLevel)
	require.NoError(t, err)
	r := NewReconciler(cl, log, sdsCache, commands, ReconcilerConfig{NodeName: testNode, HostIDDir: t.TempDir()})

	reconcile(t, r, group)

	published := &v1alpha1.LVMSharedVolumeGroup{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKey{Name: group.Name}, published))
	require.NotNil(t, published.Status, "without this the pool above has only the object to go on, and an object is not a volume group")
	assert.Equal(t, "Created", published.Status.Phase)
	assert.Equal(t, "vg-uuid", published.Status.VGUUID)
	assert.Equal(t, int32(1), published.Status.LogicalVolumeCount, "the lease area is not a volume of the pool")
	assert.NotEmpty(t, published.Status.LeaseAreaSize)
}

func TestForeignGroupOnTheLUNIsRefused(t *testing.T) {
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	// A group that is not the one the pool asks for. Found on the stand: the LUN
	// had been used for something else, and the check that only asked "is there a
	// group here" would have declared the pool's group ready without it existing.
	commands.EXPECT().GetPV(gomock.Any()).
		Return(internal.PVData{PVName: "/dev/mapper/mpathi", VGName: "someone-elses-vg"}, "pvs", bytes.Buffer{}, nil).AnyTimes()
	r, _, _ := testReconciler(t, nodeWith(map[string]string{SanlockHostIDAnnotation: "7"}), commands, nil)

	_, err := r.Reconcile(context.Background(),
		controller.ReconcileRequest[*v1alpha1.LVMSharedVolumeGroup]{Object: ownedGroup()})

	require.Error(t, err, "neither creating over it nor adopting it is defensible")
	assert.Contains(t, err.Error(), "someone-elses-vg")
}

func TestMissingLUNPostponesCreation(t *testing.T) {
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	r, cl, _ := testReconciler(t, nodeWith(map[string]string{SanlockHostIDAnnotation: "7"}), commands, nil)

	group := ownedGroup(func(g *v1alpha1.LVMSharedVolumeGroup) {
		g.Spec.Devices = append(g.Spec.Devices, v1alpha1.LVMSharedVolumeGroupDevice{WWID: "not-here-yet"})
	})

	res := reconcile(t, r, group)

	assert.NotZero(t, res.RequeueAfter, "an array that has not presented the LUN yet resolves itself")
	assert.NotContains(t, annotationsOf(t, cl), LockspaceStartedAnnotationPrefix+"pool-1")
}

func TestUnusableExtentSizeIsAHardStop(t *testing.T) {
	// 3 KiB granularity against a 4 MiB extent: discard would free nothing, and
	// the extent size cannot be changed once the group exists.
	fakeSysBlockWithLUN(t, 3*1024)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	r, _, _ := testReconciler(t, nodeWith(map[string]string{SanlockHostIDAnnotation: "7"}), commands, nil)

	_, err := r.Reconcile(context.Background(),
		controller.ReconcileRequest[*v1alpha1.LVMSharedVolumeGroup]{Object: ownedGroup()})

	require.Error(t, err, "retrying would not make an irreversible parameter correct")
	assert.Contains(t, err.Error(), "discard would free nothing")
}

func TestNonOwnerNeverCreatesTheGroup(t *testing.T) {
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	// Two nodes racing to create one group on one LUN is not a race that ends
	// well, so only the owner may.
	r, _, _ := testReconciler(t, nodeWith(map[string]string{SanlockHostIDAnnotation: "7"}), commands, nil)

	group := ownedGroup(func(g *v1alpha1.LVMSharedVolumeGroup) { g.Spec.MetadataOwner = "other-node" })
	commands.EXPECT().VGLockStart(gomock.Any(), testVG, 7).Return("vgchange --lock-start", nil)

	reconcile(t, r, group)
}

func TestAnUnnamedGroupIsNotAForeignGroup(t *testing.T) {
	// What the LUN looks like after a vgcreate that labelled the physical volume
	// and then failed: a PV "marked in use but no VG was found using it", which
	// lvm reports as "[unknown]". Found on the stand, where the module's own
	// debris made it refuse to create anything, forever.
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	commands.EXPECT().GetPV(gomock.Any()).
		Return(internal.PVData{PVName: "/dev/mapper/mpathi", VGName: "[unknown]"}, "pvs", bytes.Buffer{}, nil).AnyTimes()
	r, _, _ := testReconciler(t, nodeWith(map[string]string{SanlockHostIDAnnotation: "7"}), commands, nil)
	commands.EXPECT().CreateVGShared(gomock.Any(), gomock.Any()).Return("vgcreate --shared", nil)

	reconcile(t, r, ownedGroup())
}

func TestARecreatedGroupDoesNotInheritTheOldLockspaceClaim(t *testing.T) {
	// Found on the stand: a pool being commissioned had its group destroyed and
	// created again under the same name. Two of three nodes still carried
	// "lockspace started" from the previous group, never started the new one,
	// and the pool looked healthy with a single member actually holding leases.
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	commands.EXPECT().GetVG(testVG).
		Return(internal.VGData{VGName: testVG, VGUUID: "the-new-one"}, "vgs", bytes.Buffer{}, nil).AnyTimes()

	group := testGroup(testNode)
	node := nodeWith(map[string]string{
		SanlockHostIDAnnotation:                       "7",
		LockspaceStartedAnnotationPrefix + group.Name: "the-old-one",
	})
	r, cl, _ := testReconciler(t, node, commands, nil, group)

	// The lockspace of the group that exists now has to be started here.
	commands.EXPECT().VGLockStart(gomock.Any(), testVG, 7).Return("vgchange --lock-start", nil)

	reconcile(t, r, group)

	assert.Equal(t, "the-new-one", annotationsOf(t, cl)[LockspaceStartedAnnotationPrefix+group.Name])
}

func TestAStartedLockspaceReleasesMappingsNobodyAskedFor(t *testing.T) {
	// The residue of a lock-daemon restart: sanlock and lvmlockd went down
	// together and took every lease, the kernel kept every mapping. On the stand
	// this left a volume mapped on a node that had never asked for it — a device
	// with no lock behind it, which is exactly what lets a second node write to
	// a volume this one still shows as active.
	//
	// The moment to clean it up is right after the lockspace starts: the leases
	// are fresh, so everything mapped from before is known to be unlocked.
	fakeSysBlockWithLUN(t, 8192)
	fakeActiveLV(t, "stray")
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := testGroup(testNode)
	r, _, _ := testReconciler(t, nodeWith(map[string]string{SanlockHostIDAnnotation: "7"}), commands, nil, group)

	commands.EXPECT().VGLockStart(gomock.Any(), testVG, 7).Return("vgchange --lock-start", nil)
	commands.EXPECT().LVDeactivateShared(gomock.Any(), testVG, []string{"stray"}).
		Return("lvchange -an", nil)
	// lvchange cannot be taken at its word: it reports success and leaves the
	// mapping standing, so the device is torn down by name.
	commands.EXPECT().RemoveDMDevice(gomock.Any(), utils.DMName(testVG, "stray")).
		Return("dmsetup remove", nil)

	reconcile(t, r, group)
}

func TestAVolumeWithAnAttachmentIsLeftAlone(t *testing.T) {
	// The other half: a mapping that something asked for stays. Deactivating it
	// would take the volume from under a running pod, and the attachment is the
	// statement that the pod is meant to have it.
	fakeSysBlockWithLUN(t, 8192)
	fakeActiveLV(t, "vol1")
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := testGroup(testNode)
	volume := &v1alpha1.LVMSharedLogicalVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "vol-1"},
		Spec: v1alpha1.LVMSharedLogicalVolumeSpec{
			LVMSharedVolumeGroupName: group.Name,
			ActualLVNameOnTheNode:    "vol1",
			Size:                     "1Gi",
		},
	}
	attachment := &v1alpha1.LVMSharedLogicalVolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "att-1"},
		Spec: v1alpha1.LVMSharedLogicalVolumeAttachmentSpec{
			LVMSharedLogicalVolumeName: "vol-1",
			NodeName:                   testNode,
		},
	}
	r, _, _ := testReconciler(t, nodeWith(map[string]string{SanlockHostIDAnnotation: "7"}), commands,
		nil, group, volume, attachment)

	// No LVDeactivateShared expectation: this volume belongs here.
	commands.EXPECT().VGLockStart(gomock.Any(), testVG, 7).Return("vgchange --lock-start", nil)

	reconcile(t, r, group)
}

func TestALockspaceStartedBeforeThisAgentIsTakenStockOf(t *testing.T) {
	// The upgrade path, and the one the stand caught: the lockspace was already
	// started, so the code that counts incarnations never ran — no number was
	// written, no residue was released, and the attachment reconciler compared
	// two zeroes and concluded the lock was held.
	//
	// A lockspace started before this agent could count it is indistinguishable
	// from one whose leases were lost: the node adopts a number now and treats
	// what it finds as residue.
	fakeSysBlockWithLUN(t, 8192)
	fakeActiveLV(t, "stray")
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := testGroup(testNode)
	commands.EXPECT().GetVG(testVG).
		Return(internal.VGData{VGName: testVG, VGUUID: "vg-uuid"}, "vgs", bytes.Buffer{}, nil).AnyTimes()

	node := nodeWith(map[string]string{
		SanlockHostIDAnnotation:                       "7",
		LockspaceStartedAnnotationPrefix + group.Name: "vg-uuid",
	})
	r, cl, _ := testReconciler(t, node, commands, nil, group)

	// No VGLockStart: the lockspace is already up. The residue goes, though.
	commands.EXPECT().LVDeactivateShared(gomock.Any(), testVG, []string{"stray"}).
		Return("lvchange -an", nil)
	commands.EXPECT().RemoveDMDevice(gomock.Any(), utils.DMName(testVG, "stray")).
		Return("dmsetup remove", nil)

	reconcile(t, r, group)

	assert.Equal(t, "1", annotationsOf(t, cl)[LockspaceGenerationAnnotationPrefix+group.Name],
		"the node now has an incarnation to compare against")
}

func TestAVolumeWithAnAttachmentKeepsItsMappingThroughTheCleanup(t *testing.T) {
	// The cleanup now reaches for dmsetup, which does not consult any lock and
	// would happily tear down a device a pod is using. The attachment is the
	// only thing standing between the two, so it is worth its own test.
	fakeSysBlockWithLUN(t, 8192)
	fakeActiveLV(t, "vol1")
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := testGroup(testNode)
	volume := &v1alpha1.LVMSharedLogicalVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "vol-1"},
		Spec: v1alpha1.LVMSharedLogicalVolumeSpec{
			LVMSharedVolumeGroupName: group.Name,
			ActualLVNameOnTheNode:    "vol1",
			Size:                     "1Gi",
		},
	}
	attachment := &v1alpha1.LVMSharedLogicalVolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "att-1"},
		Spec: v1alpha1.LVMSharedLogicalVolumeAttachmentSpec{
			LVMSharedLogicalVolumeName: "vol-1",
			NodeName:                   testNode,
		},
	}
	r, _, _ := testReconciler(t, nodeWith(map[string]string{SanlockHostIDAnnotation: "7"}), commands,
		nil, group, volume, attachment)

	// Neither the deactivation nor the removal: this volume belongs here.
	commands.EXPECT().VGLockStart(gomock.Any(), testVG, 7).Return("vgchange --lock-start", nil)

	reconcile(t, r, group)
}

func TestResidueIsReleasedOnEveryPassNotOnlyTheFirst(t *testing.T) {
	// Found on the stand after the first fix: the node had already adopted an
	// incarnation, so the branch that cleaned up never ran again — and the
	// mappings nobody asked for stayed. Residue appears whenever the lock
	// daemons restart, which this reconciler does not witness, so the cleanup
	// belongs to the state and not to a first sighting.
	fakeSysBlockWithLUN(t, 8192)
	fakeActiveLV(t, "stray")
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := testGroup(testNode)
	commands.EXPECT().GetVG(testVG).
		Return(internal.VGData{VGName: testVG, VGUUID: "vg-uuid"}, "vgs", bytes.Buffer{}, nil).AnyTimes()

	// A node that has counted this lockspace already: nothing here is new.
	node := nodeWith(map[string]string{
		SanlockHostIDAnnotation:                          "7",
		LockspaceStartedAnnotationPrefix + group.Name:    "vg-uuid",
		LockspaceGenerationAnnotationPrefix + group.Name: "4",
	})
	r, cl, _ := testReconciler(t, node, commands, nil, group)

	commands.EXPECT().LVDeactivateShared(gomock.Any(), testVG, []string{"stray"}).
		Return("lvchange -an", nil)
	commands.EXPECT().RemoveDMDevice(gomock.Any(), utils.DMName(testVG, "stray")).
		Return("dmsetup remove", nil)

	reconcile(t, r, group)

	assert.Equal(t, "4", annotationsOf(t, cl)[LockspaceGenerationAnnotationPrefix+group.Name],
		"nothing restarted, so the incarnation stands")
}

func TestAMemberLooksAtItselfAgainWithoutBeingAsked(t *testing.T) {
	// Every state this reconciler repairs — a mapping left by a lock-daemon
	// restart, a node returning from a barrier, a lockspace nobody counted —
	// produces no event on the object being watched, and the watch itself fires
	// only on membership and on the group's name. Without a period of its own a
	// node finishes one good pass and is never called again: measured on the
	// stand, where an orphan mapping outlived the pass that found it by hours.
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	// A member that does not own the metadata: the pass has nothing to create
	// and nothing to publish, which is precisely the pass that used to end the
	// node's day.
	group := testGroup(testNode)
	r, _, _ := testReconciler(t, nodeWith(map[string]string{
		SanlockHostIDAnnotation:                          "7",
		LockspaceGenerationAnnotationPrefix + group.Name: "1",
	}), commands, nil, group)

	commands.EXPECT().CreateVGShared(gomock.Any(), gomock.Any()).Return("vgcreate", nil).AnyTimes()
	commands.EXPECT().VGLockStart(gomock.Any(), testVG, 7).Return("vgchange --lock-start", nil).AnyTimes()
	commands.EXPECT().GetVG(testVG).Return(internal.VGData{VGName: testVG, VGUUID: "vg-uuid"}, "vgs", bytes.Buffer{}, nil).AnyTimes()

	res := reconcile(t, r, group)

	assert.Equal(t, time.Minute, res.RequeueAfter,
		"a successful pass is not a reason to stop looking")
}

// applyNodeEntry stands in for the one thing the fake client cannot do: a
// server-side apply of a single entry in a list keyed by name.
//
// The real merge belongs to the API server, and what the tests need from it is
// its outcome — this node's entry replaced, every other node's entry left
// alone, and an empty list from a manager meaning "withdraw mine". Emulating
// exactly that keeps the tests about the reconciler rather than about apply.
func applyNodeEntry(
	ctx context.Context,
	cl client.Client,
	_ string,
	obj client.Object,
	patch client.Patch,
	opts ...client.SubResourcePatchOption,
) error {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok || patch.Type() != types.ApplyPatchType {
		return cl.Status().Patch(ctx, obj, patch, opts...)
	}

	group := &v1alpha1.LVMSharedVolumeGroup{}
	if err := cl.Get(ctx, client.ObjectKey{Name: u.GetName()}, group); err != nil {
		return err
	}
	if group.Status == nil {
		group.Status = &v1alpha1.LVMSharedVolumeGroupStatus{}
	}

	entries, _, _ := unstructured.NestedSlice(u.Object, "status", "nodes")

	// The applied set is what this manager owns, so its own previous entries go
	// and everybody else's stay.
	kept := group.Status.Nodes[:0]
	for _, node := range group.Status.Nodes {
		if node.Name != testNode {
			kept = append(kept, node)
		}
	}
	group.Status.Nodes = kept

	for _, raw := range entries {
		entry, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		name, _ := entry["name"].(string)
		started, _ := entry["lockspaceStarted"].(bool)
		reason, _ := entry["reason"].(string)
		message, _ := entry["message"].(string)
		group.Status.Nodes = append(group.Status.Nodes, v1alpha1.LVMSharedVolumeGroupNodeStatus{
			Name:             name,
			LockspaceStarted: started,
			Reason:           reason,
			Message:          message,
			Since:            metav1.Now(),
		})
	}

	return cl.Status().Update(ctx, group)
}

func TestAReleasedVolumeThatDidNotGoIsAskedAboutByName(t *testing.T) {
	// lvchange decides whether a volume is active here from the lock it holds,
	// not from device-mapper: with the lease gone it finds nothing to do, exits
	// zero, and leaves the mapping standing. The fallback has to reach dmsetup.
	//
	// It asks about the volume it just released, by name. Listing what is active
	// a second time is what the stand disproved: the node logged the release
	// every minute for hours and never reached the removal, because the second
	// listing came back empty with the mapping plainly there.
	fakeSysBlockWithLUN(t, 8192)
	fakeActiveLV(t, "vol1")
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := testGroup(testNode)
	r, _, _ := testReconciler(t, nodeWith(map[string]string{
		SanlockHostIDAnnotation:                          "7",
		LockspaceGenerationAnnotationPrefix + group.Name: "1",
	}), commands, nil, group)

	commands.EXPECT().LVDeactivateShared(gomock.Any(), testVG, []string{"vol1"}).Return("lvchange -an", nil)
	commands.EXPECT().RemoveDMDevice(gomock.Any(), utils.DMName(testVG, "vol1")).Return("dmsetup remove", nil)
	commands.EXPECT().VGLockStart(gomock.Any(), gomock.Any(), gomock.Any()).Return("", nil).AnyTimes()
	commands.EXPECT().CreateVGShared(gomock.Any(), gomock.Any()).Return("", nil).AnyTimes()

	r.releaseOrphanActivations(context.Background(), group)
}

func TestAReleasedVolumeThatWentIsRemovedWithoutComplaint(t *testing.T) {
	// The ordinary outcome: the deactivation worked and the mapping is gone.
	// The removal still runs — asking device-mapper is cheaper and more truthful
	// than deciding from a directory listing whether it needs to — and
	// "no such device" is what success looks like from here.
	fakeSysBlockWithLUN(t, 8192)
	fakeActiveLV(t, "vol1")
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	group := testGroup(testNode)
	r, _, _ := testReconciler(t, nodeWith(map[string]string{
		SanlockHostIDAnnotation:                          "7",
		LockspaceGenerationAnnotationPrefix + group.Name: "1",
	}), commands, nil, group)

	commands.EXPECT().LVDeactivateShared(gomock.Any(), testVG, []string{"vol1"}).Return("lvchange -an", nil)
	// The real command reports the missing device as success, so the reconciler
	// sees no error and says nothing.
	commands.EXPECT().RemoveDMDevice(gomock.Any(), utils.DMName(testVG, "vol1")).Return("dmsetup remove", nil)
	commands.EXPECT().VGLockStart(gomock.Any(), gomock.Any(), gomock.Any()).Return("", nil).AnyTimes()
	commands.EXPECT().CreateVGShared(gomock.Any(), gomock.Any()).Return("", nil).AnyTimes()

	r.releaseOrphanActivations(context.Background(), group)
}
