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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	corev1 "k8s.io/api/core/v1"
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
) (*Reconciler, client.Client, string) {
	t.Helper()

	s := scheme.Scheme
	require.NoError(t, v1alpha1.AddToScheme(s))
	require.NoError(t, corev1.AddToScheme(s))
	cl := fake.NewClientBuilder().WithScheme(s).WithObjects(node).Build()

	sdsCache := cache.New()
	sdsCache.StoreLVs(lvs, bytes.Buffer{})

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

	assert.Equal(t, "true", annotationsOf(t, cl)[LockspaceStartedAnnotationPrefix+"pool-1"])
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

func TestLeaveRefusesWhileAVolumeIsActive(t *testing.T) {
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	// No VGLockStop expectation: stopping the lockspace under an active volume
	// leaves the volume writable with no lock behind it.
	node := nodeWith(map[string]string{
		SanlockHostIDAnnotation:                     "7",
		LockspaceStartedAnnotationPrefix + "pool-1": "true",
	})
	r, cl, _ := testReconciler(t, node, commands, []internal.LVData{activeLV("vgshared", "vol1")})

	res := reconcile(t, r, testGroup("other-node"))

	assert.NotZero(t, res.RequeueAfter)
	assert.Equal(t, "true", annotationsOf(t, cl)[LockspaceStartedAnnotationPrefix+"pool-1"],
		"the node still holds the lockspace, so it must still say so")
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

	assert.Equal(t, "true", annotationsOf(t, cl)[LockspaceStartedAnnotationPrefix+"pool-1"])
}

func TestGroupIsNotRecreatedOverAnExistingOne(t *testing.T) {
	fakeSysBlockWithLUN(t, 8192)
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	// The physical volume label already names a group, which is the only proof
	// that counts: under lvmlockd a skipped group looks exactly like an absent
	// one to vgs.
	r, _, _ := testReconciler(t, nodeWith(map[string]string{SanlockHostIDAnnotation: "7"}), commands, nil)
	r.sdsCache.StorePVs([]internal.PVData{{PVName: "/dev/mapper/mpathi", VGName: testVG}}, bytes.Buffer{})

	commands.EXPECT().VGLockStart(gomock.Any(), testVG, 7).Return("vgchange --lock-start", nil)

	reconcile(t, r, ownedGroup())
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
		SanlockHostIDAnnotation:                       "7",
		LockspaceStartedAnnotationPrefix + group.Name: "true",
	})
	cl := fake.NewClientBuilder().WithScheme(s).
		WithObjects(node, group).WithStatusSubresource(group).Build()

	sdsCache := cache.New()
	sdsCache.StorePVs([]internal.PVData{{PVName: "/dev/mapper/mpathi", VGName: testVG}}, bytes.Buffer{})
	sdsCache.StoreVGs([]internal.VGData{{
		VGName:       testVG,
		VGUUID:       "vg-uuid",
		VGSize:       resource.MustParse("200Gi"),
		VGFree:       resource.MustParse("197Gi"),
		VGExtentSize: resource.MustParse("4Mi"),
	}}, bytes.Buffer{})
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
	r, _, _ := testReconciler(t, nodeWith(map[string]string{SanlockHostIDAnnotation: "7"}), commands, nil)
	// A group that is not the one the pool asks for. Found on the stand: the LUN
	// had been used for something else, and the check that only asked "is there a
	// group here" would have declared the pool's group ready without it existing.
	r.sdsCache.StorePVs([]internal.PVData{{PVName: "/dev/mapper/mpathi", VGName: "someone-elses-vg"}}, bytes.Buffer{})

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
