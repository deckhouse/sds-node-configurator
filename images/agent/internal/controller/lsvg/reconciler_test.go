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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	corev1 "k8s.io/api/core/v1"
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
