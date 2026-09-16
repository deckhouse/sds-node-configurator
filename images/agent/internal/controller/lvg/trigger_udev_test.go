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

package lvg

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/mock_utils"
)

// TestTriggerUdevForPaths pins the behavioural contract of the best-effort
// udev-DB refresh that the reconciler issues after pvcreate/vgcreate.
//
// The fix in this PR exists because lvm.static is built without udev
// integration; the trigger is the only mechanism that keeps the host udev
// database in sync with newly created PVs. If any of the invariants below
// regresses, freshly created LVMVolumeGroups will silently stop reaching
// the Ready phase, which is exactly the symptom this PR was opened to
// fix — so the tests intentionally guard each invariant explicitly.
func TestTriggerUdevForPaths(t *testing.T) {
	t.Run("forwards_paths_verbatim_on_happy_path", func(t *testing.T) {
		// On a normal create/extend the reconciler must hand the exact
		// list of newly added PV paths to UdevadmTrigger so the
		// downstream udevd refreshes only those devices. Reordering or
		// deduplication would change which device gets re-probed and
		// is therefore explicitly disallowed.
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockCmds := mock_utils.NewMockCommands(ctrl)
		r := setupReconciler()
		r.commands = mockCmds

		paths := []string{"/dev/sdc", "/dev/sda", "/dev/sdb"}
		mockCmds.EXPECT().
			UdevadmTrigger(gomock.Any(), gomock.Eq(paths)).
			Return("nsenter ... udevadm trigger --action=change -- /dev/sdc /dev/sda /dev/sdb", nil).
			Times(1)

		r.triggerUdevForPaths(context.Background(), paths)
	})

	t.Run("is_noop_on_nil_paths", func(t *testing.T) {
		// Without this guard, `udevadm trigger --action=change` (with
		// no positional args) would enqueue a change uevent for EVERY
		// block device on the host. The reconciler must never reach
		// the command layer in that case.
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockCmds := mock_utils.NewMockCommands(ctrl)
		r := setupReconciler()
		r.commands = mockCmds

		// No EXPECT(): any call to UdevadmTrigger fails the test.
		r.triggerUdevForPaths(context.Background(), nil)
	})

	t.Run("is_noop_on_empty_paths_slice", func(t *testing.T) {
		// Same guarantee as the nil case — empty slices and nil
		// slices must be treated identically.
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockCmds := mock_utils.NewMockCommands(ctrl)
		r := setupReconciler()
		r.commands = mockCmds

		r.triggerUdevForPaths(context.Background(), []string{})
	})

	t.Run("swallows_udevadm_error_as_non_fatal", func(t *testing.T) {
		// The refresh is best-effort: the BD discoverer will pick up
		// the change on the next periodic scan even if `udevadm
		// trigger` fails (e.g. udevd not running on the node). A
		// failure here must therefore NOT propagate to the caller —
		// otherwise createVGComplex/extendVGComplex would surface
		// "VGCreationFailed" on the LVG and force a manual recovery
		// for what is, in practice, only a temporary observability
		// gap.
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockCmds := mock_utils.NewMockCommands(ctrl)
		r := setupReconciler()
		r.commands = mockCmds

		mockCmds.EXPECT().
			UdevadmTrigger(gomock.Any(), gomock.Any()).
			Return("nsenter ... udevadm trigger ...", errors.New("udevd not running")).
			Times(1)

		assert.NotPanics(t, func() {
			r.triggerUdevForPaths(context.Background(), []string{"/dev/sda"})
		})
	})

	t.Run("passes_a_bounded_child_context_derived_from_parent", func(t *testing.T) {
		// Two invariants are checked at once:
		//  1. the ctx forwarded to UdevadmTrigger has a Deadline set
		//     (i.e. it isn't context.Background()) — without it a
		//     hung nsenter would block reconciliation indefinitely;
		//  2. the deadline is in the future and within
		//     udevadmTriggerTimeout of "now" — pins the magic-value
		//     extraction so any future bump of the constant requires
		//     touching this test.
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockCmds := mock_utils.NewMockCommands(ctrl)
		r := setupReconciler()
		r.commands = mockCmds

		var captured context.Context
		mockCmds.EXPECT().
			UdevadmTrigger(gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, _ []string) (string, error) {
				captured = ctx
				return "", nil
			}).
			Times(1)

		r.triggerUdevForPaths(context.Background(), []string{"/dev/sda"})

		if !assert.NotNil(t, captured, "UdevadmTrigger was not invoked") {
			return
		}
		dl, ok := captured.Deadline()
		if !assert.True(t, ok, "ctx must carry a deadline; udevadmTriggerTimeout must be enforced") {
			return
		}
		assert.True(t, dl.After(time.Now()), "deadline must be in the future")
		assert.LessOrEqual(t, time.Until(dl), udevadmTriggerTimeout,
			"deadline must be within udevadmTriggerTimeout of now")
	})

	t.Run("honours_parent_context_cancellation", func(t *testing.T) {
		// A SIGTERM from kubelet cancels the reconcile loop's ctx.
		// Because triggerUdevForPaths derives its ctx from the parent
		// (via context.WithTimeout(parent, ...)), the cancellation
		// must reach UdevadmTrigger as a Done() signal — proving that
		// the original PR's `context.Background()` was replaced with
		// the parent ctx.
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockCmds := mock_utils.NewMockCommands(ctrl)
		r := setupReconciler()
		r.commands = mockCmds

		parent, cancel := context.WithCancel(context.Background())
		cancel()

		mockCmds.EXPECT().
			UdevadmTrigger(gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, _ []string) (string, error) {
				select {
				case <-ctx.Done():
					return "", ctx.Err()
				default:
					return "", errors.New("ctx not propagated: parent cancellation did not reach UdevadmTrigger")
				}
			}).
			Times(1)

		r.triggerUdevForPaths(parent, []string{"/dev/sda"})
	})
}

// pvPathsOfVG feeds the re-probe the reconciler issues for a resource that has
// never been described — an LVMVolumeGroup created by hand over an existing Volume
// Group. Keyed by VG UUID rather than name, because two Volume Groups answering to
// one name is precisely the situation the surrounding code exists for.
func TestPVPathsOfVG(t *testing.T) {
	vg := internal.VGData{VGName: "vg-adopt", VGUUID: "uuid-adopt"}
	pvs := []internal.PVData{
		{PVName: "/dev/sdc", VGName: "vg-adopt", VGUuid: "uuid-adopt"},
		{PVName: "/dev/sdd", VGName: "vg-adopt", VGUuid: "uuid-adopt"},
		{PVName: "/dev/sde", VGName: "vg-adopt", VGUuid: "uuid-another-group-with-the-same-name"},
		{PVName: "", VGName: "vg-adopt", VGUuid: "uuid-adopt"},
		{PVName: "/dev/sdf", VGName: "vg-other", VGUuid: "uuid-other"},
	}

	assert.Equal(t, []string{"/dev/sdc", "/dev/sdd"}, pvPathsOfVG(pvs, vg),
		"only the PVs of this very Volume Group, by UUID, and never an empty path")
	assert.Empty(t, pvPathsOfVG(nil, vg), "no PVs, nothing to trigger — triggerUdevForPaths then stays away from udevadm")
}

// TestTriggerUdevForAdoption pins the adoption re-probe: an LVMVolumeGroup that
// has never been described (no status.vgUuid) over a Volume Group that already
// exists gets the group's own Physical Volumes re-probed — once, however many
// reconciles pass before the discoverer describes it — and a described resource
// is never re-probed at all.
func TestTriggerUdevForAdoption(t *testing.T) {
	vg := internal.VGData{VGName: "vg-adopt", VGUUID: "vg-uuid-adopt"}
	pvs := []internal.PVData{
		{PVName: "/dev/sdb1", VGUuid: "vg-uuid-adopt"},
		{PVName: "/dev/sdz", VGUuid: "vg-uuid-other"},
	}
	undescribed := func() *v1alpha1.LVMVolumeGroup {
		return &v1alpha1.LVMVolumeGroup{
			ObjectMeta: metav1.ObjectMeta{Name: "lvg-adopt"},
			Spec:       v1alpha1.LVMVolumeGroupSpec{ActualVGNameOnTheNode: "vg-adopt"},
		}
	}
	described := func() *v1alpha1.LVMVolumeGroup {
		lvg := undescribed()
		lvg.Status.VGUuid = "vg-uuid-adopt"
		return lvg
	}

	t.Run("re_probes_the_groups_own_pvs_once", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockCmds := mock_utils.NewMockCommands(ctrl)
		r := setupReconciler()
		r.commands = mockCmds

		// Only the PVs of this Volume Group, and only once across three reconciles.
		mockCmds.EXPECT().
			UdevadmTrigger(gomock.Any(), gomock.Eq([]string{"/dev/sdb1"})).
			Return("nsenter ... udevadm trigger --action=change -- /dev/sdb1", nil).
			Times(1)

		assert.True(t, r.triggerUdevForAdoption(context.Background(), undescribed(), vg, pvs))
		assert.False(t, r.triggerUdevForAdoption(context.Background(), undescribed(), vg, pvs),
			"a second reconcile of the same undescribed resource does not re-probe")
		assert.False(t, r.triggerUdevForAdoption(context.Background(), undescribed(), vg, pvs))
	})

	t.Run("does_nothing_for_a_described_resource", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockCmds := mock_utils.NewMockCommands(ctrl)
		r := setupReconciler()
		r.commands = mockCmds

		// No EXPECT(): any call to UdevadmTrigger fails the test.
		assert.False(t, r.triggerUdevForAdoption(context.Background(), described(), vg, pvs))
	})

	t.Run("forgets_the_resource_once_it_is_described", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockCmds := mock_utils.NewMockCommands(ctrl)
		r := setupReconciler()
		r.commands = mockCmds

		// Adopted, described, then re-created under the same name: two re-probes.
		mockCmds.EXPECT().
			UdevadmTrigger(gomock.Any(), gomock.Eq([]string{"/dev/sdb1"})).
			Return("", nil).
			Times(2)

		assert.True(t, r.triggerUdevForAdoption(context.Background(), undescribed(), vg, pvs))
		assert.False(t, r.triggerUdevForAdoption(context.Background(), described(), vg, pvs),
			"the described resource is not re-probed, and the memo is dropped")
		assert.True(t, r.triggerUdevForAdoption(context.Background(), undescribed(), vg, pvs),
			"a resource re-created under the same name is re-probed again")
	})

	t.Run("is_independent_per_resource", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockCmds := mock_utils.NewMockCommands(ctrl)
		r := setupReconciler()
		r.commands = mockCmds

		mockCmds.EXPECT().UdevadmTrigger(gomock.Any(), gomock.Any()).Return("", nil).Times(2)

		other := undescribed()
		other.Name = "lvg-adopt-2"
		assert.True(t, r.triggerUdevForAdoption(context.Background(), undescribed(), vg, pvs))
		assert.True(t, r.triggerUdevForAdoption(context.Background(), other, vg, pvs),
			"the memo is per resource name, not per Volume Group")
	})
}
