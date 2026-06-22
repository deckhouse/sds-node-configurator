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
