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

package utils_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/mock_utils"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

// withEmptyLoopRegistry isolates a test from the process-wide registry and puts it
// back afterwards. The registry is package-level state on purpose (see
// loopfilter.go), so tests have to say so rather than pretend otherwise.
func withEmptyLoopRegistry(t *testing.T) {
	t.Helper()

	saved := utils.OwnedLoops()
	for _, dev := range saved {
		utils.ForgetOwnedLoop(dev)
	}

	t.Cleanup(func() {
		for _, dev := range utils.OwnedLoops() {
			utils.ForgetOwnedLoop(dev)
		}
		for _, dev := range saved {
			utils.RememberOwnedLoop(dev)
		}
	})
}

// The regression lock. Dropping the loop reject rule from the filter is what took
// ten LVMVolumeGroups out of twenty offline: the agent read the LVM of every guest
// on the hypervisor, whose Volume Group names collide with each other and with the
// node's own. It was dropped in one line, to make room for spec.fileDevices, and
// nothing failed when it happened.
//
// The rule may only be relaxed for a device the agent owns, one device at a time,
// which is what LVMGlobalFilterAcceptingLoops is for.
func TestLVMFilterAlwaysRejectsUnownedLoopDevices(t *testing.T) {
	t.Run("the plain filter rejects every loop device", func(t *testing.T) {
		assert.Contains(t, internal.LVMGlobalFilter, `"r|^/dev/loop|"`,
			"a loop device on a hypervisor is a virtual machine's disk; the agent must not read the LVM inside it")
		assert.NotContains(t, internal.LVMGlobalFilter, `"a|`,
			"the plain form must carry no accept rule at all")
	})

	t.Run("the built filter still rejects the loops we do not own", func(t *testing.T) {
		got := internal.LVMGlobalFilterAcceptingLoops([]string{"/dev/loop7"})
		assert.Contains(t, got, `"r|^/dev/loop|"`)
	})

	t.Run("an owned loop is accepted exactly, and before the reject", func(t *testing.T) {
		got := internal.LVMGlobalFilterAcceptingLoops([]string{"/dev/loop7"})
		assert.Contains(t, got, `"a|^/dev/loop7$|"`,
			"anchored on both ends: /dev/loop70 is somebody else's device")
		assert.Less(t, indexOf(got, `"a|^/dev/loop7$|"`), indexOf(got, `"r|^/dev/loop|"`),
			"LVM applies the first matching rule, so the accept has to come first")
	})

	t.Run("the rules are ordered and complete", func(t *testing.T) {
		got := internal.LVMGlobalFilterAcceptingLoops([]string{"/dev/loop2", "/dev/loop10"})
		assert.Equal(t,
			`devices/global_filter=["a|^/dev/loop2$|","a|^/dev/loop10$|","r|^/dev/rbd|","r|^/dev/drbd|","r|^/dev/nbd|","r|^/dev/loop|"]`,
			got)
	})

	t.Run("no owned loop means the plain filter", func(t *testing.T) {
		assert.Equal(t, internal.LVMGlobalFilter, internal.LVMGlobalFilterAcceptingLoops(nil))
		assert.Equal(t, internal.LVMGlobalFilter, internal.LVMGlobalFilterAcceptingLoops([]string{}))
	})

	// A value spliced into an LVM configuration string whose own field separator is
	// `|`. A mangled filter does not fail one command, it fails every command on the
	// node — so anything that is not a canonical loop path is dropped instead.
	t.Run("a path that is not a canonical loop device is refused, not escaped", func(t *testing.T) {
		for _, bad := range []string{
			"/dev/loop7|,\"a|.*|\"",
			"/dev/sda",
			"/dev/loop",
			"loop7",
			"/dev/loop7 ",
			"../dev/loop7",
			"",
		} {
			got := internal.LVMGlobalFilterAcceptingLoops([]string{bad})
			assert.Equal(t, internal.LVMGlobalFilter, got, "must not appear in the filter: %q", bad)
		}
	})
}

func TestOwnedLoopRegistry(t *testing.T) {
	t.Run("remembering and forgetting", func(t *testing.T) {
		withEmptyLoopRegistry(t)

		utils.RememberOwnedLoop("/dev/loop3")
		utils.RememberOwnedLoop("/dev/loop1")
		utils.RememberOwnedLoop("/dev/loop3") // idempotent
		utils.RememberOwnedLoop("")           // ignored

		assert.Equal(t, []string{"/dev/loop1", "/dev/loop3"}, utils.OwnedLoops(),
			"sorted, so the command line in the log does not change between scans for no reason")

		utils.ForgetOwnedLoop("/dev/loop1")
		assert.Equal(t, []string{"/dev/loop3"}, utils.OwnedLoops())

		assert.Equal(t,
			internal.LVMGlobalFilterAcceptingLoops([]string{"/dev/loop3"}),
			utils.LVMGlobalFilterForOwnedLoops())
	})

	t.Run("an empty registry yields the plain filter", func(t *testing.T) {
		withEmptyLoopRegistry(t)
		assert.Equal(t, internal.LVMGlobalFilter, utils.LVMGlobalFilterForOwnedLoops())
	})
}

func TestRefreshOwnedLoops(t *testing.T) {
	log, err := logger.NewLogger(logger.WarningLevel)
	assert.NoError(t, err)

	const managed = "/opt/deckhouse/sds/file-devices/sds-lvg-a.d0.img"

	t.Run("only the loops backed by a file this agent named are kept", func(t *testing.T) {
		withEmptyLoopRegistry(t)
		ctrl := gomock.NewController(t)
		mc := mock_utils.NewMockCommands(ctrl)

		mc.EXPECT().ListLoopDevices(gomock.Any()).Return("losetup", []internal.LoopDeviceEntry{
			{Device: "/dev/loop0", Backing: internal.LoopBackingFile{Path: managed}},
			// A guest's disk: a block-mode volume kubelet attached for a virtual machine.
			{Device: "/dev/loop1", Backing: internal.LoopBackingFile{Path: "/var/lib/kubelet/plugins/kubernetes.io/csi/volumeDevices/pvc-1/dev/abc"}},
			// An image an operator attached by hand while restoring a node.
			{Device: "/dev/loop2", Backing: internal.LoopBackingFile{Path: "/backup/node2-root.img"}},
			// Attached but with no file behind it.
			{Device: "/dev/loop3", Backing: internal.LoopBackingFile{}},
		}, nil)

		assert.NoError(t, utils.RefreshOwnedLoops(context.Background(), log, mc, time.Minute))
		assert.Equal(t, []string{"/dev/loop0"}, utils.OwnedLoops())
	})

	// The backing file is gone but the loop is live and its PV is still in a Volume
	// Group. Hiding it now would drop that group from the cache, which reads as "the
	// node's own storage disappeared".
	t.Run("an unlinked backing file still identifies its owner", func(t *testing.T) {
		withEmptyLoopRegistry(t)
		ctrl := gomock.NewController(t)
		mc := mock_utils.NewMockCommands(ctrl)

		mc.EXPECT().ListLoopDevices(gomock.Any()).Return("losetup", []internal.LoopDeviceEntry{
			{Device: "/dev/loop0", Backing: internal.LoopBackingFile{Path: managed, Deleted: true}},
		}, nil)

		assert.NoError(t, utils.RefreshOwnedLoops(context.Background(), log, mc, time.Minute))
		assert.Equal(t, []string{"/dev/loop0"}, utils.OwnedLoops())
	})

	t.Run("a loop detached behind our back leaves the set", func(t *testing.T) {
		withEmptyLoopRegistry(t)
		utils.RememberOwnedLoop("/dev/loop0")
		utils.RememberOwnedLoop("/dev/loop5")

		ctrl := gomock.NewController(t)
		mc := mock_utils.NewMockCommands(ctrl)
		mc.EXPECT().ListLoopDevices(gomock.Any()).Return("losetup", []internal.LoopDeviceEntry{
			{Device: "/dev/loop0", Backing: internal.LoopBackingFile{Path: managed}},
		}, nil)

		assert.NoError(t, utils.RefreshOwnedLoops(context.Background(), log, mc, time.Minute))
		assert.Equal(t, []string{"/dev/loop0"}, utils.OwnedLoops(),
			"a minor the kernel has taken back may be handed to a virtual machine next")
	})

	// The conservative direction, and the one that matters: an empty set on a
	// transient losetup failure would hide the node's own file-backed Volume Groups
	// from the very scan about to decide whether they still exist.
	t.Run("a failure keeps the previously known set", func(t *testing.T) {
		withEmptyLoopRegistry(t)
		utils.RememberOwnedLoop("/dev/loop0")

		ctrl := gomock.NewController(t)
		mc := mock_utils.NewMockCommands(ctrl)
		mc.EXPECT().ListLoopDevices(gomock.Any()).Return("losetup", nil, errors.New("losetup: cannot open /dev/loop-control"))

		err := utils.RefreshOwnedLoops(context.Background(), log, mc, time.Minute)
		assert.Error(t, err)
		assert.Equal(t, []string{"/dev/loop0"}, utils.OwnedLoops())
	})
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}
