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

package utils

import (
	"context"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
)

// The file-device commands are the one place where a silent flag typo has no
// safety net: --nooverlap and --direct-io=on are load-bearing (a second loop
// minor bound to the same backing file, and a double page cache), and nothing
// downstream would notice their absence until a cluster misbehaves.
//
// Note that the two are issued as separate commands on purpose, and this test
// pins that split: --direct-io is applied after the device is attached, so
// folding it into the attach makes a kernel that refuses direct I/O look like a
// failed attach — with the loop already bound. See SetLoopDirectIO.
//
// Each command is invoked with an already-cancelled context. os/exec checks
// the context before forking, so nothing is executed on the test machine —
// but the Cmd is fully built by then, and its String() is returned alongside
// the error, which is exactly the argv we want to assert.
func fileDeviceCancelledCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func TestFileDeviceCommandsArgv(t *testing.T) {
	c := commands{}
	ctx := fileDeviceCancelledCtx(t)

	const (
		path    = "/opt/deckhouse/sds/file-devices/sds-vg-a.d5.img"
		loopDev = "/dev/loop7"
		dir     = "/opt/deckhouse/sds/file-devices"
	)

	// Every host command goes through the same nsenter wrapper into PID 1's
	// namespaces; asserting it once here keeps the per-command checks focused
	// on the tool and its flags.
	const nsenterPrefix = internal.NSENTERCmd + " -t 1 -m -u -i -n -p --"

	tests := []struct {
		name     string
		run      func() string
		contains []string
		omits    []string
	}{
		{
			name: "CreateFileDevice preallocates the exact byte count",
			run: func() string {
				cmd, err := c.CreateFileDevice(ctx, path, 10737418240)
				assert.ErrorIs(t, err, context.Canceled)
				return cmd
			},
			contains: []string{nsenterPrefix, "/usr/bin/fallocate -l 10737418240 " + path},
		},
		{
			name: "SetupLoopDevice reuses an existing loop",
			run: func() string {
				cmd, _, err := c.SetupLoopDevice(ctx, path)
				assert.ErrorIs(t, err, context.Canceled)
				return cmd
			},
			// --nooverlap: never bind a second minor to the same backing file.
			// --show: the loop path is read back from stdout.
			contains: []string{
				nsenterPrefix, "/sbin/losetup", "--find", "--nooverlap", "--show", path,
			},
			// Attaching must not depend on direct I/O being available.
			omits: []string{"--direct-io"},
		},
		{
			name: "SetLoopDirectIO is a separate, best-effort step",
			run: func() string {
				cmd, err := c.SetLoopDirectIO(ctx, loopDev)
				assert.ErrorIs(t, err, context.Canceled)
				return cmd
			},
			contains: []string{nsenterPrefix, "/sbin/losetup --direct-io=on " + loopDev},
		},
		{
			// Allocated blocks, not the apparent size: the caller uses this to work
			// out how much fallocate still has to reserve, and a sparse file would
			// report a full apparent size while occupying nothing.
			name: "GetFileAllocatedBytes reads the allocated size",
			run: func() string {
				cmd, _, err := c.GetFileAllocatedBytes(ctx, path)
				assert.ErrorIs(t, err, context.Canceled)
				return cmd
			},
			contains: []string{nsenterPrefix, "/usr/bin/stat -c %b %B " + path},
		},
		{
			name: "DetachLoopDevice detaches exactly one minor",
			run: func() string {
				cmd, err := c.DetachLoopDevice(ctx, loopDev)
				assert.ErrorIs(t, err, context.Canceled)
				return cmd
			},
			contains: []string{nsenterPrefix, "/sbin/losetup -d " + loopDev},
		},
		{
			name: "FindLoopDeviceByFile queries by backing file",
			run: func() string {
				cmd, _, err := c.FindLoopDeviceByFile(ctx, path)
				assert.ErrorIs(t, err, context.Canceled)
				return cmd
			},
			contains: []string{nsenterPrefix, "/sbin/losetup -j " + path},
		},
		{
			name: "GetLoopBackingFile reads a single bare column",
			run: func() string {
				cmd, _, err := c.GetLoopBackingFile(ctx, loopDev)
				assert.ErrorIs(t, err, context.Canceled)
				return cmd
			},
			// --noheadings keeps the column header out of the parsed value.
			contains: []string{nsenterPrefix, "/sbin/losetup --noheadings --output BACK-FILE " + loopDev},
		},
		{
			name: "RemoveFileDevice tolerates an already-missing file",
			run: func() string {
				cmd, err := c.RemoveFileDevice(ctx, path)
				assert.ErrorIs(t, err, context.Canceled)
				return cmd
			},
			contains: []string{nsenterPrefix, "/bin/rm -f " + path},
		},
		{
			name: "EnsureFileDeviceDirectory creates parents idempotently",
			run: func() string {
				cmd, err := c.EnsureFileDeviceDirectory(ctx, dir)
				assert.ErrorIs(t, err, context.Canceled)
				return cmd
			},
			contains: []string{nsenterPrefix, "/bin/mkdir -p " + dir},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.run()
			for _, want := range tt.contains {
				assert.Contains(t, got, want)
			}
			for _, unwanted := range tt.omits {
				assert.NotContains(t, got, unwanted)
			}
		})
	}
}

// A backing file unlinked while its loop is still attached is reported by
// losetup with a literal " (deleted)" suffix. Leaving it on the path makes the
// ownership check miss the basename, so cleanup refuses to detach and the
// minor is stranded on the node for good — and dropping it without reporting it
// makes provisioning create a second backing file at the same path, doubling the
// Volume Group.
func TestParseBackingFileSplitsDeletedMarker(t *testing.T) {
	type want struct {
		path    string
		deleted bool
	}
	tests := map[string]struct {
		in   string
		want want
	}{
		"plain":                {"/data/sds-vg-a.d5.img", want{"/data/sds-vg-a.d5.img", false}},
		"trailing_newline":     {"/data/sds-vg-a.d5.img\n", want{"/data/sds-vg-a.d5.img", false}},
		"deleted_marker":       {"/data/sds-vg-a.d5.img (deleted)", want{"/data/sds-vg-a.d5.img", true}},
		"deleted_and_newline":  {"/data/sds-vg-a.d5.img (deleted)\n", want{"/data/sds-vg-a.d5.img", true}},
		"leading_whitespace":   {"  /data/sds-vg-a.d5.img", want{"/data/sds-vg-a.d5.img", false}},
		"detached_empty_value": {"", want{"", false}},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got := parseBackingFile(tt.in)
			assert.Equal(t, tt.want.path, got.Path)
			assert.Equal(t, tt.want.deleted, got.Deleted)
		})
	}
}

// One backing file bound to two loop devices is not a curiosity: it is two
// Physical Volumes of the same size over the same blocks, which is how a
// file-backed Volume Group silently doubled on a real cluster. The parser has to
// report both, so FindLoopDeviceByFile can refuse rather than pick one — a
// caller handed only the first would call the file "already attached" and carry
// on, or detach one loop and `rm` the file the other is still reading from.
func TestParseLoopDeviceListing(t *testing.T) {
	tests := map[string]struct {
		in   string
		want []string
	}{
		"detached":         {"", []string{}},
		"whitespace_only":  {"  \n\n", []string{}},
		"single":           {"/dev/loop0: 0 /data/sds-vg-a.d1.img\n", []string{"/dev/loop0"}},
		"single_no_offset": {"/dev/loop3: []: (/data/sds-vg-a.d1.img)", []string{"/dev/loop3"}},
		"double_attach": {
			"/dev/loop0: 0 /data/sds-vg-a.d1.img\n/dev/loop7: 0 /data/sds-vg-a.d1.img\n",
			[]string{"/dev/loop0", "/dev/loop7"},
		},
		// A blank line must not become an empty device name: counted as a device it
		// would make a single attachment look like a double one and stop
		// provisioning outright.
		"blank_line_between": {
			"/dev/loop0: 0 /data/sds-vg-a.d1.img\n\n/dev/loop7: 0 /data/sds-vg-a.d1.img",
			[]string{"/dev/loop0", "/dev/loop7"},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tt.want, parseLoopDeviceListing(tt.in))
		})
	}
}

// GetFileAllocatedBytes reports "the file is not there" only when stat actually
// ran and exited non-zero. Everything else — a binary that could not be started,
// a process killed by the per-command deadline or by SIGTERM during shutdown —
// establishes nothing about the path, and reading it as "not there" is what let a
// transient timeout end with the create rollback removing a backing file that
// carried a live Physical Volume.
func TestRanAndFailed(t *testing.T) {
	t.Run("a command that ran and chose a non-zero exit code", func(t *testing.T) {
		err := exec.Command("/bin/sh", "-c", "exit 1").Run()
		require.Error(t, err)
		assert.True(t, ranAndFailed(err))
	})

	t.Run("a command killed by a signal did not choose anything", func(t *testing.T) {
		// What exec.CommandContext does to the child when the deadline expires.
		err := exec.Command("/bin/sh", "-c", "kill -9 $$").Run()
		require.Error(t, err)
		assert.False(t, ranAndFailed(err))
	})

	t.Run("a command that never started", func(t *testing.T) {
		err := exec.Command("/nonexistent/definitely-not-a-binary").Run()
		require.Error(t, err)
		assert.False(t, ranAndFailed(err))
	})

	t.Run("a context that was already done", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := exec.CommandContext(ctx, "/bin/sh", "-c", "exit 1").Run()
		require.Error(t, err)
		assert.False(t, ranAndFailed(err))
	})

	t.Run("success is not a failure", func(t *testing.T) {
		assert.False(t, ranAndFailed(nil))
	})
}
