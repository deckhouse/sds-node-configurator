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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildFileDevicePath_DeterministicAndDistinguishesEntries(t *testing.T) {
	dir := "/data"
	lvg := "vg-a"

	p1 := BuildFileDevicePath(dir, lvg, "data-1")
	p2 := BuildFileDevicePath(dir, lvg, "data-1")
	p3 := BuildFileDevicePath(dir, lvg, "data-2")
	p4 := BuildFileDevicePath("/other", lvg, "data-1")

	assert.Equal(t, p1, p2, "same inputs must produce the same path")
	assert.NotEqual(t, p1, p3, "a different entry name must produce a different basename")
	assert.NotEqual(t, p1, p4, "a different directory must produce a different path")

	assert.True(t, IsManagedFileDevicePath(p1, lvg))
	assert.True(t, IsManagedFileDevicePath(p3, lvg))
	assert.True(t, IsManagedFileDevicePath(p4, lvg))
}

// The size is deliberately absent from the path: an in-place grow
// (fallocate + losetup -c + pvresize) must not move the file out from under
// the loop device that is already attached to it.
func TestBuildFileDevicePath_IndependentOfSize(t *testing.T) {
	before := BuildFileDevicePath("/data", "vg-a", "data-1")
	assert.Equal(t, before, BuildFileDevicePath("/data", "vg-a", "data-1"),
		"the path must depend only on (directory, lvg, entry name)")
}

func TestParseFileDevicePath(t *testing.T) {
	tests := []struct {
		name      string
		path      string
		wantLVG   string
		wantEntry string
		wantOK    bool
	}{
		{"plain", "/data/sds-vg-a.data-1.img", "vg-a", "data-1", true},
		{"lvg name with dots", "/data/sds-my.vg.a.data-1.img", "my.vg.a", "data-1", true},
		{"lvg name with dashes", "/data/sds-long-name-with-dashes.d1.img", "long-name-with-dashes", "d1", true},
		{"empty", "", "", "", false},
		{"no prefix", "/data/random.img", "", "", false},
		{"no suffix", "/data/sds-vg-a.data-1", "", "", false},
		{"no separator", "/data/sds-vga.img", "", "", false},
		{"empty entry name", "/data/sds-vg-a..img", "", "", false},
		{"entry name with uppercase", "/data/sds-vg-a.Data1.img", "", "", false},
		{"entry name with underscore", "/data/sds-vg-a.data_1.img", "", "", false},
		{"libvirt qcow2", "/var/lib/libvirt/images/disk0.qcow2", "", "", false},
		{"snap image", "/var/lib/snapd/snaps/core22.snap", "", "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lvg, entry, ok := ParseFileDevicePath(tc.path)
			assert.Equal(t, tc.wantOK, ok)
			assert.Equal(t, tc.wantLVG, lvg)
			assert.Equal(t, tc.wantEntry, entry)
		})
	}
}

// Invertibility is what lets a file-backed Volume Group be re-imported after
// its LVMVolumeGroup resource is lost: the files on the node must be enough to
// say which LVG and which spec entry each one belongs to.
func TestBuildFileDevicePath_IsInvertible(t *testing.T) {
	for _, lvg := range []string{"vg-a", "data-vg", "long-name-with-dashes", "my.vg.with.dots"} {
		for _, entry := range []string{"d1", "data-1", "a", strings.Repeat("x", 63)} {
			p := BuildFileDevicePath("/data", lvg, entry)
			gotLVG, gotEntry, ok := ParseFileDevicePath(p)
			assert.True(t, ok, "%s must parse", p)
			assert.Equal(t, lvg, gotLVG, "%s", p)
			assert.Equal(t, entry, gotEntry, "%s", p)
		}
	}
}

func TestIsManagedFileDevicePath(t *testing.T) {
	tests := []struct {
		name string
		path string
		lvg  string
		want bool
	}{
		{"empty path", "", "vg-a", false},
		{"no prefix", "/data/random.img", "vg-a", false},
		{"no suffix", "/data/sds-vg-a.data-1", "vg-a", false},
		{"wrong lvg", "/data/sds-vg-b.data-1.img", "vg-a", false},
		{"matching lvg", "/data/sds-vg-a.data-1.img", "vg-a", true},
		{"any-lvg ok", "/data/sds-vg-a.data-1.img", "", true},
		{"any-lvg rejects bogus", "/etc/passwd", "", false},
		{"libvirt qcow2 looks nothing like ours", "/var/lib/libvirt/images/disk0.qcow2", "vg-a", false},
		{"snap loop image", "/var/lib/snapd/snaps/core22.snap", "vg-a", false},
		// A prefix match must not be enough: `vg-a` must not claim `vg-a-2`'s files.
		{"lvg name is a prefix of another", "/data/sds-vg-a-2.data-1.img", "vg-a", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, IsManagedFileDevicePath(tc.path, tc.lvg))
		})
	}
}

func TestIsManagedFileDevicePath_RoundTripWithBuildFileDevicePath(t *testing.T) {
	for _, dir := range []string{"/data", "/var/lib/sds", "/mnt/x"} {
		for _, entry := range []string{"d1", "data-1", "pool-0"} {
			for _, lvg := range []string{"vg-a", "data-vg", "long-name-with-dashes"} {
				p := BuildFileDevicePath(dir, lvg, entry)
				assert.True(t, IsManagedFileDevicePath(p, lvg), "%s should match lvg %s", p, lvg)
				assert.False(t, IsManagedFileDevicePath(p, "other-lvg"), "%s must NOT match foreign lvg", p)
			}
		}
	}
}

func TestFileDeviceBasenameTooLong(t *testing.T) {
	// `sds-` + lvg + `.` + entry + `.img` must fit into NAME_MAX (255). Both
	// operands are separately valid for the apiserver — 253 and 63 — so only
	// their sum can be judged here.
	assert.False(t, FileDeviceBasenameTooLong("vg-a", "data-1"))
	assert.False(t, FileDeviceBasenameTooLong(strings.Repeat("a", 245), "d"))
	assert.True(t, FileDeviceBasenameTooLong(strings.Repeat("a", 246), "d"))
	assert.True(t, FileDeviceBasenameTooLong(strings.Repeat("a", 253), strings.Repeat("x", 63)))
}
