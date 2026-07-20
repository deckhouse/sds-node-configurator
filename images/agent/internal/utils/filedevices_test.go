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
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestBuildFileDevicePath_DeterministicAndDistinguishesEntries(t *testing.T) {
	dir := "/data"
	lvg := "vg-a"
	size10 := resource.MustParse("10Gi")
	size20 := resource.MustParse("20Gi")

	p1 := BuildFileDevicePath(dir, lvg, size10)
	p2 := BuildFileDevicePath(dir, lvg, size10)
	p3 := BuildFileDevicePath(dir, lvg, size20)
	p4 := BuildFileDevicePath("/other", lvg, size10)

	assert.Equal(t, p1, p2, "same inputs must produce the same path")
	assert.NotEqual(t, p1, p3, "different size must produce a different basename")
	assert.NotEqual(t, p1, p4, "different directory must produce a different path")

	assert.True(t, IsManagedFileDevicePath(p1, lvg))
	assert.True(t, IsManagedFileDevicePath(p3, lvg))
	assert.True(t, IsManagedFileDevicePath(p4, lvg))
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
		{"no suffix", "/data/sds-vg-a-deadbeef0011", "vg-a", false},
		{"wrong lvg", "/data/sds-vg-b-deadbeef0011.img", "vg-a", false},
		{"matching lvg", "/data/sds-vg-a-deadbeef0011.img", "vg-a", true},
		{"hash too short", "/data/sds-vg-a-dead.img", "vg-a", false},
		{"non-hex hash", "/data/sds-vg-a-zzzzzzzzzzzz.img", "vg-a", false},
		{"any-lvg ok", "/data/sds-vg-a-deadbeef0011.img", "", true},
		{"any-lvg rejects bogus", "/etc/passwd", "", false},
		{"libvirt qcow2 looks nothing like ours", "/var/lib/libvirt/images/disk0.qcow2", "vg-a", false},
		{"snap loop image", "/var/lib/snapd/snaps/core22.snap", "vg-a", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, IsManagedFileDevicePath(tc.path, tc.lvg))
		})
	}
}

func TestIsManagedFileDevicePath_RoundTripWithBuildFileDevicePath(t *testing.T) {
	// Walk a representative grid of inputs to make sure Build/IsManaged
	// stay in sync no matter what (directory, size) pair the user gives.
	for _, dir := range []string{"/data", "/var/lib/sds", "/mnt/x"} {
		for _, sz := range []string{"1Gi", "10Gi", "999Gi", "2Ti"} {
			for _, lvg := range []string{"vg-a", "data-vg", "long-name-with-dashes"} {
				q := resource.MustParse(sz)
				p := BuildFileDevicePath(dir, lvg, q)
				assert.True(t, IsManagedFileDevicePath(p, lvg), "%s should match lvg %s", p, lvg)
				assert.False(t, IsManagedFileDevicePath(p, "other-lvg"), "%s must NOT match foreign lvg", p)
			}
		}
	}
}
