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

package bd

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
)

// Taken from a client node after a drain: the LUNs behind mpathd and mpathg had
// been unmapped on the array when their volumes moved, the one path each map
// still listed was failed, and lsblk kept reporting both as 2G mpath devices —
// the agent had a consumable BlockDevice on /dev/mapper/mpathd for hours.
func multipathDevicesAfterDrain() []internal.Device {
	two := resource.MustParse("2Gi")
	one := resource.MustParse("1Gi")

	return []internal.Device{
		{Name: "/dev/sdh", KName: "/dev/sdh", Type: "disk", FSType: "mpath_member", Size: resource.MustParse("0")},
		{Name: "/dev/mapper/mpathd", KName: "/dev/dm-70", PkName: "/dev/sdh", Type: "mpath", Size: two, Wwn: "0x61452900326008fc40024000000004dd"},
		{Name: "/dev/sda", KName: "/dev/sda", Type: "disk", FSType: "mpath_member", Size: one},
		{Name: "/dev/mapper/mpathf", KName: "/dev/dm-72", PkName: "/dev/sda", Type: "mpath", Size: one, Wwn: "0x61452900326008fc40024000000004df"},
		{Name: "/dev/mapper/mpathc", KName: "/dev/dm-71", Type: "mpath", Size: one, Wwn: "0x61452900326008fc40024000000004d9"},
	}
}

func TestFilterDevices_DropsMultipathMapsWithoutAnActivePath(t *testing.T) {
	d := setupDiscoverer()
	d.unreadableMultipathDevices = func() []string {
		return []string{"/dev/dm-70", "/dev/dm-71", "/dev/mapper/mpathc", "/dev/mapper/mpathd"}
	}

	filtered, err := d.filterDevices(multipathDevicesAfterDrain())
	require.NoError(t, err)

	names := make([]string, 0, len(filtered))
	for i := range filtered {
		names = append(names, filtered[i].Name)
	}
	assert.Contains(t, names, "/dev/mapper/mpathf",
		"a map with an active path is a SAN LUN the operator may build on and stays a candidate")
	assert.NotContains(t, names, "/dev/mapper/mpathd",
		"a map whose only path is failed has nothing behind it and must not become a BlockDevice")
	assert.NotContains(t, names, "/dev/mapper/mpathc",
		"a map with no paths at all neither")
}

func TestFilterDevices_KeepsMultipathMapsWhenNoneIsUnreadable(t *testing.T) {
	d := setupDiscoverer()
	d.unreadableMultipathDevices = func() []string { return nil }

	filtered, err := d.filterDevices(multipathDevicesAfterDrain())
	require.NoError(t, err)

	names := make([]string, 0, len(filtered))
	for i := range filtered {
		names = append(names, filtered[i].Name)
	}
	assert.Contains(t, names, "/dev/mapper/mpathd")
	assert.Contains(t, names, "/dev/mapper/mpathf")
}
