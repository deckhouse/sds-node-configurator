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

// Taken from a node with a Tatlin array over iSCSI right after a volume was
// staged, before its publish formatted it: the CSI driver scanned the LUN in
// on every session before it registered the WWID, so udev claimed only the
// paths that appeared after, and lsblk listed the map under one path. sdk,
// unclaimed and childless, was published as a consumable BlockDevice, and
// stayed so after the map on top of it got an XFS and a mount.
func multipathDevicesWithUnclaimedPaths() []internal.Device {
	one := resource.MustParse("1Gi")
	wwn := "0x61452900326008fc40024000000004f9"

	return []internal.Device{
		{Name: "/dev/sdh", KName: "/dev/sdh", Type: "disk", Size: one, Wwn: wwn},
		{Name: "/dev/sdi", KName: "/dev/sdi", Type: "disk", FSType: "mpath_member", Size: one, Wwn: wwn},
		{Name: "/dev/sdk", KName: "/dev/sdk", Type: "disk", Size: one, Wwn: wwn},
		{Name: "/dev/mapper/mpathba", KName: "/dev/dm-1", PkName: "/dev/sdh", Type: "mpath", Size: one, Wwn: wwn},
		{Name: "/dev/sdz", KName: "/dev/sdz", Type: "disk", Size: one, Wwn: "0x5000c500a1b2c3d4"},
	}
}

func filteredNames(t *testing.T, d *Discoverer, devices []internal.Device) []string {
	t.Helper()

	filtered, err := d.filterDevices(devices)
	require.NoError(t, err)

	names := make([]string, 0, len(filtered))
	for i := range filtered {
		names = append(names, filtered[i].Name)
	}
	return names
}

func TestFilterDevices_DropsPathsOfMultipathMapsUdevDidNotClaim(t *testing.T) {
	d := setupDiscoverer()
	d.unreadableMultipathDevices = func() []string { return nil }
	d.multipathPaths = func() map[string]struct{} {
		return map[string]struct{}{"/dev/sdh": {}, "/dev/sdi": {}, "/dev/sdk": {}}
	}

	names := filteredNames(t, d, multipathDevicesWithUnclaimedPaths())

	assert.NotContains(t, names, "/dev/sdk",
		"a path of a map is a raw way into the LUN the map serves, claimed by udev or not")
	assert.NotContains(t, names, "/dev/sdh")
	assert.NotContains(t, names, "/dev/sdi")
	assert.Contains(t, names, "/dev/mapper/mpathba", "the map itself is still listed")
	assert.Contains(t, names, "/dev/sdz", "a disk that is no map's path stays a candidate")
}

func TestFilterDevices_UnclaimedPathPassesWithoutTheSysfsSet(t *testing.T) {
	d := setupDiscoverer()
	d.unreadableMultipathDevices = func() []string { return nil }
	d.multipathPaths = func() map[string]struct{} { return nil }

	names := filteredNames(t, d, multipathDevicesWithUnclaimedPaths())

	assert.Contains(t, names, "/dev/sdk", "what the agent published before it read the maps' slaves")
}
