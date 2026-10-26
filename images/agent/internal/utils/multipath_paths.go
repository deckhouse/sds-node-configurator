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

// This file tells the paths of multipath maps apart without asking udev.
//
// The discoverer drops a path of a multipath map because lsblk reports it with
// fstype mpath_member. lsblk takes that from the udev database, where
// 62-multipath.rules puts it only if `multipath -u` claimed the device at its
// add event, and with find_multipaths yes (on) multipath claims a device only
// when its WWID is known already. A path that appears before its WWID is
// registered is never claimed and keeps an empty fstype for as long as it
// exists. That is the order a CSI driver works in: it scans the LUN in on every
// session, takes the WWID from the first path it finds, and only then runs
// `multipath -a`. Nothing else catches such a path either — lsblk lists a map
// under one of its paths only, so "has children" covers one path of many — and
// the agent published one of them as a consumable BlockDevice: a raw path of a
// LUN carrying a workload's data, which an LVMVolumeGroup would have wiped.
// Seen on a node with a Tatlin array over iSCSI: five of the seven paths of a
// mounted volume were unclaimed, and the BlockDevice moved from one to the next
// as change events claimed them one by one.
//
// sysfs knows regardless of udev: the paths of a map are its slaves.

package utils

import (
	"os"
	"path/filepath"
	"strings"
)

// MultipathPathDevices returns the device paths (/dev/sdX, /dev/nvmeXnY) of
// the slaves of every multipath map on the host, whether or not udev marked
// them mpath_member.
//
// A map or a slaves directory that cannot be read is skipped: its paths then
// stay subject to the fstype check alone, which is how the agent treated them
// before.
func MultipathPathDevices() map[string]struct{} {
	paths := map[string]struct{}{}

	entries, err := os.ReadDir(sysBlockPath)
	if err != nil {
		return paths
	}

	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "dm-") {
			continue
		}

		uuid, err := os.ReadFile(filepath.Join(sysBlockPath, name, "dm", "uuid"))
		if err != nil || !strings.HasPrefix(strings.TrimSpace(string(uuid)), multipathUUIDPrefix) {
			continue
		}

		slaves, err := os.ReadDir(filepath.Join(sysBlockPath, name, "slaves"))
		if err != nil {
			continue
		}
		for _, s := range slaves {
			paths["/dev/"+s.Name()] = struct{}{}
		}
	}

	return paths
}
