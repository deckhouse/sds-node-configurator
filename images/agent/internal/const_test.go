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

package internal

import (
	"strings"
	"testing"
)

// TestLVMGlobalFilter_NoBlanketAccept verifies that LVMGlobalFilter does not
// contain a blanket "a|.*|" accept rule. Adding such a rule overrides LVM's
// built-in device filter and forces lvm.static to scan non-standard paths
// (e.g. /dev/disk/by-diskseq/*), which may surface duplicate VG names when the
// same PV is visible through multiple device aliases. Duplicate VG names break
// lvremove and other commands that address LVs by VG name.
func TestLVMGlobalFilter_NoBlanketAccept(t *testing.T) {
	if strings.Contains(LVMGlobalFilter, `a|.*|`) {
		t.Fatalf(
			"LVMGlobalFilter must not contain a blanket accept rule \"a|.*|\". "+
				"It overrides LVM's built-in device filter and causes duplicate VG names "+
				"when PVs are visible through non-standard paths. "+
				"Remove it and let LVM use its default accept-on-no-match behaviour. "+
				"Current filter: %s",
			LVMGlobalFilter,
		)
	}
}

// TestConditionReasonsCrossingTheModuleBoundary pins the wire value of the
// reasons the controller matches by string.
//
// The agent writes these into an LVMVolumeGroup condition; the controller reads
// them back and decides, in images/controller/pkg/controller, whether the reason
// keeps the Volume Group in service. The two live in different Go modules with
// separate copies of the constant, so nothing links them at compile time and the
// existing tests on each side pass whichever value their own copy holds.
//
// What a drift costs is not a broken test but a silent outage: rename the value
// here and BlockDeviceNotFound stops matching the controller's acceptableReasons,
// so every LVMVolumeGroup carrying a filtered-out or undersized Physical Volume
// leaves Ready, its phase becomes NotReady, and the scheduler stops placing
// volumes on Volume Groups that are serving perfectly well. NodeNotDescribed is
// pinned for the opposite reason: it must keep NOT matching.
//
// The mirror of this test lives beside the controller's copy. Both have to be
// updated together, which is the point.
func TestConditionReasonsCrossingTheModuleBoundary(t *testing.T) {
	for name, pair := range map[string]struct{ got, want string }{
		"ReasonBlockDeviceNotFound": {ReasonBlockDeviceNotFound, "BlockDeviceNotFound"},
		"ReasonNodeNotDescribed":    {ReasonNodeNotDescribed, "NodeNotDescribed"},
		"ReasonVGCheckFailed":       {ReasonVGCheckFailed, "VGCheckFailed"},
		"ReasonCacheStale":          {ReasonCacheStale, "CacheStale"},
	} {
		if pair.got != pair.want {
			t.Errorf("%s is %q, but the controller matches it as %q; change both or neither",
				name, pair.got, pair.want)
		}
	}
}
