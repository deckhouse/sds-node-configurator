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
