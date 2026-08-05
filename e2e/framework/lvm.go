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

// Tier A — pending upstream: storage-e2e

package framework

import (
	"fmt"
	"strings"
)

// CountPVsInVG counts non-empty lines of `pvs -o pv_name --noheadings -S vg_name --select vg_name=<vg>` output.
func CountPVsInVG(out string) int {
	count := 0
	for _, line := range strings.Split(out, "\n") {
		if strings.TrimSpace(line) != "" {
			count++
		}
	}
	return count
}

// VGInListing reports whether vgName appears as a line in `vgs -o vg_name --noheadings` output.
func VGInListing(out, vgName string) bool {
	for _, line := range strings.Split(out, "\n") {
		if strings.TrimSpace(line) == vgName {
			return true
		}
	}
	return false
}

// ThinPoolDataLVPresent parses `lvs -a -o lv_name,lv_attr --noheadings <vg>` output and reports whether a
// thin-pool data LV exists for thinPoolName (lv_attr starts with 't' and lv_name == pool or pool+"_tdata").
func ThinPoolDataLVPresent(out, thinPoolName string) bool {
	for _, line := range strings.Split(out, "\n") {
		line = strings.ReplaceAll(line, "[", "")
		line = strings.ReplaceAll(line, "]", "")
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		lvName, lvAttr := fields[0], fields[1]
		if strings.HasPrefix(lvAttr, "t") && (lvName == thinPoolName || lvName == thinPoolName+"_tdata") {
			return true
		}
	}
	return false
}

// RemoveThinPoolStackScript returns a shell script to brute-force teardown a thin-pool stack for a VG.
func RemoveThinPoolStackScript(vgName, thinPoolName string) string {
	return fmt.Sprintf(`set +e
VG=%q
POOL=%q
runlv() { lvs "$@" 2>/dev/null || sudo -n lvs "$@" 2>/dev/null; }
runrm() { lvremove -fy "$@" 2>/dev/null || sudo -n lvremove -fy "$@" 2>/dev/null; }
for pass in 1 2 3 4 5 6 7 8 9 10; do
  runlv -a --noheadings -o lv_name,pool_lv "$VG" | while IFS= read -r line; do
    lv=$(echo "$line" | awk '{print $1}' | tr -d '[]')
    pl=$(echo "$line" | awk '{print $2}' | tr -d '[]')
    [ -z "$lv" ] && continue
    [ -n "$pl" ] && [ "$pl" = "$POOL" ] && [ "$lv" != "$POOL" ] && runrm "/dev/$VG/$lv"
  done
done
runrm "/dev/$VG/$POOL"
for pass in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do
  cnt=$(runlv -a --noheadings -o lv_name "$VG" | sed '/^$/d' | wc -l)
  cnt=$(echo "$cnt" | tr -cd '0-9')
  [ "${cnt:-0}" -eq 0 ] && break
  runlv -a --noheadings -o lv_name "$VG" | while IFS= read -r line; do
    lv=$(echo "$line" | awk '{print $1}' | tr -d '[]')
    [ -n "$lv" ] && runrm "/dev/$VG/$lv"
  done
done
`, vgName, thinPoolName)
}
