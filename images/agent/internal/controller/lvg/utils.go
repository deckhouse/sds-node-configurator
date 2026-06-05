/*
Copyright 2025 Flant JSC

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

package lvg

import (
	"fmt"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
)

func isApplied(lvg *v1alpha1.LVMVolumeGroup) bool {
	for _, c := range lvg.Status.Conditions {
		if c.Type == internal.TypeVGConfigurationApplied && c.Status == v1.ConditionTrue {
			return true
		}
	}

	return false
}

func isThinPool(lv internal.LVData) bool {
	return string(lv.LVAttr[0]) == "t"
}

func getVGAllocatedSize(vg internal.VGData) resource.Quantity {
	allocatedSize := vg.VGSize
	allocatedSize.Sub(vg.VGFree)
	return allocatedSize
}

// findDuplicateVGNames returns VG names that appear on the node more than
// once, mapped to the sorted list of their UUIDs.
//
// LVM cannot resolve VGs by name when duplicates exist (`vgs <name>` and
// `lvs <name>` fail with `Multiple VGs found with the same name: skipping`),
// and the agent's internal lookups (`cache.FindVG`, `cache.FindLV`, the
// candidate/LVG maps in the discoverer) are keyed by VG name alone, so a
// duplicate silently mixes data from two unrelated VGs. The discoverer and
// the watcher reconciler use this helper to refuse to act on such VGs and
// to surface a clear, actionable error in the LVMVolumeGroup status instead
// of comparing data from the wrong VG.
func findDuplicateVGNames(vgs []internal.VGData) map[string][]string {
	uuidsByName := make(map[string]map[string]struct{}, len(vgs))
	for _, vg := range vgs {
		if vg.VGName == "" {
			continue
		}
		set, ok := uuidsByName[vg.VGName]
		if !ok {
			set = make(map[string]struct{}, 2)
			uuidsByName[vg.VGName] = set
		}
		set[vg.VGUUID] = struct{}{}
	}

	duplicates := make(map[string][]string, len(uuidsByName))
	for name, set := range uuidsByName {
		if len(set) < 2 {
			continue
		}
		uuids := make([]string, 0, len(set))
		for uuid := range set {
			uuids = append(uuids, uuid)
		}
		sort.Strings(uuids)
		duplicates[name] = uuids
	}
	return duplicates
}

// duplicateVGMessage formats the standard human-readable message used in the
// LVMVolumeGroup status when the underlying VG name is ambiguous on the node.
func duplicateVGMessage(vgName string, uuids []string) string {
	return fmt.Sprintf(
		"Multiple LVM VGs share the name %q on the node (UUIDs: %s). LVM cannot resolve the VG by name and the agent cannot safely reconcile this LVMVolumeGroup. Resolve the conflict on the node (e.g. `vgrename --select vg_uuid=<uuid> %s <new-name>`, or remove the stale duplicate) and the agent will recover automatically.",
		vgName, strings.Join(uuids, ", "), vgName,
	)
}

// filterCandidatesByDuplicateVGs drops candidates whose VG name is ambiguous
// on the node. Such candidates were assembled from a mix of LV/PV data
// belonging to different VGs that happen to share a name and would corrupt
// the LVMVolumeGroup status if propagated.
func filterCandidatesByDuplicateVGs(
	candidates []internal.LVMVolumeGroupCandidate,
	duplicateVGs map[string][]string,
) []internal.LVMVolumeGroupCandidate {
	if len(duplicateVGs) == 0 || len(candidates) == 0 {
		return candidates
	}
	filtered := candidates[:0]
	for _, c := range candidates {
		if _, dup := duplicateVGs[c.ActualVGNameOnTheNode]; dup {
			continue
		}
		filtered = append(filtered, c)
	}
	return filtered
}
