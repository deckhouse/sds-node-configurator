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

package scanner

import (
	"fmt"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

// dropPublishedPVs removes from a fresh scan the Physical Volumes that sit on
// devices this cluster published to workloads as block volumes, together with
// the Volume Groups and Logical Volumes that no remaining PV accounts for.
//
// It is the second line behind lvm's own filter, which already rejects these
// devices (utils.PublishedDevices): it catches a PV read in the window between
// the device being published and the filter learning of it. It has to run before
// utils.EnsureVGActivation, and it has to take the Volume Groups along right
// away rather than leave them to pruneToPresentPVs at the end of the pass: a
// guest Volume Group left in the list gets its `vgchange -ay` on the host before
// the prune would have reached it.
//
// A Volume Group with one PV on a published device and another on a device of
// the node's own stays, with its Logical Volumes: no Volume Group of the node's
// may vanish over a guess, and the next pass rejects the device in lvm anyway.
// Nothing about a healthy node produces such a group; it is the direction the
// filter fails in, not a layout it expects.
func dropPublishedPVs(
	log logger.Logger,
	pvs []internal.PVData, vgs []internal.VGData, lvs []internal.LVData,
	published utils.PublishedBlockDevices,
) ([]internal.PVData, []internal.VGData, []internal.LVData) {
	kept := utils.FilterPublishedPVs(log, pvs, published)
	dropped := len(pvs) - len(kept)
	if dropped == 0 {
		return pvs, vgs, lvs
	}
	log.Info(fmt.Sprintf("dropped %d PV(s) on devices published to workloads as block volumes", dropped))

	vgs = utils.FilterVGsByPresentPVs(vgs, kept)
	lvs = utils.FilterLVsByPresentVGs(lvs, vgs)
	return kept, vgs, lvs
}

// pruneToPresentPVs is the last step of a pass: once any filter has dropped a
// Physical Volume, the Volume Groups no remaining PV accounts for and their
// Logical Volumes go too, so the three caches describe the same node.
//
// Keyed on the PV count before any filter of the pass, not on the count before
// the filters that follow activation: utils.EnsureVGActivation re-lists Volume
// Groups and Logical Volumes raw after it activates something, and that re-list
// brings back the groups an earlier filter had already taken out.
func pruneToPresentPVs(
	log logger.Logger,
	pvs []internal.PVData, vgs []internal.VGData, lvs []internal.LVData,
	rawPVCount int,
) ([]internal.VGData, []internal.LVData) {
	if len(pvs) >= rawPVCount {
		return vgs, lvs
	}

	beforeVG, beforeLV := len(vgs), len(lvs)
	vgs = utils.FilterVGsByPresentPVs(vgs, pvs)
	lvs = utils.FilterLVsByPresentVGs(lvs, vgs)
	log.Info(fmt.Sprintf(
		"cache pruned to local view: VGs %d -> %d, LVs %d -> %d",
		beforeVG, len(vgs), beforeLV, len(lvs),
	))
	return vgs, lvs
}
