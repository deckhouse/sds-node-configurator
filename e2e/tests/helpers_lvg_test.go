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

package tests

import (
	"fmt"
	"strings"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
)

// describeLVGStatus renders the part of an LVMVolumeGroup's status that explains
// why it is not Ready, for use in a Gomega failure message.
//
// It exists because "Phase should be Ready, got Pending" is not a diagnosis. The
// phase is derived by the conditions watcher from the conditions, and the three
// ways to sit at Pending look identical from the phase alone:
//
//   - a condition is missing entirely, so the watcher is still waiting for the
//     set to be complete (Ready says "wait for conditions to got configured");
//   - a condition is False and names the reason;
//   - status.nodes is empty, so neither NodeReady nor AgentReady is ever written
//     for the resource.
//
// A spec that waits fifteen minutes and then reports only the phase leaves
// nothing in the log to work from — which is exactly what happened to the
// LVMLogicalVolume clone spec's BeforeAll on a real run.
func describeLVGStatus(lvg *v1alpha1.LVMVolumeGroup) string {
	var b strings.Builder

	fmt.Fprintf(&b, "vgSize=%s vgFree=%s thinPools=%d conditions=%d",
		lvg.Status.VGSize.String(), lvg.Status.VGFree.String(),
		len(lvg.Status.ThinPools), len(lvg.Status.Conditions))

	for i := range lvg.Status.Conditions {
		c := &lvg.Status.Conditions[i]
		fmt.Fprintf(&b, "\n  condition %s status=%s reason=%s msg=%s", c.Type, c.Status, c.Reason, c.Message)
	}
	if len(lvg.Status.Conditions) == 0 {
		b.WriteString("\n  (no conditions at all — the agent has not touched the resource yet)")
	}

	// Named explicitly because an empty list is the state in which NodeReady and
	// AgentReady are never written, and that is indistinguishable from "not yet"
	// unless it is said out loud.
	if len(lvg.Status.Nodes) == 0 {
		b.WriteString("\n  status.nodes is EMPTY — the discoverer has not reported this Volume Group")
	}
	for i := range lvg.Status.Nodes {
		n := &lvg.Status.Nodes[i]
		fmt.Fprintf(&b, "\n  node %s: %d device(s), %d fileDevice(s)", n.Name, len(n.Devices), len(n.FileDevices))
		for j := range n.Devices {
			d := &n.Devices[j]
			fmt.Fprintf(&b, "\n    device %s path=%s pvSize=%s", d.BlockDevice, d.Path, d.PVSize.String())
		}
	}
	for i := range lvg.Status.ThinPools {
		tp := &lvg.Status.ThinPools[i]
		fmt.Fprintf(&b, "\n  thinPool %s ready=%t actualSize=%s msg=%s", tp.Name, tp.Ready, tp.ActualSize.String(), tp.Message)
	}

	return b.String()
}
