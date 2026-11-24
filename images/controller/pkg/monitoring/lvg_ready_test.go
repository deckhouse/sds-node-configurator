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

package monitoring

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

const lvgReadyName = namespace + "_lvm_volume_group_ready"

func TestLVGReadinessExportsOneSeriesPerGroup(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	require.NoError(t, RegisterLVGReadiness(reg, func() []LVGReadiness {
		return []LVGReadiness{
			{Name: "vg-a", Node: "node-1", Condition: "Ready", Reason: "ValidConditionStates", Ready: true},
			{Name: "vg-b", Node: "node-2", Condition: "AgentReady", Reason: "PodNotReady"},
		}
	}))

	expected := `
# HELP sds_node_configurator_lvm_volume_group_ready Whether the Ready condition of an LVMVolumeGroup is True (1) or not (0). condition and reason name the condition that decides it.
# TYPE sds_node_configurator_lvm_volume_group_ready gauge
sds_node_configurator_lvm_volume_group_ready{condition="AgentReady",lvm_volume_group="vg-b",node="node-2",reason="PodNotReady"} 0
sds_node_configurator_lvm_volume_group_ready{condition="Ready",lvm_volume_group="vg-a",node="node-1",reason="ValidConditionStates"} 1
`
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expected), lvgReadyName))
}

func TestLVGReadinessFollowsTheSourceOnEveryScrape(t *testing.T) {
	current := []LVGReadiness{{Name: "vg-a", Node: "node-1", Condition: "NodeReady", Reason: "NodeNotReady"}}

	reg := prometheus.NewPedanticRegistry()
	require.NoError(t, RegisterLVGReadiness(reg, func() []LVGReadiness { return current }))

	count, err := testutil.GatherAndCount(reg, lvgReadyName)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	current = nil

	count, err = testutil.GatherAndCount(reg, lvgReadyName)
	require.NoError(t, err)
	require.Equal(t, 0, count)
}
