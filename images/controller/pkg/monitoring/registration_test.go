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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// The mirror of the agent's test, in the binary that shares this metric set. A
// collector that is declared and incremented but never registered counts into
// nothing: the value stays inside the process and no scrape ever sees it. That is
// what happened to the two utils-command counters, which are incremented on every
// lvm and lsblk call and were missing from init().
//
// The assertion is on the registry rather than on the counter, because the counter
// works either way -- it is registration that makes it observable.
func TestEveryUsedCollectorIsRegistered(t *testing.T) {
	m := GetMetrics("test-node")

	m.UtilsCommandsExecutionCount("test-controller", "lvs").Inc()
	m.UtilsCommandsErrorsCount("test-controller", "lvs").Inc()
	m.APIMethodsExecutionCount("test-controller", "list").Inc()
	m.APIMethodsErrors("test-controller", "list").Inc()
	m.ReconcilesCountTotal("test-controller").Inc()
	m.NoOperationalResourcesCount("LVMVolumeGroup").Set(0)
	m.LVGNodeMissingSince("test-lvg", "test-node").Set(0)
	m.MissingNodeResourcesHeld("LVMVolumeGroup", "test-node", "test-reason").Set(0)

	families, err := metrics.Registry.Gather()
	require.NoError(t, err)

	exported := make(map[string]struct{}, len(families))
	for _, f := range families {
		exported[f.GetName()] = struct{}{}
	}

	for _, name := range []string{
		namespace + "_utils_commands_execution_count_total",
		namespace + "_utils_commands_errors_count_total",
		namespace + "_api_methods_execution_count_total",
		namespace + "_api_methods_errors_count_total",
		namespace + "_reconciles_count_total",
		namespace + "_no_operational_resources_count_total",
		namespace + "_lvm_volume_group_node_missing_timestamp_seconds",
		namespace + "_missing_node_resources_held",
	} {
		assert.Contains(t, exported, name, "incremented in this binary but not exported")
	}
}
