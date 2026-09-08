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

// A collector that is declared and incremented but never registered counts into
// nothing: the value stays inside the process and no scrape ever sees it. That is
// exactly what happened to the two utils-command counters, which are incremented
// on every lvm and lsblk call this binary makes and were missing from init().
//
// The test asserts on the registry rather than on the counter, because the counter
// works either way -- it is registration that makes it observable.
func TestEveryUsedCollectorIsRegistered(t *testing.T) {
	m := GetMetrics("test-node")

	m.UtilsCommandsExecutionCount("bd-discoverer", "lsblk").Inc()
	m.UtilsCommandsErrorsCount("bd-discoverer", "lsblk").Inc()
	m.APIMethodsExecutionCount("bd-discoverer", "list").Inc()
	m.ReconcilesCountTotal("bd-discoverer").Inc()

	families, err := metrics.Registry.Gather()
	require.NoError(t, err)

	exported := make(map[string]struct{}, len(families))
	for _, f := range families {
		exported[f.GetName()] = struct{}{}
	}

	for _, name := range []string{
		"sds_node_configurator_utils_commands_execution_count_total",
		"sds_node_configurator_utils_commands_errors_count_total",
		"sds_node_configurator_api_methods_execution_count_total",
		"sds_node_configurator_reconciles_count_total",
	} {
		assert.Contains(t, exported, name, "incremented in this binary but not exported")
	}
}
