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
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
)

// wantSurface is every series family this agent publishes: name, Prometheus type
// and label names, sorted.
//
// It is spelled out rather than derived because that is the whole point. A metric
// name and its label set are the contract with everything outside this repository
// -- dashboards, recording rules, alerts -- and none of it lives here, so nothing
// else in CI can notice a rename. A type change is the same kind of break and
// harder to spot: turning a summary into a histogram deletes every
// `{quantile="..."}` series and creates `_bucket` ones instead, and a graph built
// on the first shape goes silently empty rather than red.
//
// Updating this list is expected when a metric is deliberately added or renamed.
// The failure is there to make that a decision instead of an accident, and to say
// in the diff which series consumers have to be migrated.
var wantSurface = []string{
	"sds_node_configurator_api_commands_duration_seconds                    SUMMARY controller,method,node",
	"sds_node_configurator_api_methods_errors_count_total                   COUNTER controller,method,node",
	"sds_node_configurator_api_methods_execution_count_total                COUNTER controller,method,node",
	"sds_node_configurator_custom_utils_commands_duration_seconds           SUMMARY command,controller,node",
	"sds_node_configurator_file_device_size_bytes                           GAUGE   file_device,lvg_name,node,volume_group",
	"sds_node_configurator_file_devices_directory_allocated_bytes           GAUGE   directory,node",
	"sds_node_configurator_file_devices_directory_free_bytes                GAUGE   directory,node",
	"sds_node_configurator_file_devices_directory_total_bytes               GAUGE   directory,node",
	"sds_node_configurator_lvg_thin_pool_actual_size_bytes                  GAUGE   lvg_name,node,thin_pool,volume_group",
	"sds_node_configurator_lvg_thin_pool_allocated_size_bytes               GAUGE   lvg_name,node,thin_pool,volume_group",
	"sds_node_configurator_lvg_thin_pool_allocation_limit_bytes             GAUGE   lvg_name,node,thin_pool,volume_group",
	"sds_node_configurator_lvg_thin_pool_used_size_bytes                    GAUGE   lvg_name,node,thin_pool,volume_group",
	"sds_node_configurator_lvg_vg_free_bytes                                GAUGE   lvg_name,node,volume_group",
	"sds_node_configurator_lvg_vg_size_bytes                                GAUGE   lvg_name,node,volume_group",
	"sds_node_configurator_lvm_activation_total                             COUNTER node,result,volume_group",
	"sds_node_configurator_lvm_logical_volume_size_bytes                    GAUGE   logical_volume,node,volume_group",
	"sds_node_configurator_lvm_logical_volume_used_bytes                    GAUGE   logical_volume,node,volume_group",
	"sds_node_configurator_lvm_logical_volume_used_percent                  GAUGE   logical_volume,node,volume_group",
	"sds_node_configurator_lvm_thin_pool_metadata_used_percent              GAUGE   node,thin_pool,volume_group",
	"sds_node_configurator_lvm_thin_pool_size_bytes                         GAUGE   node,thin_pool,volume_group",
	"sds_node_configurator_lvm_thin_pool_used_bytes                         GAUGE   node,thin_pool,volume_group",
	"sds_node_configurator_lvm_thin_pool_used_percent                       GAUGE   node,thin_pool,volume_group",
	"sds_node_configurator_lvm_volume_group_free_bytes                      GAUGE   node,volume_group",
	"sds_node_configurator_lvm_volume_group_import_refused_total            COUNTER lvg_name,node,volume_group",
	"sds_node_configurator_lvm_volume_group_size_bytes                      GAUGE   node,volume_group",
	"sds_node_configurator_lvm_volume_group_used_bytes                      GAUGE   node,volume_group",
	"sds_node_configurator_lvm_volume_group_used_percent                    GAUGE   node,volume_group",
	"sds_node_configurator_no_operational_resources_count_total             GAUGE   resource",
	"sds_node_configurator_reconcile_duration_seconds                       SUMMARY controller,node",
	"sds_node_configurator_reconciles_count_total                           COUNTER controller,node",
	"sds_node_configurator_utils_commands_errors_count_total                COUNTER controller,method,node",
	"sds_node_configurator_utils_commands_execution_count_total             COUNTER controller,method,node",
}

// touchEverySeries writes one sample into every family, since Gather() reports
// only families that carry at least one series.
func touchEverySeries(m *Metrics) {
	m.ReconcilesCountTotal("c").Inc()
	m.ReconcileDuration("c").Observe(1)
	m.UtilsCommandsDuration("c", "cmd").Observe(1)
	m.UtilsCommandsExecutionCount("c", "cmd").Inc()
	m.UtilsCommandsErrorsCount("c", "cmd").Inc()
	m.APIMethodsDuration("c", "meth").Observe(1)
	m.APIMethodsExecutionCount("c", "meth").Inc()
	m.APIMethodsErrors("c", "meth").Inc()
	m.NoOperationalResourcesCount("r").Set(1)
	m.LVMVolumeGroupSizeBytes("vg").Set(1)
	m.LVMVolumeGroupFreeBytes("vg").Set(1)
	m.LVMVolumeGroupUsedBytes("vg").Set(1)
	m.LVMVolumeGroupUsedPercent("vg").Set(1)
	m.LVMThinPoolSizeBytes("vg", "tp").Set(1)
	m.LVMThinPoolUsedBytes("vg", "tp").Set(1)
	m.LVMThinPoolUsedPercent("vg", "tp").Set(1)
	m.LVMThinPoolMetadataUsedPercent("vg", "tp").Set(1)
	m.LVMLogicalVolumeSizeBytes("vg", "lv").Set(1)
	m.LVMLogicalVolumeUsedBytes("vg", "lv").Set(1)
	m.LVMLogicalVolumeUsedPercent("vg", "lv").Set(1)
	m.LVGVGSizeBytes("lvg", "vg").Set(1)
	m.LVGVGFreeBytes("lvg", "vg").Set(1)
	m.LVGThinPoolActualSizeBytes("lvg", "vg", "tp").Set(1)
	m.LVGThinPoolAllocatedSizeBytes("lvg", "vg", "tp").Set(1)
	m.LVGThinPoolUsedSizeBytes("lvg", "vg", "tp").Set(1)
	m.LVGThinPoolAllocationLimitBytes("lvg", "vg", "tp").Set(1)
	m.LVMActivationTotal("vg", "success").Inc()
	m.LVMVolumeGroupImportRefusedTotal("vg", "lvg").Inc()

	// The four file-device gauges have no per-series accessor; they are written
	// only through this method.
	m.UpdateFileDeviceMetrics(
		map[string]v1alpha1.LVMVolumeGroup{
			"lvg": {
				ObjectMeta: metav1.ObjectMeta{Name: "lvg"},
				Spec:       v1alpha1.LVMVolumeGroupSpec{ActualVGNameOnTheNode: "vg"},
				Status: v1alpha1.LVMVolumeGroupStatus{
					Nodes: []v1alpha1.LVMVolumeGroupNode{{
						Name:        m.node,
						FileDevices: []v1alpha1.LVMVolumeGroupFileDevice{{Name: "fd", Size: resource.MustParse("1Gi")}},
					}},
				},
			},
		},
		[]FileDeviceDirectoryUsage{{
			Directory:      "/var/lib/deckhouse/sds/file-devices",
			FreeBytes:      1,
			TotalBytes:     2,
			AllocatedBytes: 1,
			Known:          true,
		}},
	)
}

// TestSeriesSurfaceIsUnchanged pins the name, type and labels of every family the
// agent publishes. See wantSurface for why this is spelled out.
func TestSeriesSurfaceIsUnchanged(t *testing.T) {
	touchEverySeries(GetMetrics("node-1"))

	families, err := metrics.Registry.Gather()
	require.NoError(t, err)

	var got []string
	for _, f := range families {
		if !strings.HasPrefix(f.GetName(), "sds_node_configurator") {
			continue
		}
		require.NotEmpty(t, f.GetMetric(), "family %s reported with no series", f.GetName())

		labels := make([]string, 0, len(f.GetMetric()[0].GetLabel()))
		for _, l := range f.GetMetric()[0].GetLabel() {
			labels = append(labels, l.GetName())
		}
		sort.Strings(labels)

		got = append(got, fmt.Sprintf("%-70s %-7s %s",
			f.GetName(), f.GetType().String(), strings.Join(labels, ",")))
	}
	sort.Strings(got)

	assert.Equal(t, wantSurface, got,
		"the published series changed; every consumer of a renamed or retyped series "+
			"lives outside this repository, so update wantSurface only together with them")
}
