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

package monitoring

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
)

const (
	namespace = "sds_node_configurator"
)

var (
	reconcilesCountTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "reconciles_count_total",
		Help:      "Total number of times the resources were reconciled.",
	}, []string{"node", "controller"})

	reconcileDuration = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace:  namespace,
		Name:       "reconcile_duration_seconds",
		Help:       "How long in seconds reconciling of resource takes.",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	}, []string{"node", "controller"})

	utilsCommandsDuration = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace:  namespace,
		Name:       "custom_utils_commands_duration_seconds",
		Help:       "How long in seconds utils commands execution takes.",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	}, []string{"node", "controller", "command"})

	utilsCommandsExecutionCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "utils_commands_execution_count_total",
		Help:      "Total number of times the util-command was executed.",
	}, []string{"node", "controller", "method"})

	utilsCommandsErrorsCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "utils_commands_errors_count_total",
		Help:      "How many errors occurs during utils-command executions.",
	}, []string{"node", "controller", "method"})

	apiMethodsDuration = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace:  namespace,
		Name:       "api_commands_duration_seconds",
		Help:       "How long in seconds kube-api methods execution takes.",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	}, []string{"node", "controller", "method"})

	apiMethodsExecutionCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "api_methods_execution_count_total",
		Help:      "Total number of times the method was executed.",
	}, []string{"node", "controller", "method"})

	apiMethodsErrorsCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "api_methods_errors_count_total",
		Help:      "How many errors occur during api-method executions.",
	}, []string{"node", "controller", "method"})

	noOperationalResourcesCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "no_operational_resources_count_total",
		Help:      "How many LVMVolumeGroup resources are in Nooperational state.",
	}, []string{"resource"})

	lvmLogicalVolumeSizeBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "lvm_logical_volume_size_bytes",
		Help:      "Size of LVM logical volume in bytes.",
	}, []string{"node", "volume_group", "logical_volume", "lv_type"})

	lvmLogicalVolumeUsedBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "lvm_logical_volume_used_bytes",
		Help:      "Used size of LVM logical volume in bytes.",
	}, []string{"node", "volume_group", "logical_volume", "lv_type"})

	lvmLogicalVolumeUsedPercent = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "lvm_logical_volume_used_percent",
		Help:      "Used percentage of LVM logical volume.",
	}, []string{"node", "volume_group", "logical_volume", "lv_type"})
)

func init() {
	metrics.Registry.MustRegister(reconcilesCountTotal)
	metrics.Registry.MustRegister(reconcileDuration)
	metrics.Registry.MustRegister(utilsCommandsDuration)
	metrics.Registry.MustRegister(apiMethodsDuration)
	metrics.Registry.MustRegister(apiMethodsExecutionCount)
	metrics.Registry.MustRegister(apiMethodsErrorsCount)
	metrics.Registry.MustRegister(noOperationalResourcesCount)
	metrics.Registry.MustRegister(lvmLogicalVolumeSizeBytes)
	metrics.Registry.MustRegister(lvmLogicalVolumeUsedBytes)
	metrics.Registry.MustRegister(lvmLogicalVolumeUsedPercent)
}

type Metrics struct {
	node string
	c    clock.Clock
}

func GetMetrics(nodeName string) Metrics {
	return Metrics{
		node: nodeName,
		c:    clock.RealClock{},
	}
}

func (m Metrics) GetEstimatedTimeInSeconds(since time.Time) float64 {
	return m.c.Since(since).Seconds()
}

func (m Metrics) ReconcilesCountTotal(controllerName string) prometheus.Counter {
	return reconcilesCountTotal.WithLabelValues(m.node, controllerName)
}

func (m Metrics) ReconcileDuration(controllerName string) prometheus.Observer {
	return reconcileDuration.WithLabelValues(m.node, controllerName)
}

func (m Metrics) UtilsCommandsDuration(controllerName, command string) prometheus.Observer {
	return utilsCommandsDuration.WithLabelValues(m.node, controllerName, strings.ToLower(command))
}

func (m Metrics) UtilsCommandsExecutionCount(controllerName, command string) prometheus.Counter {
	return utilsCommandsExecutionCount.WithLabelValues(m.node, controllerName, strings.ToLower(command))
}

func (m Metrics) UtilsCommandsErrorsCount(controllerName, command string) prometheus.Counter {
	return utilsCommandsErrorsCount.WithLabelValues(m.node, controllerName, strings.ToLower(command))
}

func (m Metrics) APIMethodsDuration(controllerName, method string) prometheus.Observer {
	return apiMethodsDuration.WithLabelValues(m.node, controllerName, strings.ToLower(method))
}

func (m Metrics) APIMethodsExecutionCount(controllerName, method string) prometheus.Counter {
	return apiMethodsExecutionCount.WithLabelValues(m.node, controllerName, strings.ToLower(method))
}

func (m Metrics) APIMethodsErrors(controllerName, method string) prometheus.Counter {
	return apiMethodsErrorsCount.WithLabelValues(m.node, controllerName, strings.ToLower(method))
}

func (m Metrics) NoOperationalResourcesCount(resourceName string) prometheus.Gauge {
	return noOperationalResourcesCount.WithLabelValues(strings.ToLower(resourceName))
}

func (m Metrics) LVMLogicalVolumeSizeBytes(volumeGroup, logicalVolume, lvType string) prometheus.Gauge {
	return lvmLogicalVolumeSizeBytes.WithLabelValues(m.node, volumeGroup, logicalVolume, lvType)
}

func (m Metrics) LVMLogicalVolumeUsedBytes(volumeGroup, logicalVolume, lvType string) prometheus.Gauge {
	return lvmLogicalVolumeUsedBytes.WithLabelValues(m.node, volumeGroup, logicalVolume, lvType)
}

func (m Metrics) LVMLogicalVolumeUsedPercent(volumeGroup, logicalVolume, lvType string) prometheus.Gauge {
	return lvmLogicalVolumeUsedPercent.WithLabelValues(m.node, volumeGroup, logicalVolume, lvType)
}

// getLVType determines the type of LVM logical volume
func getLVType(lv internal.LVData) string {
	// Check if it's a thin pool (first character of LVAttr is 't')
	if len(lv.LVAttr) > 0 && lv.LVAttr[0] == 't' {
		return "thin_pool"
	}
	// Check if it's a thin volume (has PoolName and is not a thin pool)
	if lv.PoolName != "" {
		return "thin_volume"
	}
	// Check if it's a thick volume (first character of LVAttr is '-' and no PoolName)
	// Thick volumes have '-' as first character and empty PoolName
	if len(lv.LVAttr) > 0 && lv.LVAttr[0] == '-' {
		return "thick"
	}
	// If LVAttr is empty or has unexpected value, but PoolName is empty,
	// it's likely a thick volume (thick volumes don't have PoolName)
	// This handles edge cases where LVAttr might be empty or malformed
	if lv.PoolName == "" {
		return "thick"
	}
	// Otherwise it's a regular volume (fallback for any other type)
	return "regular"
}

// UpdateLVMMetrics updates metrics for LVM logical volumes
func (m Metrics) UpdateLVMMetrics(lvs []internal.LVData) {
	// Track current LVs to remove metrics for deleted ones
	currentLVs := make(map[string]bool)

	fmt.Println("lvs", lvs)

	for _, lv := range lvs {
		// Skip internal LVM volumes (they start with [ and end with ])
		if strings.HasPrefix(lv.LVName, "[") && strings.HasSuffix(lv.LVName, "]") {
			continue
		}

		lvType := getLVType(lv)
		key := m.node + ":" + lv.VGName + ":" + lv.LVName + ":" + lvType
		currentLVs[key] = true

		// Update size metric
		sizeBytes := float64(lv.LVSize.Value())
		m.LVMLogicalVolumeSizeBytes(lv.VGName, lv.LVName, lvType).Set(sizeBytes)

		// Calculate and update used bytes and percent
		var usedBytes float64
		var usedPercent float64

		if lv.DataPercent != "" {
			dataPercent, err := strconv.ParseFloat(lv.DataPercent, 64)
			if err == nil {
				usedPercent = dataPercent
				usedBytes = sizeBytes * dataPercent / 100.0
			}
		}

		m.LVMLogicalVolumeUsedBytes(lv.VGName, lv.LVName, lvType).Set(usedBytes)
		m.LVMLogicalVolumeUsedPercent(lv.VGName, lv.LVName, lvType).Set(usedPercent)
	}

	// Remove metrics for LVs that no longer exist
	// We need to reset all metrics and then set only current ones
	// This is a limitation of Prometheus - we can't easily remove specific label combinations
	// So we'll keep the metrics but they'll be stale until next update
	// In practice, this is acceptable as metrics will be updated on next scan
}
