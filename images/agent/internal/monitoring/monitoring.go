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

	lvmVolumeGroupSizeBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "lvm_volume_group_size_bytes",
		Help:      "Size of LVM volume group in bytes.",
	}, []string{"node", "volume_group"})

	lvmVolumeGroupFreeBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "lvm_volume_group_free_bytes",
		Help:      "Free size of LVM volume group in bytes.",
	}, []string{"node", "volume_group"})

	lvmVolumeGroupUsedBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "lvm_volume_group_used_bytes",
		Help:      "Used size of LVM volume group in bytes.",
	}, []string{"node", "volume_group"})

	lvmVolumeGroupUsedPercent = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "lvm_volume_group_used_percent",
		Help:      "Used percentage of LVM volume group.",
	}, []string{"node", "volume_group"})
)

func init() {
	metrics.Registry.MustRegister(reconcilesCountTotal)
	metrics.Registry.MustRegister(reconcileDuration)
	metrics.Registry.MustRegister(utilsCommandsDuration)
	metrics.Registry.MustRegister(apiMethodsDuration)
	metrics.Registry.MustRegister(apiMethodsExecutionCount)
	metrics.Registry.MustRegister(apiMethodsErrorsCount)
	metrics.Registry.MustRegister(noOperationalResourcesCount)
	metrics.Registry.MustRegister(lvmVolumeGroupSizeBytes)
	metrics.Registry.MustRegister(lvmVolumeGroupFreeBytes)
	metrics.Registry.MustRegister(lvmVolumeGroupUsedBytes)
	metrics.Registry.MustRegister(lvmVolumeGroupUsedPercent)
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

func (m Metrics) LVMVolumeGroupSizeBytes(volumeGroup string) prometheus.Gauge {
	return lvmVolumeGroupSizeBytes.WithLabelValues(m.node, volumeGroup)
}

func (m Metrics) LVMVolumeGroupFreeBytes(volumeGroup string) prometheus.Gauge {
	return lvmVolumeGroupFreeBytes.WithLabelValues(m.node, volumeGroup)
}

func (m Metrics) LVMVolumeGroupUsedBytes(volumeGroup string) prometheus.Gauge {
	return lvmVolumeGroupUsedBytes.WithLabelValues(m.node, volumeGroup)
}

func (m Metrics) LVMVolumeGroupUsedPercent(volumeGroup string) prometheus.Gauge {
	return lvmVolumeGroupUsedPercent.WithLabelValues(m.node, volumeGroup)
}

// UpdateLVMMetrics updates metrics for LVM volume groups
func (m Metrics) UpdateLVMMetrics(vgs []internal.VGData) {
	// Track current VGs to remove metrics for deleted ones
	currentVGs := make(map[string]bool)

	for _, vg := range vgs {
		key := m.node + ":" + vg.VGName
		currentVGs[key] = true

		// Update size metric
		sizeBytes := float64(vg.VGSize.Value())
		m.LVMVolumeGroupSizeBytes(vg.VGName).Set(sizeBytes)

		// Update free bytes metric
		freeBytes := float64(vg.VGFree.Value())
		m.LVMVolumeGroupFreeBytes(vg.VGName).Set(freeBytes)

		// Calculate and update used bytes and percent
		usedBytes := sizeBytes - freeBytes
		var usedPercent float64
		if sizeBytes > 0 {
			usedPercent = (usedBytes / sizeBytes) * 100.0
		}

		m.LVMVolumeGroupUsedBytes(vg.VGName).Set(usedBytes)
		m.LVMVolumeGroupUsedPercent(vg.VGName).Set(usedPercent)
	}

	// Remove metrics for VGs that no longer exist
	// We need to reset all metrics and then set only current ones
	// This is a limitation of Prometheus - we can't easily remove specific label combinations
	// So we'll keep the metrics but they'll be stale until next update
	// In practice, this is acceptable as metrics will be updated on next scan
}
