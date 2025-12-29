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
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
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

	lvmThinPoolSizeBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "lvm_thin_pool_size_bytes",
		Help:      "Size of LVM thin pool in bytes.",
	}, []string{"node", "volume_group", "thin_pool"})

	lvmThinPoolUsedBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "lvm_thin_pool_used_bytes",
		Help:      "Used size of LVM thin pool in bytes.",
	}, []string{"node", "volume_group", "thin_pool"})

	lvmThinPoolUsedPercent = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "lvm_thin_pool_used_percent",
		Help:      "Used percentage of LVM thin pool.",
	}, []string{"node", "volume_group", "thin_pool"})

	lvmThinPoolMetadataUsedPercent = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "lvm_thin_pool_metadata_used_percent",
		Help:      "Used percentage of LVM thin pool metadata.",
	}, []string{"node", "volume_group", "thin_pool"})

	lvmLogicalVolumeSizeBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "lvm_logical_volume_size_bytes",
		Help:      "Size of LVM logical volume in bytes.",
	}, []string{"node", "volume_group", "logical_volume"})

	lvmLogicalVolumeUsedBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "lvm_logical_volume_used_bytes",
		Help:      "Used size of LVM logical volume in bytes.",
	}, []string{"node", "volume_group", "logical_volume"})

	lvmLogicalVolumeUsedPercent = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "lvm_logical_volume_used_percent",
		Help:      "Used percentage of LVM logical volume.",
	}, []string{"node", "volume_group", "logical_volume"})

	// LVMVolumeGroup status metrics
	lvgVGSizeBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "lvg_vg_size_bytes",
		Help:      "VG size from LVMVolumeGroup status in bytes.",
	}, []string{"node", "lvg_name", "volume_group"})

	lvgVGFreeBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "lvg_vg_free_bytes",
		Help:      "VG free space from LVMVolumeGroup status in bytes.",
	}, []string{"node", "lvg_name", "volume_group"})

	lvgThinPoolActualSizeBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "lvg_thin_pool_actual_size_bytes",
		Help:      "Actual size of thin pool from LVMVolumeGroup status in bytes.",
	}, []string{"node", "lvg_name", "volume_group", "thin_pool"})

	lvgThinPoolAllocatedSizeBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "lvg_thin_pool_allocated_size_bytes",
		Help:      "Allocated size of thin pool from LVMVolumeGroup status in bytes.",
	}, []string{"node", "lvg_name", "volume_group", "thin_pool"})

	lvgThinPoolUsedSizeBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "lvg_thin_pool_used_size_bytes",
		Help:      "Used size of thin pool from LVMVolumeGroup status in bytes.",
	}, []string{"node", "lvg_name", "volume_group", "thin_pool"})

	lvgThinPoolAllocationLimitBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "lvg_thin_pool_allocation_limit_bytes",
		Help:      "Maximum allocatable size of thin pool considering allocation limit (actual_size * allocation_limit / 100) in bytes.",
	}, []string{"node", "lvg_name", "volume_group", "thin_pool"})
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
	metrics.Registry.MustRegister(lvmThinPoolSizeBytes)
	metrics.Registry.MustRegister(lvmThinPoolUsedBytes)
	metrics.Registry.MustRegister(lvmThinPoolUsedPercent)
	metrics.Registry.MustRegister(lvmThinPoolMetadataUsedPercent)
	metrics.Registry.MustRegister(lvmLogicalVolumeSizeBytes)
	metrics.Registry.MustRegister(lvmLogicalVolumeUsedBytes)
	metrics.Registry.MustRegister(lvmLogicalVolumeUsedPercent)
	metrics.Registry.MustRegister(lvgVGSizeBytes)
	metrics.Registry.MustRegister(lvgVGFreeBytes)
	metrics.Registry.MustRegister(lvgThinPoolActualSizeBytes)
	metrics.Registry.MustRegister(lvgThinPoolAllocatedSizeBytes)
	metrics.Registry.MustRegister(lvgThinPoolUsedSizeBytes)
	metrics.Registry.MustRegister(lvgThinPoolAllocationLimitBytes)
}

type Metrics struct {
	node string
	c    clock.Clock

	// State for tracking previous metrics to cleanup stale ones
	mu                sync.Mutex
	previousVGs       map[string]bool
	previousThinPools map[string]bool
	previousLVs       map[string]bool
	previousLVGVGs    map[string]bool
	previousLVGTPs    map[string]bool
}

func GetMetrics(nodeName string) *Metrics {
	return &Metrics{
		node:              nodeName,
		c:                 clock.RealClock{},
		previousVGs:       make(map[string]bool),
		previousThinPools: make(map[string]bool),
		previousLVs:       make(map[string]bool),
		previousLVGVGs:    make(map[string]bool),
		previousLVGTPs:    make(map[string]bool),
	}
}

func (m *Metrics) GetEstimatedTimeInSeconds(since time.Time) float64 {
	return m.c.Since(since).Seconds()
}

func (m *Metrics) ReconcilesCountTotal(controllerName string) prometheus.Counter {
	return reconcilesCountTotal.WithLabelValues(m.node, controllerName)
}

func (m *Metrics) ReconcileDuration(controllerName string) prometheus.Observer {
	return reconcileDuration.WithLabelValues(m.node, controllerName)
}

func (m *Metrics) UtilsCommandsDuration(controllerName, command string) prometheus.Observer {
	return utilsCommandsDuration.WithLabelValues(m.node, controllerName, strings.ToLower(command))
}

func (m *Metrics) UtilsCommandsExecutionCount(controllerName, command string) prometheus.Counter {
	return utilsCommandsExecutionCount.WithLabelValues(m.node, controllerName, strings.ToLower(command))
}

func (m *Metrics) UtilsCommandsErrorsCount(controllerName, command string) prometheus.Counter {
	return utilsCommandsErrorsCount.WithLabelValues(m.node, controllerName, strings.ToLower(command))
}

func (m *Metrics) APIMethodsDuration(controllerName, method string) prometheus.Observer {
	return apiMethodsDuration.WithLabelValues(m.node, controllerName, strings.ToLower(method))
}

func (m *Metrics) APIMethodsExecutionCount(controllerName, method string) prometheus.Counter {
	return apiMethodsExecutionCount.WithLabelValues(m.node, controllerName, strings.ToLower(method))
}

func (m *Metrics) APIMethodsErrors(controllerName, method string) prometheus.Counter {
	return apiMethodsErrorsCount.WithLabelValues(m.node, controllerName, strings.ToLower(method))
}

func (m *Metrics) NoOperationalResourcesCount(resourceName string) prometheus.Gauge {
	return noOperationalResourcesCount.WithLabelValues(strings.ToLower(resourceName))
}

func (m *Metrics) LVMVolumeGroupSizeBytes(volumeGroup string) prometheus.Gauge {
	return lvmVolumeGroupSizeBytes.WithLabelValues(m.node, volumeGroup)
}

func (m *Metrics) LVMVolumeGroupFreeBytes(volumeGroup string) prometheus.Gauge {
	return lvmVolumeGroupFreeBytes.WithLabelValues(m.node, volumeGroup)
}

func (m *Metrics) LVMVolumeGroupUsedBytes(volumeGroup string) prometheus.Gauge {
	return lvmVolumeGroupUsedBytes.WithLabelValues(m.node, volumeGroup)
}

func (m *Metrics) LVMVolumeGroupUsedPercent(volumeGroup string) prometheus.Gauge {
	return lvmVolumeGroupUsedPercent.WithLabelValues(m.node, volumeGroup)
}

func (m *Metrics) LVMThinPoolSizeBytes(volumeGroup, thinPool string) prometheus.Gauge {
	return lvmThinPoolSizeBytes.WithLabelValues(m.node, volumeGroup, thinPool)
}

func (m *Metrics) LVMThinPoolUsedBytes(volumeGroup, thinPool string) prometheus.Gauge {
	return lvmThinPoolUsedBytes.WithLabelValues(m.node, volumeGroup, thinPool)
}

func (m *Metrics) LVMThinPoolUsedPercent(volumeGroup, thinPool string) prometheus.Gauge {
	return lvmThinPoolUsedPercent.WithLabelValues(m.node, volumeGroup, thinPool)
}

func (m *Metrics) LVMThinPoolMetadataUsedPercent(volumeGroup, thinPool string) prometheus.Gauge {
	return lvmThinPoolMetadataUsedPercent.WithLabelValues(m.node, volumeGroup, thinPool)
}

func (m *Metrics) LVMLogicalVolumeSizeBytes(volumeGroup, logicalVolume string) prometheus.Gauge {
	return lvmLogicalVolumeSizeBytes.WithLabelValues(m.node, volumeGroup, logicalVolume)
}

func (m *Metrics) LVMLogicalVolumeUsedBytes(volumeGroup, logicalVolume string) prometheus.Gauge {
	return lvmLogicalVolumeUsedBytes.WithLabelValues(m.node, volumeGroup, logicalVolume)
}

func (m *Metrics) LVMLogicalVolumeUsedPercent(volumeGroup, logicalVolume string) prometheus.Gauge {
	return lvmLogicalVolumeUsedPercent.WithLabelValues(m.node, volumeGroup, logicalVolume)
}

func (m *Metrics) LVGVGSizeBytes(lvgName, volumeGroup string) prometheus.Gauge {
	return lvgVGSizeBytes.WithLabelValues(m.node, lvgName, volumeGroup)
}

func (m *Metrics) LVGVGFreeBytes(lvgName, volumeGroup string) prometheus.Gauge {
	return lvgVGFreeBytes.WithLabelValues(m.node, lvgName, volumeGroup)
}

func (m *Metrics) LVGThinPoolActualSizeBytes(lvgName, volumeGroup, thinPool string) prometheus.Gauge {
	return lvgThinPoolActualSizeBytes.WithLabelValues(m.node, lvgName, volumeGroup, thinPool)
}

func (m *Metrics) LVGThinPoolAllocatedSizeBytes(lvgName, volumeGroup, thinPool string) prometheus.Gauge {
	return lvgThinPoolAllocatedSizeBytes.WithLabelValues(m.node, lvgName, volumeGroup, thinPool)
}

func (m *Metrics) LVGThinPoolUsedSizeBytes(lvgName, volumeGroup, thinPool string) prometheus.Gauge {
	return lvgThinPoolUsedSizeBytes.WithLabelValues(m.node, lvgName, volumeGroup, thinPool)
}

func (m *Metrics) LVGThinPoolAllocationLimitBytes(lvgName, volumeGroup, thinPool string) prometheus.Gauge {
	return lvgThinPoolAllocationLimitBytes.WithLabelValues(m.node, lvgName, volumeGroup, thinPool)
}

// isThinPool determines if an LVM logical volume is a thin pool
func isThinPool(lv internal.LVData) bool {
	return len(lv.LVAttr) > 0 && lv.LVAttr[0] == 't'
}

// UpdateLVMMetrics updates metrics for LVM volume groups, thin pools, and logical volumes.
// Only VGs and LVs that belong to managed VGs (from LVMVolumeGroup resources) are included.
// Returns collected parsing errors that should be logged by the caller.
func (m *Metrics) UpdateLVMMetrics(vgs []internal.VGData, lvs []internal.LVData, managedVGs map[string]struct{}) []error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var errs []error

	// Track current VGs to remove metrics for deleted ones
	currentVGs := make(map[string]bool)

	for _, vg := range vgs {
		// Skip VGs that are not managed by LVMVolumeGroup resources
		if _, managed := managedVGs[vg.VGName]; !managed {
			continue
		}

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

	// Remove stale VG metrics
	for key := range m.previousVGs {
		if !currentVGs[key] {
			parts := strings.SplitN(key, ":", 2)
			if len(parts) == 2 {
				lvmVolumeGroupSizeBytes.DeleteLabelValues(parts[0], parts[1])
				lvmVolumeGroupFreeBytes.DeleteLabelValues(parts[0], parts[1])
				lvmVolumeGroupUsedBytes.DeleteLabelValues(parts[0], parts[1])
				lvmVolumeGroupUsedPercent.DeleteLabelValues(parts[0], parts[1])
			}
		}
	}
	m.previousVGs = currentVGs

	// Update metrics for thin pools and logical volumes
	currentThinPools := make(map[string]bool)
	currentLVs := make(map[string]bool)

	for _, lv := range lvs {
		// Skip LVs that belong to VGs not managed by LVMVolumeGroup resources
		if _, managed := managedVGs[lv.VGName]; !managed {
			continue
		}

		// Skip internal LVM volumes (they start with [ and end with ])
		if strings.HasPrefix(lv.LVName, "[") && strings.HasSuffix(lv.LVName, "]") {
			continue
		}

		lvKey := m.node + ":" + lv.VGName + ":" + lv.LVName

		if isThinPool(lv) {
			// Process thin pools
			currentThinPools[lvKey] = true

			// Update size metric
			sizeBytes := float64(lv.LVSize.Value())
			m.LVMThinPoolSizeBytes(lv.VGName, lv.LVName).Set(sizeBytes)

			// Calculate and update used bytes and percent
			var usedBytes float64
			var usedPercent float64
			var metadataUsedPercent float64

			if lv.DataPercent != "" {
				dataPercent, err := strconv.ParseFloat(lv.DataPercent, 64)
				if err != nil {
					errs = append(errs, fmt.Errorf("failed to parse DataPercent %q for thin pool %s/%s: %w",
						lv.DataPercent, lv.VGName, lv.LVName, err))
				} else {
					usedPercent = dataPercent
					usedBytes = sizeBytes * dataPercent / 100.0
				}
			}

			if lv.MetadataPercent != "" {
				metadataPercent, err := strconv.ParseFloat(lv.MetadataPercent, 64)
				if err != nil {
					errs = append(errs, fmt.Errorf("failed to parse MetadataPercent %q for thin pool %s/%s: %w",
						lv.MetadataPercent, lv.VGName, lv.LVName, err))
				} else {
					metadataUsedPercent = metadataPercent
				}
			}

			m.LVMThinPoolUsedBytes(lv.VGName, lv.LVName).Set(usedBytes)
			m.LVMThinPoolUsedPercent(lv.VGName, lv.LVName).Set(usedPercent)
			m.LVMThinPoolMetadataUsedPercent(lv.VGName, lv.LVName).Set(metadataUsedPercent)
		} else {
			// Process regular logical volumes (both thick and thin)
			currentLVs[lvKey] = true

			// Update size metric
			sizeBytes := float64(lv.LVSize.Value())
			m.LVMLogicalVolumeSizeBytes(lv.VGName, lv.LVName).Set(sizeBytes)

			// Calculate and update used bytes and percent
			var usedBytes float64
			var usedPercent float64

			if lv.DataPercent != "" {
				// Thin volume - has DataPercent
				dataPercent, err := strconv.ParseFloat(lv.DataPercent, 64)
				if err != nil {
					errs = append(errs, fmt.Errorf("failed to parse DataPercent %q for LV %s/%s: %w",
						lv.DataPercent, lv.VGName, lv.LVName, err))
					// Fallback to thick volume behavior on parse error
					usedBytes = sizeBytes
					usedPercent = 100.0
				} else {
					usedPercent = dataPercent
					usedBytes = sizeBytes * dataPercent / 100.0
				}
			} else {
				// Thick volume - 100% of allocated size is used
				usedBytes = sizeBytes
				usedPercent = 100.0
			}

			m.LVMLogicalVolumeUsedBytes(lv.VGName, lv.LVName).Set(usedBytes)
			m.LVMLogicalVolumeUsedPercent(lv.VGName, lv.LVName).Set(usedPercent)
		}
	}

	// Remove stale thin pool metrics
	for key := range m.previousThinPools {
		if !currentThinPools[key] {
			parts := strings.SplitN(key, ":", 3)
			if len(parts) == 3 {
				lvmThinPoolSizeBytes.DeleteLabelValues(parts[0], parts[1], parts[2])
				lvmThinPoolUsedBytes.DeleteLabelValues(parts[0], parts[1], parts[2])
				lvmThinPoolUsedPercent.DeleteLabelValues(parts[0], parts[1], parts[2])
				lvmThinPoolMetadataUsedPercent.DeleteLabelValues(parts[0], parts[1], parts[2])
			}
		}
	}
	m.previousThinPools = currentThinPools

	// Remove stale LV metrics
	for key := range m.previousLVs {
		if !currentLVs[key] {
			parts := strings.SplitN(key, ":", 3)
			if len(parts) == 3 {
				lvmLogicalVolumeSizeBytes.DeleteLabelValues(parts[0], parts[1], parts[2])
				lvmLogicalVolumeUsedBytes.DeleteLabelValues(parts[0], parts[1], parts[2])
				lvmLogicalVolumeUsedPercent.DeleteLabelValues(parts[0], parts[1], parts[2])
			}
		}
	}
	m.previousLVs = currentLVs

	return errs
}

// UpdateLVGStatusMetrics updates metrics based on LVMVolumeGroup resource status.
// This includes VG size/free and thin pool actual/allocated sizes from the LVG status.
// Returns collected parsing errors that should be logged by the caller.
func (m *Metrics) UpdateLVGStatusMetrics(lvgs map[string]v1alpha1.LVMVolumeGroup) []error {
	var errs []error

	currentLVGVGs := make(map[string]bool)
	currentLVGTPs := make(map[string]bool)

	for _, lvg := range lvgs {
		vgName := lvg.Spec.ActualVGNameOnTheNode

		// Skip LVGs that don't have VG created yet
		if vgName == "" {
			continue
		}

		vgKey := m.node + ":" + lvg.Name + ":" + vgName
		currentLVGVGs[vgKey] = true

		// Update VG metrics from LVG status
		m.LVGVGSizeBytes(lvg.Name, vgName).Set(float64(lvg.Status.VGSize.Value()))
		m.LVGVGFreeBytes(lvg.Name, vgName).Set(float64(lvg.Status.VGFree.Value()))

		// Update thin pool metrics from LVG status
		for _, tp := range lvg.Status.ThinPools {
			tpKey := m.node + ":" + lvg.Name + ":" + vgName + ":" + tp.Name
			currentLVGTPs[tpKey] = true

			actualSize := float64(tp.ActualSize.Value())
			m.LVGThinPoolActualSizeBytes(lvg.Name, vgName, tp.Name).Set(actualSize)
			m.LVGThinPoolAllocatedSizeBytes(lvg.Name, vgName, tp.Name).Set(float64(tp.AllocatedSize.Value()))
			m.LVGThinPoolUsedSizeBytes(lvg.Name, vgName, tp.Name).Set(float64(tp.UsedSize.Value()))

			// Calculate allocation limit in bytes: actualSize * allocationLimit / 100
			// AllocationLimit is stored as "150%" string, default is 150%
			allocationLimitPercent := 150.0 // default value
			if tp.AllocationLimit != "" {
				limitStr := strings.TrimSuffix(tp.AllocationLimit, "%")
				parsed, err := strconv.ParseFloat(limitStr, 64)
				if err != nil {
					errs = append(errs, fmt.Errorf("failed to parse AllocationLimit %q for thin pool %s in LVG %s: %w",
						tp.AllocationLimit, tp.Name, lvg.Name, err))
					// Keep default value of 150%
				} else {
					allocationLimitPercent = parsed
				}
			}
			allocationLimitBytes := actualSize * allocationLimitPercent / 100.0
			m.LVGThinPoolAllocationLimitBytes(lvg.Name, vgName, tp.Name).Set(allocationLimitBytes)
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Remove stale LVG VG metrics
	for key := range m.previousLVGVGs {
		if !currentLVGVGs[key] {
			parts := strings.SplitN(key, ":", 3)
			if len(parts) == 3 {
				lvgVGSizeBytes.DeleteLabelValues(parts[0], parts[1], parts[2])
				lvgVGFreeBytes.DeleteLabelValues(parts[0], parts[1], parts[2])
			}
		}
	}
	m.previousLVGVGs = currentLVGVGs

	// Remove stale LVG thin pool metrics
	for key := range m.previousLVGTPs {
		if !currentLVGTPs[key] {
			parts := strings.SplitN(key, ":", 4)
			if len(parts) == 4 {
				lvgThinPoolActualSizeBytes.DeleteLabelValues(parts[0], parts[1], parts[2], parts[3])
				lvgThinPoolAllocatedSizeBytes.DeleteLabelValues(parts[0], parts[1], parts[2], parts[3])
				lvgThinPoolUsedSizeBytes.DeleteLabelValues(parts[0], parts[1], parts[2], parts[3])
				lvgThinPoolAllocationLimitBytes.DeleteLabelValues(parts[0], parts[1], parts[2], parts[3])
			}
		}
	}
	m.previousLVGTPs = currentLVGTPs

	return errs
}
