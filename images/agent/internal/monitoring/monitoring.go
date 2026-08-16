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

	lvmActivationTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "lvm_activation_total",
		Help:      "Total number of LVM VG activation attempts.",
	}, []string{"node", "volume_group", "result"})

	// File-backed device metrics. Backing files are preallocated, so the module
	// holds a fixed share of a filesystem it does not own: the two gauges below
	// are what makes that share, and what is left of the filesystem, visible.
	// Without them the only protection against filling a node is the free-space
	// check the agent runs once, at provisioning time.
	fileDeviceSizeBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "file_device_size_bytes",
		Help:      "Size of the physical volume created on a spec.fileDevices backing file, in bytes.",
	}, []string{"node", "lvg_name", "volume_group", "file_device"})

	fileDevicesDirectoryFreeBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "file_devices_directory_free_bytes",
		Help:      "Free space on the filesystem holding spec.fileDevices backing files, in bytes.",
	}, []string{"node", "directory"})

	fileDevicesDirectoryAllocatedBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "file_devices_directory_allocated_bytes",
		Help:      "Total size of the backing files this module has allocated in the directory, in bytes.",
	}, []string{"node", "directory"})

	// Published alongside the free figure so the share that is left can be
	// expressed without knowing anything about the node. The agent refuses to
	// allocate below a configured share of the filesystem, and kubelet evicts
	// below a share of it too — an absolute number of free bytes cannot be
	// compared against either.
	fileDevicesDirectoryTotalBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "file_devices_directory_total_bytes",
		Help:      "Size of the filesystem holding spec.fileDevices backing files, in bytes.",
	}, []string{"node", "directory"})

	// A Volume Group the discoverer refuses to import leaves no other trace in the
	// API: there is no resource for it, and the LVMVolumeGroup whose name its tag
	// claims is healthy and must not be marked otherwise. Without this counter the
	// only record is a line in one node's agent log, so `kubectl get lvg` looks
	// clean while a Volume Group on the node is permanently unmanaged.
	//
	// A counter rather than a gauge because the state never resolves on its own:
	// a non-zero rate is "this is happening now" and falls back to zero by itself
	// once the tag is fixed, whereas a gauge set to 1 would need clearing by
	// whichever pass stops seeing the Volume Group.
	lvmVolumeGroupImportRefusedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "lvm_volume_group_import_refused_total",
		// Both reasons, because HELP is what an operator reads in /metrics and in
		// Grafana's completion — not the Go doc comment on the accessor. Naming only
		// the tag conflict sent whoever the alert woke looking for a duplicate
		// lvmVolumeGroupName that was not there.
		Help: "Number of times the discoverer started refusing to import a Volume Group: either its owner tag names an existing LVMVolumeGroup, or one of its Physical Volumes has no BlockDevice resource yet and a selector derived now would not cover the whole group. Counted when the refusal appears or changes, not once per discovery pass, so increase() over a window means new refusals rather than the age of a standing one.",
		// lvg_name is empty when the import had no name of its own to use — an
		// unimported Volume Group with no owner tag is given a freshly generated
		// name on every pass, and that is not something to put in a label.
	}, []string{"node", "volume_group", "lvg_name"})
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
	metrics.Registry.MustRegister(lvmActivationTotal)
	metrics.Registry.MustRegister(fileDeviceSizeBytes)
	metrics.Registry.MustRegister(fileDevicesDirectoryFreeBytes)
	metrics.Registry.MustRegister(fileDevicesDirectoryAllocatedBytes)
	metrics.Registry.MustRegister(fileDevicesDirectoryTotalBytes)
	metrics.Registry.MustRegister(lvmVolumeGroupImportRefusedTotal)
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
	previousFileDevs  map[string]bool
	previousFileDirs  map[string]bool
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
		previousFileDevs:  make(map[string]bool),
		previousFileDirs:  make(map[string]bool),
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

func (m *Metrics) LVMActivationTotal(volumeGroup, result string) prometheus.Counter {
	return lvmActivationTotal.WithLabelValues(m.node, volumeGroup, result)
}

// LVMVolumeGroupImportRefusedTotal counts a Volume Group the discoverer will not
// import, whichever of the two reasons it has: the name its owner tag claims is
// already taken by another LVMVolumeGroup, or one of its Physical Volumes has no
// BlockDevice yet and a selector derived now would not cover the whole group.
//
// lvgName is the name the import would have used — in the first case, the
// LVMVolumeGroup that already holds it — so an operator reading the metric has
// both ends of the conflict without going to the node's log.
//
// It must be a name somebody chose, and callers are responsible for that: a
// Volume Group with no owner tag is given a generated name, minted afresh on
// every discovery pass, and a label that changes every pass is a new time series
// every pass, retained for the lifetime of the process. Pass "" instead —
// LVMVolumeGroupCandidate.LVMVGNameGenerated says which case it is.
//
// Both are counted because both can be permanent: a tag on two Volume Groups is
// not going to resolve itself, and a Physical Volume excluded by a
// BlockDeviceFilter never becomes a BlockDevice. Counting one and not the other
// would make "the agent can see a Volume Group it will not adopt" alertable only
// half the time.
//
// And because both can be permanent, callers must increment when the refusal
// appears or changes, not on every pass that finds it still holding — the same
// rule, and the same condition, their log lines are gated on. The discoverer runs
// on every udev event, so a per-pass increment would keep increase(…[1h]) non-zero
// for as long as a deliberate BlockDeviceFilter exclusion stands, which is this
// counter's own alert firing on a decision somebody made on purpose.
func (m *Metrics) LVMVolumeGroupImportRefusedTotal(volumeGroup, lvgName string) prometheus.Counter {
	return lvmVolumeGroupImportRefusedTotal.WithLabelValues(m.node, volumeGroup, lvgName)
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

// FileDeviceDirectoryUsage is one directory's worth of backing-file accounting,
// as observed on the node.
type FileDeviceDirectoryUsage struct {
	Directory string
	// FreeBytes is what the filesystem holding the directory has left. Zero when
	// the agent could not read it this cycle; the caller reports the error and
	// the previous sample is left standing rather than replaced with a false 0.
	FreeBytes int64
	// TotalBytes is the size of that filesystem. Free alone cannot be compared
	// against either the agent's reserve or kubelet's eviction threshold, since
	// both are shares of the whole.
	TotalBytes int64
	Known      bool
	// AllocatedBytes is the total size of the backing files this module put there.
	AllocatedBytes int64
}

// UpdateFileDeviceMetrics publishes per-file-device sizes and, per directory,
// how much of the filesystem the module has taken and how much is left.
//
// Backing files are preallocated, so a file-backed Volume Group holds a fixed
// share of a filesystem the module does not own — usually the node's root. The
// agent refuses to create a file larger than the free space at provisioning
// time, but nothing watches that filesystem afterwards, and anything else on the
// node can fill it. That failure is node-level (kubelet DiskPressure eviction),
// so it needs to be visible before it happens rather than diagnosed after.
func (m *Metrics) UpdateFileDeviceMetrics(lvgs map[string]v1alpha1.LVMVolumeGroup, usage []FileDeviceDirectoryUsage) {
	currentDevs := make(map[string]bool)
	currentDirs := make(map[string]bool)

	for _, lvg := range lvgs {
		vgName := lvg.Spec.ActualVGNameOnTheNode
		if vgName == "" {
			continue
		}
		for _, node := range lvg.Status.Nodes {
			if node.Name != m.node {
				continue
			}
			for _, fd := range node.FileDevices {
				if fd.Name == "" {
					continue
				}
				currentDevs[m.node+":"+lvg.Name+":"+vgName+":"+fd.Name] = true
				fileDeviceSizeBytes.WithLabelValues(m.node, lvg.Name, vgName, fd.Name).
					Set(float64(fd.Size.Value()))
			}
		}
	}

	for _, u := range usage {
		currentDirs[m.node+":"+u.Directory] = true
		fileDevicesDirectoryAllocatedBytes.WithLabelValues(m.node, u.Directory).
			Set(float64(u.AllocatedBytes))
		if u.Known {
			fileDevicesDirectoryFreeBytes.WithLabelValues(m.node, u.Directory).
				Set(float64(u.FreeBytes))
			fileDevicesDirectoryTotalBytes.WithLabelValues(m.node, u.Directory).
				Set(float64(u.TotalBytes))
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for key := range m.previousFileDevs {
		if !currentDevs[key] {
			if parts := strings.SplitN(key, ":", 4); len(parts) == 4 {
				fileDeviceSizeBytes.DeleteLabelValues(parts[0], parts[1], parts[2], parts[3])
			}
		}
	}
	m.previousFileDevs = currentDevs

	for key := range m.previousFileDirs {
		if !currentDirs[key] {
			if parts := strings.SplitN(key, ":", 2); len(parts) == 2 {
				fileDevicesDirectoryFreeBytes.DeleteLabelValues(parts[0], parts[1])
				fileDevicesDirectoryTotalBytes.DeleteLabelValues(parts[0], parts[1])
				fileDevicesDirectoryAllocatedBytes.DeleteLabelValues(parts[0], parts[1])
			}
		}
	}
	m.previousFileDirs = currentDirs
}
