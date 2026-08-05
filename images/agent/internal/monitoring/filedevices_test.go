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

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
)

const testNode = "node-0"

func fileDeviceLVG(name, vgName string, devices ...v1alpha1.LVMVolumeGroupFileDevice) v1alpha1.LVMVolumeGroup {
	return v1alpha1.LVMVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1alpha1.LVMVolumeGroupSpec{
			ActualVGNameOnTheNode: vgName,
			FileDevices: []v1alpha1.LVMVolumeGroupFileDeviceSpec{
				{Name: "d1", Directory: "/opt/deckhouse/sds/file-devices", Size: resource.MustParse("10Gi")},
			},
		},
		Status: v1alpha1.LVMVolumeGroupStatus{
			Nodes: []v1alpha1.LVMVolumeGroupNode{{Name: testNode, FileDevices: devices}},
		},
	}
}

func TestUpdateFileDeviceMetrics(t *testing.T) {
	fileDeviceSizeBytes.Reset()
	fileDevicesDirectoryFreeBytes.Reset()
	fileDevicesDirectoryAllocatedBytes.Reset()

	m := GetMetrics(testNode)
	lvgs := map[string]v1alpha1.LVMVolumeGroup{
		"vg-a": fileDeviceLVG("lvg-a", "vg-a",
			v1alpha1.LVMVolumeGroupFileDevice{Name: "d1", Size: resource.MustParse("1069547520")}),
	}
	usage := []FileDeviceDirectoryUsage{{
		Directory:      "/opt/deckhouse/sds/file-devices",
		FreeBytes:      5 << 30,
		Known:          true,
		AllocatedBytes: 10 << 30,
	}}

	m.UpdateFileDeviceMetrics(lvgs, usage)

	assert.Equal(t, 1, testutil.CollectAndCount(fileDeviceSizeBytes))
	assert.Equal(t, float64(1069547520),
		testutil.ToFloat64(fileDeviceSizeBytes.WithLabelValues(testNode, "lvg-a", "vg-a", "d1")))
	assert.Equal(t, float64(5<<30),
		testutil.ToFloat64(fileDevicesDirectoryFreeBytes.WithLabelValues(testNode, "/opt/deckhouse/sds/file-devices")))
	assert.Equal(t, float64(10<<30),
		testutil.ToFloat64(fileDevicesDirectoryAllocatedBytes.WithLabelValues(testNode, "/opt/deckhouse/sds/file-devices")))
}

// A gauge left behind after its LVMVolumeGroup is gone reads as a Volume Group
// that still exists, which is worse than no metric at all.
func TestUpdateFileDeviceMetrics_DropsStaleSeries(t *testing.T) {
	fileDeviceSizeBytes.Reset()
	fileDevicesDirectoryFreeBytes.Reset()
	fileDevicesDirectoryAllocatedBytes.Reset()

	m := GetMetrics(testNode)
	dir := "/opt/deckhouse/sds/file-devices"

	m.UpdateFileDeviceMetrics(map[string]v1alpha1.LVMVolumeGroup{
		"vg-a": fileDeviceLVG("lvg-a", "vg-a",
			v1alpha1.LVMVolumeGroupFileDevice{Name: "d1", Size: resource.MustParse("1Gi")}),
		"vg-b": fileDeviceLVG("lvg-b", "vg-b",
			v1alpha1.LVMVolumeGroupFileDevice{Name: "d1", Size: resource.MustParse("1Gi")}),
	}, []FileDeviceDirectoryUsage{{Directory: dir, FreeBytes: 1 << 30, Known: true, AllocatedBytes: 2 << 30}})
	assert.Equal(t, 2, testutil.CollectAndCount(fileDeviceSizeBytes))

	// lvg-b is deleted, and with it the last user of the directory.
	m.UpdateFileDeviceMetrics(map[string]v1alpha1.LVMVolumeGroup{
		"vg-a": fileDeviceLVG("lvg-a", "vg-a",
			v1alpha1.LVMVolumeGroupFileDevice{Name: "d1", Size: resource.MustParse("1Gi")}),
	}, nil)

	assert.Equal(t, 1, testutil.CollectAndCount(fileDeviceSizeBytes), "lvg-b's series must be dropped")
	assert.Equal(t, 0, testutil.CollectAndCount(fileDevicesDirectoryFreeBytes))
	assert.Equal(t, 0, testutil.CollectAndCount(fileDevicesDirectoryAllocatedBytes))
}

// A failed free-space read must not be published as 0 — that is indistinguishable
// from a full filesystem and would fire exactly the alert it should not.
func TestUpdateFileDeviceMetrics_UnknownFreeSpaceKeepsPreviousSample(t *testing.T) {
	fileDevicesDirectoryFreeBytes.Reset()
	fileDevicesDirectoryAllocatedBytes.Reset()

	m := GetMetrics(testNode)
	dir := "/opt/deckhouse/sds/file-devices"
	lvgs := map[string]v1alpha1.LVMVolumeGroup{"vg-a": fileDeviceLVG("lvg-a", "vg-a")}

	m.UpdateFileDeviceMetrics(lvgs, []FileDeviceDirectoryUsage{
		{Directory: dir, FreeBytes: 7 << 30, Known: true, AllocatedBytes: 1 << 30},
	})
	m.UpdateFileDeviceMetrics(lvgs, []FileDeviceDirectoryUsage{
		{Directory: dir, Known: false, AllocatedBytes: 1 << 30},
	})

	assert.Equal(t, float64(7<<30),
		testutil.ToFloat64(fileDevicesDirectoryFreeBytes.WithLabelValues(testNode, dir)),
		"an unreadable sample must leave the last known value in place")
}

// Devices the discoverer could not attribute to a spec entry carry no name;
// labelling them "" would collapse them into one meaningless series.
func TestUpdateFileDeviceMetrics_SkipsNamelessDevices(t *testing.T) {
	fileDeviceSizeBytes.Reset()

	m := GetMetrics(testNode)
	m.UpdateFileDeviceMetrics(map[string]v1alpha1.LVMVolumeGroup{
		"vg-a": fileDeviceLVG("lvg-a", "vg-a",
			v1alpha1.LVMVolumeGroupFileDevice{Name: "", Size: resource.MustParse("1Gi")},
			v1alpha1.LVMVolumeGroupFileDevice{Name: "d1", Size: resource.MustParse("1Gi")}),
	}, nil)

	assert.Equal(t, 1, testutil.CollectAndCount(fileDeviceSizeBytes))
}
