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

package udev

import (
	"strings"
)

// Resolver transforms parsed Properties + sysfs attributes into the fields
// needed by internal.Device (type, name, parent, etc.). It is stateless and
// safe for concurrent use.
type Resolver struct {
	sysfsProvider *SysFSDataProvider
}

func NewResolver(sysfsProvider *SysFSDataProvider) *Resolver {
	return &Resolver{
		sysfsProvider: sysfsProvider,
	}
}

// DeviceType returns the lsblk-style TYPE (util-linux misc-utils/lsblk.c get_type()).
// Raw udev DEVTYPE is not enough: order is partition, device-mapper, loop, md,
// then SCSI device/type or "disk".
func (r *Resolver) DeviceType(props Properties, devName string) string {
	bare := r.sysfsProvider.SysfsDevName(devName)
	if bare == "" {
		bare = r.sysfsProvider.SysfsDevName(props.DevName)
	}

	if props.DevType == "partition" {
		return "part"
	}
	if props.DevType == "" && bare != "" && r.sysfsProvider.IsPartition(bare) {
		return "part"
	}

	if strings.HasPrefix(bare, "dm-") {
		return r.dmTypeFromDMUUID(props.DMUUID)
	}

	if strings.HasPrefix(bare, "loop") {
		return "loop"
	}

	if strings.HasPrefix(bare, "md") {
		if lvl := strings.TrimSpace(props.MDLevel); lvl != "" {
			return strings.ToLower(lvl)
		}
		return "md"
	}

	if name, ok := r.sysfsProvider.ReadScsiTypeFromSysfs(bare); ok {
		return name
	}
	if strings.HasPrefix(bare, "sr") {
		return "rom"
	}
	return "disk"
}

// DeviceName returns the user-facing path: /dev/mapper/<DM_NAME> for
// device-mapper devices, /dev/<DEVNAME> otherwise.
func (r *Resolver) DeviceName(props Properties) string {
	if props.DMName != "" {
		return "/dev/mapper/" + props.DMName
	}
	return ensureDevPrefix(props.DevName)
}

// KernelName returns the kernel device path with /dev/ prefix.
func (r *Resolver) KernelName(devName string) string {
	return ensureDevPrefix(devName)
}

// ParentDevice returns PkName: sysfs parent for partitions (checked first);
// for dm-* / md* the first slave. Partition-first avoids md0p1 -> wrong path.
func (r *Resolver) ParentDevice(devName string) string {
	devName = r.sysfsProvider.SysfsDevName(devName)
	if r.sysfsProvider.IsPartition(devName) {
		if parent := r.sysfsProvider.ParentFromSysfs(devName); parent != "" {
			return "/dev/" + parent
		}
	}
	if strings.HasPrefix(devName, "dm-") || strings.HasPrefix(devName, "md") {
		slaves, err := r.sysfsProvider.ReadSysfsSlaves(devName)
		if err == nil && len(slaves) > 0 {
			return "/dev/" + slaves[0]
		}
	}
	return ""
}

// dmTypeFromDMUUID derives TYPE from dm uuid prefix (same source as sysfs dm/uuid, udev DM_UUID):
// substring before first '-', lowercased; kpartx uses a "part*" prefix trimmed to "part" (lsblk get_type).
func (r *Resolver) dmTypeFromDMUUID(dmUUID string) string {
	if dmUUID == "" {
		return "dm"
	}
	prefix := dmUUID
	if i := strings.Index(dmUUID, "-"); i >= 0 {
		prefix = dmUUID[:i]
	}
	if prefix == "" {
		return "dm"
	}
	if len(prefix) >= 4 && strings.EqualFold(prefix[:4], "part") {
		prefix = "part"
	}
	return strings.ToLower(prefix)
}
