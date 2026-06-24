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

type Resolver struct {
	sysfsProvider *SysFSDataProvider
}

func NewResolver(sysfsProvider *SysFSDataProvider) *Resolver {
	return &Resolver{
		sysfsProvider: sysfsProvider,
	}
}

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

func (r *Resolver) DeviceName(props Properties) string {
	if props.DMName != "" {
		return "/dev/mapper/" + props.DMName
	}
	return ensureDevPrefix(props.DevName)
}

func (r *Resolver) KernelName(devName string) string {
	return ensureDevPrefix(devName)
}

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
