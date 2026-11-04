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

package utils

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
)

// ProcHostMountInfo is the mountinfo of PID 1 (the host's init process).
// The agent runs with hostPID: true (see templates/agent/daemonset.yaml), so PID 1
// in the pod's PID namespace is the host's init. /proc/1/mountinfo therefore
// exposes the host's mount namespace (see proc_pid_mountinfo(5)).
// This is equivalent to `nsenter -t 1 -m -- cat /proc/self/mountinfo` (the same
// PID 1 reference used by all LVM commands, see nsentrerExpendedArgs) but avoids
// spawning a separate process.
const ProcHostMountInfo = "/proc/1/mountinfo"

// ParseMountInfo parses a mountinfo file (see proc(5)) into a map of "major:minor" → mount-point path.
//
// mountinfo format (see proc(5), one line per mount):
//
//	mount_id parent_id major:minor root mount_point mount_options optional_fields - fs_type source super_options
//
// When a device appears multiple times (e.g. bind mounts), the last mount point wins,
// matching lsblk MOUNTPOINT semantics ("usually the last mounted instance").
//
// Mount-point paths are decoded from kernel octal escapes (\NNN) so the returned
// strings match real filesystem paths (parity with lsblk output).
func ParseMountInfo(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("[ParseMountInfo] opening mountinfo: %w", err)
	}
	defer f.Close()

	result := make(map[string]string, 64)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 5 {
			continue
		}

		devID := fields[2]
		if !strings.Contains(devID, ":") {
			continue
		}

		mountPoint := decodeMountInfoOctal(fields[4])

		result[devID] = mountPoint
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("[ParseMountInfo] scanning mountinfo: %w", err)
	}
	return result, nil
}

// lsblkSwapMountPoint is what lsblk reports as the mount point of an active swap
// device. lsblk reads it from /proc/swaps, which is not per mount namespace, so
// in the agent container it already describes the host.
const lsblkSwapMountPoint = "[SWAP]"

// ApplyHostMountPoints replaces the mount points lsblk reported with the host's.
//
// lsblk runs in the agent container and reads the container's own mountinfo: it
// misses every host mount and reports the container's bind mounts (/etc/hosts and
// the like) instead. A disk mounted on the host whose filesystem signature is gone
// from the udev database then looked unused and was made consumable.
//
// mounts is ParseMountInfo of the host's mountinfo, the same map netlink discovery
// takes its mount points from; a device it does not name is not mounted on the
// host. An active swap device keeps lsblk's [SWAP]: mountinfo does not list swap.
//
// A device without a device number cannot be looked up, and taking it for
// unmounted is the wrong way to fail, so it fails the call.
func ApplyHostMountPoints(devices []internal.Device, mounts map[string]string) error {
	for i := range devices {
		device := &devices[i]
		if device.MajMin == "" {
			return fmt.Errorf("[ApplyHostMountPoints] lsblk reported no MAJ:MIN for %s", device.Name)
		}
		if mountPoint, ok := mounts[device.MajMin]; ok {
			device.MountPoint = mountPoint
			continue
		}
		if device.MountPoint != lsblkSwapMountPoint {
			device.MountPoint = ""
		}
	}
	return nil
}

// decodeMountInfoOctal decodes octal escape sequences (\NNN) used by the kernel
// in /proc/*/mountinfo paths (see mangle_path in fs/seq_file.c).
// Currently the kernel escapes space (\040), tab (\011), newline (\012),
// and backslash (\134), but this decoder handles any valid 3-digit octal sequence.
func decodeMountInfoOctal(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) {
			o1, o2, o3 := s[i+1]-'0', s[i+2]-'0', s[i+3]-'0'
			if o1 <= 7 && o2 <= 7 && o3 <= 7 {
				b.WriteByte(o1*64 + o2*8 + o3)
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}
