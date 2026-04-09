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

package udev

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// ParseMountInfo reads /proc/1/mountinfo (the host init process) and returns a map
// of "major:minor" -> mount point. We read PID 1's mountinfo because the agent runs
// in a container with hostPID: true, and /proc/self/mountinfo would only show the
// container's mount namespace (overlayfs, tmpfs), not the actual host block device mounts.
// See https://www.kernel.org/doc/Documentation/filesystems/proc.txt (section 3.5)
// Format: 36 35 98:0 /mnt1 /mnt2 rw,noatime master:1 - ext3 /dev/root rw,errors=continue
// Fields: [0]=mount_id [1]=parent_id [2]=major:minor [3]=root [4]=mount_point ...
func ParseMountInfo() (map[string]string, error) {
	f, err := os.Open(procSelfMountInfo)
	if err != nil {
		return nil, fmt.Errorf("opening mountinfo: %w", err)
	}
	defer f.Close()

	result := make(map[string]string)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 5 {
			continue
		}
		devID := fields[2]
		mountPoint := fields[4]
		// Keep only the first mount point per device
		if _, exists := result[devID]; !exists {
			result[devID] = mountPoint
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scanning mountinfo: %w", err)
	}
	return result, nil
}
