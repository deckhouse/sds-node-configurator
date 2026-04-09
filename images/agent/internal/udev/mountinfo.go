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

// ParseMountInfo parses mountinfo (default PID 1 under hostPID) into major:minor -> mount point;
// first mount per device wins.
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
