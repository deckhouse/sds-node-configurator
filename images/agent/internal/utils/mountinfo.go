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
)

const ProcHostMountInfo = "/proc/1/mountinfo"

// ParseMountInfo parses a mountinfo file (see proc(5)) into a map of "major:minor" → mount-point path.
//
// mountinfo format (see proc(5), one line per mount):
//
//	mount_id parent_id major:minor root mount_point mount_options optional_fields - fs_type source super_options
//
// When a device appears multiple times (e.g. bind mounts), the last mount point wins,
// matching lsblk MOUNTPOINT semantics ("usually the last mounted instance").
// For the current use case (determining whether a device is mounted at all)
// the specific path does not matter — any non-empty value is sufficient.
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
