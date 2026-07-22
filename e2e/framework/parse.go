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

package framework

import (
	"strconv"
	"strings"
)

// LsblkLine is one device line from lsblk -b -P -o NAME,SIZE,SERIAL,PATH (keyed by PATH).
type LsblkLine struct {
	Path      string
	Serial    string
	Size      string
	SizeBytes int64
}

// ParseLsblk parses `lsblk -b -P -o NAME,SIZE,SERIAL,PATH` output (KEY="value"
// per line), keyed by PATH.
//
// TODO: split-by-space breaks on values containing spaces (e.g. disk model).
// Root fix = upstream lsblk parser in storage-e2e.
func ParseLsblk(out string) map[string]LsblkLine {
	result := make(map[string]LsblkLine)
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var path, serial, sizeStr string
		for _, part := range strings.Split(line, " ") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			if idx := strings.Index(part, "="); idx >= 0 && len(part) > idx+2 {
				k, v := part[:idx], strings.Trim(part[idx+1:], "\"")
				switch k {
				case "PATH":
					path = v
				case "SERIAL":
					serial = v
				case "SIZE":
					sizeStr = v
				}
			}
		}
		if path == "" {
			continue
		}
		var sizeBytes int64
		if sizeStr != "" {
			sizeBytes, _ = strconv.ParseInt(sizeStr, 10, 64)
		}
		result[path] = LsblkLine{Path: path, Serial: serial, Size: sizeStr, SizeBytes: sizeBytes}
	}
	return result
}
