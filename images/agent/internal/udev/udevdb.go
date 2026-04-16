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
	"fmt"
	"os"
	"strconv"
	"strings"
)

const DefaultRunUdevDataPath = "/run/udev/data"

// ReadUdevDB reads /run/udev/data/b<major>:<minor> and returns the E: properties
// as a key-value map. Non-E: lines (N:, S:, I:, etc.) are ignored.
func ReadUdevDB(basePath string, major, minor int) (map[string]string, error) {
	path := fmt.Sprintf("%s/b%d:%d", basePath, major, minor)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading udev db %s: %w", path, err)
	}
	props := make(map[string]string)
	for _, line := range strings.Split(string(data), "\n") {
		after, ok := strings.CutPrefix(strings.TrimSpace(line), "E:")
		if !ok {
			continue
		}
		if idx := strings.Index(after, "="); idx > 0 {
			props[after[:idx]] = after[idx+1:]
		}
	}
	return props, nil
}

// EnrichWithUdevDB merges the udev database properties into env. Event keys
// win over DB keys. Returns the enriched map and a nil error on success.
// On any issue (missing MAJOR/MINOR, unreadable DB file) the original env
// is returned along with an explanatory error.
func EnrichWithUdevDB(basePath string, env map[string]string) (map[string]string, error) {
	majorStr, hasMajor := env["MAJOR"]
	minorStr, hasMinor := env["MINOR"]
	if !hasMajor || !hasMinor {
		return env, fmt.Errorf("udev db merge skipped: MAJOR/MINOR missing")
	}
	major, err := strconv.Atoi(majorStr)
	if err != nil {
		return env, fmt.Errorf("udev db merge skipped: invalid MAJOR %q: %w", majorStr, err)
	}
	minor, err := strconv.Atoi(minorStr)
	if err != nil {
		return env, fmt.Errorf("udev db merge skipped: invalid MINOR %q: %w", minorStr, err)
	}
	dbProps, err := ReadUdevDB(basePath, major, minor)
	if err != nil {
		return env, fmt.Errorf("udev db merge failed for %d:%d: %w", major, minor, err)
	}
	merged := make(map[string]string, len(env)+len(dbProps))
	for k, v := range dbProps {
		merged[k] = v
	}
	for k, v := range env {
		merged[k] = v
	}
	return merged, nil
}
