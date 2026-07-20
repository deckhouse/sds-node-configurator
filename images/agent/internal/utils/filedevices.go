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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
)

// fileDeviceBasenameHashLen is the prefix length of the lower-case hex
// SHA-256 digest embedded in a managed backing-file basename. 12 hex
// chars carry 48 bits — collision-resistant enough to discriminate two
// fileDevices entries that share the same directory while staying short
// enough to fit comfortably into status fields.
const fileDeviceBasenameHashLen = 12

// BuildFileDevicePath returns the deterministic absolute path the agent
// uses for the backing file of a single spec.fileDevices entry.
//
// The basename is `<FileDevicePrefix><lvgName>-<hash><FileDeviceImageSuffix>`
// where <hash> is a truncated SHA-256 of (directory, size). The name is
// fully determined by the spec entry's contents — not its position in
// the slice — so reordering, deleting, or inserting entries in
// `spec.fileDevices` does not silently rename an existing file.
//
// Together with IsManagedFileDevicePath this is the single owner marker
// the agent relies on: the discoverer never claims a loop PV as a
// managed file device unless its backing file's basename matches this
// pattern with the same lvgName.
func BuildFileDevicePath(directory, lvgName string, size resource.Quantity) string {
	digest := sha256.Sum256(fmt.Appendf(nil, "%s|%d", directory, size.Value()))
	hash := hex.EncodeToString(digest[:])[:fileDeviceBasenameHashLen]
	basename := internal.FileDevicePrefix + lvgName + "-" + hash + internal.FileDeviceImageSuffix
	return filepath.Join(directory, basename)
}

// IsManagedFileDevicePath reports whether path looks like a backing file
// the agent itself created for the given lvgName. The check is purely
// structural (basename pattern) — it does not stat the file. It is used
// by the discoverer to gate "is this loop PV one of ours?" and by
// cleanup to refuse rm-ing unrelated paths even if status was somehow
// corrupted.
//
// An empty lvgName matches any LVG-owned file, which is the right
// behaviour during cluster-wide cleanup where the caller does not know
// the owning LVG name yet.
func IsManagedFileDevicePath(path, lvgName string) bool {
	if path == "" {
		return false
	}
	base := filepath.Base(path)
	if !strings.HasPrefix(base, internal.FileDevicePrefix) {
		return false
	}
	if !strings.HasSuffix(base, internal.FileDeviceImageSuffix) {
		return false
	}
	middle := strings.TrimSuffix(strings.TrimPrefix(base, internal.FileDevicePrefix), internal.FileDeviceImageSuffix)
	dashIdx := strings.LastIndex(middle, "-")
	if dashIdx <= 0 || dashIdx >= len(middle)-1 {
		return false
	}
	name := middle[:dashIdx]
	hash := middle[dashIdx+1:]
	if len(hash) != fileDeviceBasenameHashLen {
		return false
	}
	for _, c := range hash {
		switch {
		case c >= '0' && c <= '9':
		case c >= 'a' && c <= 'f':
		default:
			return false
		}
	}
	if lvgName == "" {
		return name != ""
	}
	return name == lvgName
}
