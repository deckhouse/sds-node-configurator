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
	"path/filepath"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
)

// fileDeviceNameSeparator separates the LVMVolumeGroup name from the
// spec.fileDevices entry name inside a managed backing-file basename.
//
// A dot is what makes the basename parseable. An LVMVolumeGroup name is a
// DNS-1123 subdomain and may contain both '-' and '.', while an entry name is
// restricted by the CRD to a DNS-1123 label, which may contain '-' but never a
// '.'. So the last dot before the suffix always separates the two, whatever the
// LVG name looks like.
const fileDeviceNameSeparator = "."

// maxFileDeviceBasenameLen is NAME_MAX: the kernel limit on a single path
// component. Kubernetes accepts resource names of up to 253 characters and an
// entry name of up to 63, whose sum can exceed it, so the composed basename is
// checked rather than assumed to fit.
const maxFileDeviceBasenameLen = 255

// IsWithinBaseDir reports whether dir is base or a descendant of it. Both are
// cleaned before comparison so trailing slashes and redundant separators do
// not matter. dir is expected to be absolute and already free of '..'
// segments (validateFileDevice enforces that first), so no '..' can walk out
// of base.
//
// The comparison is lexical only: it does not resolve symlinks, so a symlinked
// component INSIDE base still leads wherever it points. That is accepted rather
// than fixed, because the threat this guard is for is a mistyped `directory`
// filling up an unintended filesystem, and planting such a symlink already
// requires write access to base on the node.
//
// It lives here rather than next to its main caller because all four paths that
// touch a spec-supplied directory need it, and one of them is the startup
// reattach in cmd/main.go, which cannot import the lvg controller package.
func IsWithinBaseDir(dir, base string) bool {
	base = filepath.Clean(base)
	dir = filepath.Clean(dir)
	return dir == base || strings.HasPrefix(dir, base+string(filepath.Separator))
}

// BuildFileDevicePath returns the deterministic absolute path the agent
// uses for the backing file of a single spec.fileDevices entry.
//
// The basename is
// `<FileDevicePrefix><lvgName>.<entryName><FileDeviceImageSuffix>`. It is
// determined by the entry's identity — not its position in the slice, and
// deliberately not its size — so reordering entries never renames a file, and a
// future in-place resize (fallocate + losetup -c + pvresize) can change `size`
// without the file moving out from under the loop device.
//
// Both names are embedded verbatim rather than hashed so that the mapping is
// invertible: given only the files on a node, ParseFileDevicePath recovers
// which LVMVolumeGroup and which entry each one belongs to. That is what lets
// a file-backed Volume Group be re-imported after its LVMVolumeGroup resource
// is lost, and it makes the files self-describing when debugging on the node.
//
// Together with IsManagedFileDevicePath this is the single owner marker
// the agent relies on: the discoverer never claims a loop PV as a
// managed file device unless its backing file's basename matches this
// pattern with the same lvgName.
func BuildFileDevicePath(directory, lvgName, entryName string) string {
	basename := internal.FileDevicePrefix + lvgName + fileDeviceNameSeparator + entryName + internal.FileDeviceImageSuffix
	return filepath.Join(directory, basename)
}

// FileDeviceBasenameTooLong reports whether the backing file for this entry
// would overflow NAME_MAX on the node. The apiserver cannot catch this: it is
// the sum of two separately valid lengths, and only the agent knows the naming
// scheme. Without the check the failure surfaces as a bare ENAMETOOLONG from
// fallocate.
func FileDeviceBasenameTooLong(lvgName, entryName string) bool {
	return len(internal.FileDevicePrefix)+len(lvgName)+len(fileDeviceNameSeparator)+
		len(entryName)+len(internal.FileDeviceImageSuffix) > maxFileDeviceBasenameLen
}

// ParseFileDevicePath splits a managed backing-file path into the
// LVMVolumeGroup name and the spec.fileDevices entry name encoded in it.
// ok is false for any path that does not match the managed pattern.
func ParseFileDevicePath(path string) (lvgName, entryName string, ok bool) {
	if path == "" {
		return "", "", false
	}

	base := filepath.Base(path)
	if !strings.HasPrefix(base, internal.FileDevicePrefix) || !strings.HasSuffix(base, internal.FileDeviceImageSuffix) {
		return "", "", false
	}

	middle := strings.TrimSuffix(strings.TrimPrefix(base, internal.FileDevicePrefix), internal.FileDeviceImageSuffix)
	sepIdx := strings.LastIndex(middle, fileDeviceNameSeparator)
	if sepIdx <= 0 || sepIdx >= len(middle)-1 {
		return "", "", false
	}

	lvgName, entryName = middle[:sepIdx], middle[sepIdx+1:]
	// Mirrors the CRD pattern on spec.fileDevices[].name. Applying it while
	// parsing keeps an unrelated file that merely happens to start with `sds-`
	// and end with `.img` from being read as a managed device. The check comes
	// from apimachinery rather than being spelled out here so it cannot drift
	// away from the pattern it is supposed to mirror.
	if len(validation.IsDNS1123Label(entryName)) > 0 {
		return "", "", false
	}

	return lvgName, entryName, true
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
	parsedLVG, _, ok := ParseFileDevicePath(path)
	if !ok {
		return false
	}
	if lvgName == "" {
		return parsedLVG != ""
	}
	return parsedLVG == lvgName
}
