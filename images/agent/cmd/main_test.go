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

package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

const (
	reattachNode    = "node-0"
	reattachBaseDir = "/opt/deckhouse/sds/file-devices"
)

func reattachLVG(name, nodeName string, entries ...v1alpha1.LVMVolumeGroupFileDeviceSpec) v1alpha1.LVMVolumeGroup {
	return v1alpha1.LVMVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1alpha1.LVMVolumeGroupSpec{
			ActualVGNameOnTheNode: "vg-" + name,
			Local:                 v1alpha1.LVMVolumeGroupLocalSpec{NodeName: nodeName},
			FileDevices:           entries,
		},
	}
}

func reattachEntry(name, dir string) v1alpha1.LVMVolumeGroupFileDeviceSpec {
	return v1alpha1.LVMVolumeGroupFileDeviceSpec{
		Name:      name,
		Directory: dir,
		Size:      resource.MustParse("1Gi"),
	}
}

// paths flattens the collector's result so a test can assert on what losetup would
// actually be pointed at.
func paths(items []utils.LVGWithFileDevices) []string {
	var out []string
	for _, item := range items {
		for _, fd := range item.FileDevices {
			out = append(out, fd.FilePath)
		}
	}
	return out
}

// A spec-derived path outside the configured base directory must never reach
// losetup.
//
// The apiserver's pattern on spec.fileDevices[].directory rejects a malformed path
// but not one that is merely somewhere else, and this collector is the only
// file-device path whose input is not already known to be ours: the status-derived
// paths were written by the discoverer, which records a backing file only when it
// matched the managed naming pattern. Without the check here, an LVMVolumeGroup
// naming /etc would have the agent run `losetup --find` against
// /etc/sds-<lvg>.<entry>.img at every startup — attaching an arbitrary host file as
// a block device, and, since spec.fileDevices removed `loop` from LVMGlobalFilter,
// exposing whatever LVM metadata it carries to lvm.static.
func TestCollectFileDevicesToReattach_ConfinesSpecPathsToTheBaseDirectory(t *testing.T) {
	log := logger.Logger{}

	t.Run("an entry inside the base directory is collected", func(t *testing.T) {
		lvgs := []v1alpha1.LVMVolumeGroup{
			reattachLVG("lvg-a", reattachNode, reattachEntry("d1", reattachBaseDir)),
		}
		got := collectFileDevicesToReattach(log, lvgs, reattachNode, reattachBaseDir)
		assert.Equal(t, []string{reattachBaseDir + "/sds-lvg-a.d1.img"}, paths(got))
	})

	t.Run("a subdirectory of the base directory is collected", func(t *testing.T) {
		lvgs := []v1alpha1.LVMVolumeGroup{
			reattachLVG("lvg-a", reattachNode, reattachEntry("d1", reattachBaseDir+"/pool-1")),
		}
		got := collectFileDevicesToReattach(log, lvgs, reattachNode, reattachBaseDir)
		assert.Equal(t, []string{reattachBaseDir + "/pool-1/sds-lvg-a.d1.img"}, paths(got))
	})

	t.Run("an entry outside the base directory is skipped", func(t *testing.T) {
		for _, dir := range []string{"/etc", "/var/lib/kubelet", "/", "/opt/deckhouse/sds/file-devices-elsewhere"} {
			lvgs := []v1alpha1.LVMVolumeGroup{
				reattachLVG("lvg-a", reattachNode, reattachEntry("d1", dir)),
			}
			got := collectFileDevicesToReattach(log, lvgs, reattachNode, reattachBaseDir)
			assert.Empty(t, paths(got), "directory %q must not be reattached", dir)
		}
	})

	t.Run("a '..' escape is skipped", func(t *testing.T) {
		// validateFileDevice rejects '..' before provisioning, but nothing rejects
		// it before this collector runs. filepath.Clean resolves it, so the check
		// sees the path it would really open.
		lvgs := []v1alpha1.LVMVolumeGroup{
			reattachLVG("lvg-a", reattachNode, reattachEntry("d1", reattachBaseDir+"/../../../etc")),
		}
		got := collectFileDevicesToReattach(log, lvgs, reattachNode, reattachBaseDir)
		assert.Empty(t, paths(got))
	})

	t.Run("a bad entry does not take the good ones down with it", func(t *testing.T) {
		lvgs := []v1alpha1.LVMVolumeGroup{
			reattachLVG("lvg-a", reattachNode,
				reattachEntry("bad", "/etc"),
				reattachEntry("good", reattachBaseDir),
			),
		}
		got := collectFileDevicesToReattach(log, lvgs, reattachNode, reattachBaseDir)
		assert.Equal(t, []string{reattachBaseDir + "/sds-lvg-a.good.img"}, paths(got))
	})

	// An empty base directory is what the unit tests of the reconciler use and what
	// a zero-value config would carry; it must not silently start filtering.
	t.Run("an empty base directory disables the check", func(t *testing.T) {
		lvgs := []v1alpha1.LVMVolumeGroup{
			reattachLVG("lvg-a", reattachNode, reattachEntry("d1", "/somewhere/else")),
		}
		got := collectFileDevicesToReattach(log, lvgs, reattachNode, "")
		assert.Equal(t, []string{"/somewhere/else/sds-lvg-a.d1.img"}, paths(got))
	})
}

// Status-derived paths bypass the base-directory check on purpose: the discoverer
// only records a backing file whose basename matched the managed pattern, so such a
// path is one this agent created — and refusing it would leave a loop device
// unattached after a reboot for a Volume Group that is perfectly healthy, merely
// configured before the base directory was narrowed.
func TestCollectFileDevicesToReattach_KeepsStatusPaths(t *testing.T) {
	lvg := reattachLVG("lvg-a", reattachNode)
	lvg.Status.Nodes = []v1alpha1.LVMVolumeGroupNode{{
		Name: reattachNode,
		FileDevices: []v1alpha1.LVMVolumeGroupFileDevice{{
			Name:       "d1",
			FilePath:   "/mnt/data/sds-lvg-a.d1.img",
			LoopDevice: "/dev/loop0",
		}},
	}}

	got := collectFileDevicesToReattach(logger.Logger{}, []v1alpha1.LVMVolumeGroup{lvg}, reattachNode, reattachBaseDir)
	assert.Equal(t, []string{"/mnt/data/sds-lvg-a.d1.img"}, paths(got))
}

// Another node's LVMVolumeGroups are not this agent's to reattach, and a spec entry
// already covered by a status entry must not be listed twice — losetup --nooverlap
// would cope, but the duplicate spends a host command per startup for nothing.
func TestCollectFileDevicesToReattach_FiltersByNodeAndDeduplicates(t *testing.T) {
	sameFile := reattachBaseDir + "/sds-lvg-a.d1.img"

	local := reattachLVG("lvg-a", reattachNode, reattachEntry("d1", reattachBaseDir))
	local.Status.Nodes = []v1alpha1.LVMVolumeGroupNode{{
		Name:        reattachNode,
		FileDevices: []v1alpha1.LVMVolumeGroupFileDevice{{Name: "d1", FilePath: sameFile, LoopDevice: "/dev/loop0"}},
	}}
	foreign := reattachLVG("lvg-b", "node-1", reattachEntry("d1", reattachBaseDir))

	got := collectFileDevicesToReattach(logger.Logger{}, []v1alpha1.LVMVolumeGroup{local, foreign}, reattachNode, reattachBaseDir)

	assert.Equal(t, []string{sameFile}, paths(got))
	if assert.Len(t, got, 1) {
		assert.Equal(t, "lvg-a", got[0].LVGName)
		assert.Equal(t, "/dev/loop0", got[0].FileDevices[0].LoopDevice,
			"the status entry carries the recorded loop and must win over the bare spec path")
	}
}
