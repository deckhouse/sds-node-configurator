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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testIQN       = "iqn.2000-01.com.example:target"
	testOtherIQN  = "iqn.2000-01.com.example:other-target"
	testWWPN      = "0x500a0981891b8dc5"
	testOtherWWPN = "0x500a0981891b8dc4"
)

// fakeSCSISysfs lays out the parts of sysfs SCSITargetOf reads, the way the
// kernel does: the block devices under /sys/devices, symlinked from
// /sys/class/block, and the session and remote port classes beside them.
type fakeSCSISysfs struct {
	t    *testing.T
	root string
}

func withFakeSCSISysfs(t *testing.T) *fakeSCSISysfs {
	t.Helper()

	root := t.TempDir()
	saved := sysRootForSCSITargets
	sysRootForSCSITargets = root
	t.Cleanup(func() { sysRootForSCSITargets = saved })

	return &fakeSCSISysfs{t: t, root: root}
}

func (f *fakeSCSISysfs) mkdir(parts ...string) string {
	f.t.Helper()
	dir := filepath.Join(append([]string{f.root}, parts...)...)
	require.NoError(f.t, os.MkdirAll(dir, 0o755))
	return dir
}

func (f *fakeSCSISysfs) write(path, content string) {
	f.t.Helper()
	require.NoError(f.t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(f.t, os.WriteFile(path, []byte(content), 0o644))
}

func (f *fakeSCSISysfs) link(target, name string) {
	f.t.Helper()
	require.NoError(f.t, os.MkdirAll(filepath.Dir(name), 0o755))
	require.NoError(f.t, os.Symlink(target, name))
}

// disk places a SCSI disk under the transport directory given (a session or a
// remote port of host 3) and returns its directory under /sys/devices.
func (f *fakeSCSISysfs) disk(name, transport string) string {
	f.t.Helper()
	scsiDevice := f.mkdir("devices", "platform", "host3", transport, "target3:0:0", "3:0:0:1")
	blockDir := f.mkdir("devices", "platform", "host3", transport, "target3:0:0", "3:0:0:1", "block", name)
	f.link(scsiDevice, filepath.Join(blockDir, "device"))
	f.link(blockDir, filepath.Join(f.root, "class", "block", name))
	return blockDir
}

func (f *fakeSCSISysfs) iscsiDisk(name, session, iqn string) string {
	f.t.Helper()
	f.write(filepath.Join(f.root, "class", "iscsi_session", session, "targetname"), iqn+"\n")
	return f.disk(name, session)
}

func (f *fakeSCSISysfs) fcDisk(name, rport, wwpn string) {
	f.t.Helper()
	f.write(filepath.Join(f.root, "class", "fc_remote_ports", rport, "port_name"), wwpn+"\n")
	f.disk(name, rport)
}

func (f *fakeSCSISysfs) partition(diskDir, name string) {
	f.t.Helper()
	dir := filepath.Join(diskDir, name)
	f.write(filepath.Join(dir, "partition"), "1\n")
	f.link(dir, filepath.Join(f.root, "class", "block", name))
}

func (f *fakeSCSISysfs) mapping(name string, slaves ...string) {
	f.t.Helper()
	dir := f.mkdir("devices", "virtual", "block", name)
	f.mkdir("devices", "virtual", "block", name, "slaves")
	for _, slave := range slaves {
		f.link(filepath.Join(f.root, "class", "block", slave), filepath.Join(dir, "slaves", slave))
	}
	f.link(dir, filepath.Join(f.root, "class", "block", name))
}

func TestSCSITargetOf_ISCSI(t *testing.T) {
	f := withFakeSCSISysfs(t)
	f.iscsiDisk("sdb", "session1", testIQN)

	got, err := SCSITargetOf("/dev/sdb")
	require.NoError(t, err)
	assert.Equal(t, testIQN, got)
}

func TestSCSITargetOf_FibreChannel(t *testing.T) {
	f := withFakeSCSISysfs(t)
	f.fcDisk("sdc", "rport-3:0-2", "0x500A0981891B8DC5")

	got, err := SCSITargetOf("/dev/sdc")
	require.NoError(t, err)
	assert.Equal(t, testWWPN, got, "lower-cased, as the label is")
}

func TestSCSITargetOf_PartitionTakesItFromItsDisk(t *testing.T) {
	f := withFakeSCSISysfs(t)
	dir := f.iscsiDisk("sdb", "session1", testIQN)
	f.partition(dir, "sdb1")

	got, err := SCSITargetOf("/dev/sdb1")
	require.NoError(t, err)
	assert.Equal(t, testIQN, got)
}

func TestSCSITargetOf_MultipathMapTakesTheSmallestOfItsPaths(t *testing.T) {
	f := withFakeSCSISysfs(t)
	f.fcDisk("sdd", "rport-3:0-2", testWWPN)
	f.fcDisk("sde", "rport-3:0-3", testOtherWWPN)
	f.mapping("dm-1", "sdd", "sde")
	// A partition of the map, and a crypt mapping over that partition.
	f.mapping("dm-2", "dm-1")
	f.mapping("dm-3", "dm-2")

	for _, name := range []string{"/dev/dm-1", "/dev/dm-2", "/dev/dm-3"} {
		got, err := SCSITargetOf(name)
		require.NoError(t, err, name)
		assert.Equal(t, testOtherWWPN, got, name)
	}
}

func TestSCSITargetOf_MultipathMapOverTwoISCSITargets(t *testing.T) {
	f := withFakeSCSISysfs(t)
	f.iscsiDisk("sdf", "session1", testIQN)
	f.iscsiDisk("sdg", "session2", testOtherIQN)
	f.mapping("dm-4", "sdf", "sdg")

	got, err := SCSITargetOf("/dev/dm-4")
	require.NoError(t, err)
	assert.Equal(t, testOtherIQN, got, "the smallest of the targets, whichever path the map lists first")
}

func TestSCSITargetOf_DeviceOfNoTarget(t *testing.T) {
	f := withFakeSCSISysfs(t)
	// A virtio disk: a device link, but no session or remote port above it.
	pci := f.mkdir("devices", "pci0000:00", "0000:00:04.0", "virtio1")
	blockDir := f.mkdir("devices", "pci0000:00", "0000:00:04.0", "virtio1", "block", "vda")
	f.link(pci, filepath.Join(blockDir, "device"))
	f.link(blockDir, filepath.Join(f.root, "class", "block", "vda"))
	// A device-mapper node with no slaves, and a device sysfs does not know.
	f.mapping("dm-9")

	for _, name := range []string{"/dev/vda", "/dev/dm-9", "/dev/sdx"} {
		got, err := SCSITargetOf(name)
		require.NoError(t, err, name)
		assert.Empty(t, got, name)
	}
}

// A LUN whose target cannot be read must not pass for a device of no target.
func TestSCSITargetOf_UnreadableIdentityIsAnError(t *testing.T) {
	f := withFakeSCSISysfs(t)
	f.disk("sdh", "session7")
	f.iscsiDisk("sdi", "session8", "(null)")
	f.fcDisk("sdj", "rport-3:0-4", testWWPN)
	f.mapping("dm-5", "sdj", "sdh")

	for _, name := range []string{"/dev/sdh", "/dev/sdi", "/dev/dm-5"} {
		_, err := SCSITargetOf(name)
		assert.Error(t, err, name)
	}
}

func TestSCSITargetOf_LoopingStackIsAnError(t *testing.T) {
	f := withFakeSCSISysfs(t)
	f.mapping("dm-6")
	f.link(filepath.Join(f.root, "class", "block", "dm-6"), filepath.Join(f.root, "devices", "virtual", "block", "dm-6", "slaves", "dm-6"))

	_, err := SCSITargetOf("/dev/dm-6")
	assert.Error(t, err)
}
