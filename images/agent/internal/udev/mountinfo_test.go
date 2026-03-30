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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func withFakeMountInfo(t *testing.T, content string) {
	t.Helper()
	dir := t.TempDir()
	fakePath := filepath.Join(dir, "mountinfo")
	require.NoError(t, os.WriteFile(fakePath, []byte(content), 0o644))

	orig := procSelfMountInfo
	procSelfMountInfo = fakePath
	t.Cleanup(func() {
		procSelfMountInfo = orig
	})
}

func TestParseMountInfo_MultipleEntries(t *testing.T) {
	content := `22 1 8:1 / / rw,relatime shared:1 - ext4 /dev/sda1 rw
23 22 8:2 / /boot rw,nosuid,nodev shared:2 - ext4 /dev/sda2 rw
24 22 0:20 / /proc rw,nosuid,nodev,noexec,relatime shared:5 - proc proc rw
25 22 259:0 / /data rw,relatime shared:10 - xfs /dev/nvme0n1 rw
`
	withFakeMountInfo(t, content)

	mounts, err := ParseMountInfo()
	require.NoError(t, err)

	assert.Equal(t, "/", mounts["8:1"])
	assert.Equal(t, "/boot", mounts["8:2"])
	assert.Equal(t, "/proc", mounts["0:20"])
	assert.Equal(t, "/data", mounts["259:0"])
	assert.Len(t, mounts, 4)
}

func TestParseMountInfo_DuplicateDevID_FirstWins(t *testing.T) {
	content := `22 1 8:1 / / rw,relatime shared:1 - ext4 /dev/sda1 rw
30 22 8:1 / /mnt/copy rw,relatime shared:1 - ext4 /dev/sda1 rw
`
	withFakeMountInfo(t, content)

	mounts, err := ParseMountInfo()
	require.NoError(t, err)

	assert.Equal(t, "/", mounts["8:1"], "first mount point should win")
	assert.Len(t, mounts, 1)
}

func TestParseMountInfo_EmptyFile(t *testing.T) {
	withFakeMountInfo(t, "")

	mounts, err := ParseMountInfo()
	require.NoError(t, err)
	assert.Empty(t, mounts)
}

func TestParseMountInfo_FileNotExist(t *testing.T) {
	orig := procSelfMountInfo
	procSelfMountInfo = "/nonexistent/path/mountinfo"
	t.Cleanup(func() {
		procSelfMountInfo = orig
	})

	_, err := ParseMountInfo()
	assert.Error(t, err)
}

func TestParseMountInfo_ShortLines_Skipped(t *testing.T) {
	content := `22 1 8:1 /
25 22 259:0 / /data rw,relatime shared:10 - xfs /dev/nvme0n1 rw
`
	withFakeMountInfo(t, content)

	mounts, err := ParseMountInfo()
	require.NoError(t, err)
	assert.Len(t, mounts, 1)
	assert.Equal(t, "/data", mounts["259:0"])
}

func TestParseMountInfo_RealWorldKernel(t *testing.T) {
	content := `36 35 98:0 /mnt1 /mnt2 rw,noatime master:1 - ext3 /dev/root rw,errors=continue
100 35 8:1 / /boot rw,noatime - ext4 /dev/sda1 rw,data=ordered
`
	withFakeMountInfo(t, content)

	mounts, err := ParseMountInfo()
	require.NoError(t, err)

	assert.Equal(t, "/mnt2", mounts["98:0"])
	assert.Equal(t, "/boot", mounts["8:1"])
}
