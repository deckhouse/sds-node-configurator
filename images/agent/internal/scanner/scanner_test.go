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

package scanner

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/config"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/mock_utils"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

// What lsblk prints in the agent container: the root partition carries the
// container's /etc/hosts bind mount, and /dev/sdb, mounted on the host with its
// filesystem signature wiped, looks like an empty disk.
const containerLsblkOutput = `{
   "blockdevices": [
      {"name": "/dev/sda1", "maj:min": "8:1", "mountpoint": "/etc/hosts", "size": "30G", "fstype": "ext4", "type": "part", "kname": "/dev/sda1", "pkname": "/dev/sda"},
      {"name": "/dev/sdb", "maj:min": "8:16", "mountpoint": null, "size": "5G", "fstype": null, "type": "disk", "kname": "/dev/sdb", "pkname": null},
      {"name": "/dev/sdc", "maj:min": "8:32", "mountpoint": null, "size": "5G", "fstype": null, "type": "disk", "kname": "/dev/sdc", "pkname": null}
   ]
}`

const hostMountInfo = `22 1 8:1 / / rw,relatime shared:1 - ext4 /dev/sda1 rw
24 22 0:20 / /proc rw,nosuid,nodev,noexec,relatime shared:5 - proc proc rw
40 22 8:16 / /mnt/e2e-test rw,relatime shared:20 - ext4 /dev/sdb rw
`

func lsblkScanner(t *testing.T, mountInfoPath string) *scanner {
	t.Helper()
	devices, err := utils.NewCommands().UnmarshalDevices([]byte(containerLsblkOutput))
	require.NoError(t, err)

	commands := mock_utils.NewMockCommands(gomock.NewController(t))
	commands.EXPECT().GetBlockDevices(gomock.Any()).Return(devices, "lsblk", bytes.Buffer{}, nil)
	return &scanner{commands: commands, hostMountInfo: mountInfoPath}
}

func TestScanDevices_TakesMountPointsFromTheHost(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mountinfo")
	require.NoError(t, os.WriteFile(path, []byte(hostMountInfo), 0o644))
	s := lsblkScanner(t, path)

	devices, _, err := s.scanDevices(context.Background(), logger.NewNop(), config.Config{CmdDeadlineDuration: time.Minute})
	require.NoError(t, err)
	require.Len(t, devices, 3)

	assert.Equal(t, "/", devices[0].MountPoint)

	mounted := internal.NewBlockDeviceCandidateByDevice(&devices[1], "node", "machine")
	assert.False(t, mounted.Consumable)
	assert.Equal(t, v1alpha1.ReasonDeviceMounted, mounted.ConsumableReason)
	assert.Contains(t, mounted.ConsumableMessage, "/mnt/e2e-test")

	free := internal.NewBlockDeviceCandidateByDevice(&devices[2], "node", "machine")
	assert.True(t, free.Consumable)
}

// Without the host's mountinfo every device would look unmounted, and a mounted
// disk with no filesystem signature consumable.
func TestScanDevices_FailsWithoutHostMountInfo(t *testing.T) {
	s := lsblkScanner(t, filepath.Join(t.TempDir(), "missing"))

	devices, _, err := s.scanDevices(context.Background(), logger.NewNop(), config.Config{CmdDeadlineDuration: time.Minute})

	assert.Error(t, err)
	assert.Nil(t, devices)
}
