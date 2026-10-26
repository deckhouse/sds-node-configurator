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

func TestMultipathPathDevices(t *testing.T) {
	root := withFakeSysBlock(t)

	// A healthy map and a map with no paths left: the paths of the first are
	// paths whatever udev says about them, the second has none to report.
	fakeDM(t, root, "dm-1", "mpath-3600healthy", "mpathaa", 2)
	fakeDM(t, root, "dm-2", "mpath-3600dead", "mpathbb", 0)
	// A dm-crypt mapping over a disk: the disk is not a multipath path.
	fakeDM(t, root, "dm-3", "CRYPT-LUKS2-abc", "luks-abc", 0)
	require.NoError(t, os.MkdirAll(filepath.Join(root, "dm-3", "slaves", "sdz"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "sdz"), 0o755))

	assert.Equal(t, map[string]struct{}{"/dev/sda": {}, "/dev/sdb": {}}, MultipathPathDevices())
}

func TestMultipathPathDevices_NoSysBlock(t *testing.T) {
	withFakeSysBlock(t)
	sysBlockPath = filepath.Join(sysBlockPath, "absent")

	assert.Empty(t, MultipathPathDevices())
}
