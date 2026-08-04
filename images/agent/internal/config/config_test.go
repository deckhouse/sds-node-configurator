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

package config

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewConfig(t *testing.T) {
	t.Run("AllValuesSet_ReturnsNoError", func(t *testing.T) {
		expNodeName := "test-node"
		expMetricsPort := ":0000"
		expMachineID := "test-id"

		err := os.Setenv(NodeName, expNodeName)
		if err != nil {
			t.Error(err)
		}
		err = os.Setenv(MetricsPort, expMetricsPort)
		if err != nil {
			t.Error(err)
		}
		err = os.Setenv(MachineID, expMachineID)
		if err != nil {
			t.Error(err)
		}
		defer os.Clearenv()

		opts, err := NewConfig()

		if assert.NoError(t, err) {
			assert.Equal(t, expNodeName, opts.NodeName)
			assert.Equal(t, expMetricsPort, opts.MetricsPort)
			assert.Equal(t, expMachineID, opts.MachineID)
		}
	})

	t.Run("NodeNameNotSet_ReturnsError", func(t *testing.T) {
		machineIDFile := "./host-root/etc/machine-id"
		expMetricsPort := ":0000"
		expErrorMsg := fmt.Sprintf("[NewConfig] required %s env variable is not specified", NodeName)

		err := os.Setenv(MetricsPort, expMetricsPort)
		if err != nil {
			t.Error(err)
		}
		defer os.Clearenv()

		err = os.MkdirAll("./host-root/etc", 0750)
		if err != nil {
			t.Error(err)
		}

		file, err := os.Create(machineIDFile)
		if err != nil {
			t.Error(err)
		}
		defer func() {
			err = file.Close()
			if err != nil {
				t.Error(err)
			}

			err = os.RemoveAll("./host-root")
			if err != nil {
				t.Error(err)
			}
		}()

		_, err = NewConfig()
		assert.EqualError(t, err, expErrorMsg)
	})

	t.Run("MachineIDNotSet_ReturnsError", func(t *testing.T) {
		expMetricsPort := ":0000"
		expNodeName := "test-node"
		expErrorMsg := fmt.Sprintf("[NewConfig] unable to get %s, error: %s",
			MachineID, "fork/exec /opt/deckhouse/sds/bin/nsenter: no such file or directory")

		err := os.Setenv(MetricsPort, expMetricsPort)
		if err != nil {
			t.Error(err)
		}
		err = os.Setenv(NodeName, expNodeName)
		if err != nil {
			t.Error(err)
		}
		defer os.Clearenv()

		_, err = NewConfig()
		assert.EqualError(t, err, expErrorMsg)
	})

	t.Run("FileDevicesDirectoryNotSet_ReturnsDefault", func(t *testing.T) {
		err := os.Setenv(NodeName, "test-node")
		assert.NoError(t, err)
		err = os.Setenv(MachineID, "test-id")
		assert.NoError(t, err)
		defer os.Clearenv()

		opts, err := NewConfig()
		if assert.NoError(t, err) {
			assert.Equal(t, DefaultFileDevicesDirectory, opts.FileDevicesDirectory)
		}
	})

	t.Run("FileDevicesDirectorySet_IsHonoured", func(t *testing.T) {
		err := os.Setenv(NodeName, "test-node")
		assert.NoError(t, err)
		err = os.Setenv(MachineID, "test-id")
		assert.NoError(t, err)
		err = os.Setenv(FileDevicesDirectory, "/mnt/data/file-devices")
		assert.NoError(t, err)
		defer os.Clearenv()

		opts, err := NewConfig()
		if assert.NoError(t, err) {
			assert.Equal(t, "/mnt/data/file-devices", opts.FileDevicesDirectory)
		}
	})

	t.Run("MetricsPortNotSet_ReturnsDefaultPort", func(t *testing.T) {
		expNodeName := "test-node"
		expMetricsPort := ":4202"
		expMachineID := "test-id"

		err := os.Setenv(NodeName, expNodeName)
		if err != nil {
			t.Error(err)
		}
		err = os.Setenv(MachineID, expMachineID)
		if err != nil {
			t.Error(err)
		}

		defer os.Clearenv()

		opts, err := NewConfig()

		if assert.NoError(t, err) {
			assert.Equal(t, expNodeName, opts.NodeName)
			assert.Equal(t, expMetricsPort, opts.MetricsPort)
			assert.Equal(t, expMachineID, opts.MachineID)
		}
	})

	t.Run("NetlinkBlockDeviceDiscoveryNotSet_ReturnsDefaultFalse", func(t *testing.T) {
		require.NoError(t, os.Setenv(NodeName, "test-node"))
		require.NoError(t, os.Setenv(MachineID, "test-id"))
		defer os.Clearenv()

		cfg, err := NewConfig()

		if assert.NoError(t, err) {
			assert.False(t, cfg.Features.NetlinkBlockDeviceDiscovery)
		}
	})

	t.Run("NetlinkBlockDeviceDiscoveryTrue_ReturnsTrue", func(t *testing.T) {
		require.NoError(t, os.Setenv(NodeName, "test-node"))
		require.NoError(t, os.Setenv(MachineID, "test-id"))
		require.NoError(t, os.Setenv(NetlinkBlockDeviceDiscovery, "true"))
		defer os.Clearenv()

		cfg, err := NewConfig()

		if assert.NoError(t, err) {
			assert.True(t, cfg.Features.NetlinkBlockDeviceDiscovery)
		}
	})

	t.Run("NetlinkBlockDeviceDiscoveryInvalid_ReturnsError", func(t *testing.T) {
		require.NoError(t, os.Setenv(NodeName, "test-node"))
		require.NoError(t, os.Setenv(MachineID, "test-id"))
		require.NoError(t, os.Setenv(NetlinkBlockDeviceDiscovery, "not-a-bool"))
		defer os.Clearenv()

		_, err := NewConfig()

		assert.EqualError(t, err, fmt.Sprintf("[NewConfig] invalid value for %s: %q", NetlinkBlockDeviceDiscovery, "not-a-bool"))
	})
}

// The reserve is what stands between a mistyped `size` and a node evicting its
// pods, so a value the module chart should never produce must not silently turn
// into "no reserve". Every rejected input has to fail loudly at startup instead.
func TestNewConfig_FileDevicesMinFreeSpacePercent(t *testing.T) {
	tests := []struct {
		name    string
		env     string
		want    int
		wantErr bool
	}{
		{"unset falls back to the default", "", DefaultFileDevicesMinFreeSpacePercent, false},
		{"explicit value is honoured", "25", 25, false},
		{"zero disables the reserve", "0", 0, false},
		{"upper bound is inclusive", "90", MaxFileDevicesMinFreeSpacePercent, false},
		{"above the bound is rejected", "91", 0, true},
		{"negative is rejected", "-1", 0, true},
		{"non-numeric is rejected", "15%", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(NodeName, "test-node")
			t.Setenv(MachineID, "test-id")
			if tt.env != "" {
				t.Setenv(FileDevicesMinFreeSpacePercent, tt.env)
			}

			cfg, err := NewConfig()
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, cfg.FileDevicesMinFreeSpacePercent)
		})
	}
}

// The module config schema refuses a relative path and the filesystem root, but
// it cannot refuse a `..` component — a config pattern is an RE2 regexp and RE2
// has no lookahead. The gap matters because the boundary is enforced lexically
// (isWithinBaseDir does filepath.Clean and a prefix compare, deliberately without
// resolving symlinks), so `/opt/../etc` would silently confine every backing file
// to `/etc` while the module config still read as the default. Startup is the
// only place left to say so.
func TestValidateFileDevicesDirectory(t *testing.T) {
	tests := map[string]struct {
		dir     string
		wantErr string
	}{
		"default":                  {DefaultFileDevicesDirectory, ""},
		"dedicated_disk":           {"/mnt/data/sds-file-devices", ""},
		"dot_component_is_no_op":   {"/opt/./sds-file-devices", ""},
		"relative":                 {"opt/deckhouse/sds/file-devices", "absolute path"},
		"parent_escape":            {"/opt/../etc", "'..' segments"},
		"parent_escape_mid_path":   {"/opt/deckhouse/../../etc/sds", "'..' segments"},
		"trailing_parent":          {"/opt/deckhouse/sds/..", "'..' segments"},
		"filesystem_root":          {"/", "filesystem root"},
		"filesystem_root_via_dots": {"/opt/..", "'..' segments"},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			err := validateFileDevicesDirectory(tt.dir)
			if tt.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}
