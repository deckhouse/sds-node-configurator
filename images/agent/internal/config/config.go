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
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
)

const (
	ScanInterval                         = "SCAN_INTERVAL"
	NodeName                             = "NODE_NAME"
	LogLevel                             = "LOG_LEVEL"
	MetricsPort                          = "METRICS_PORT"
	MachineID                            = "MACHINE_ID"
	ThrottleInterval                     = "THROTTLER_INTERVAL"
	CmdDeadlineDuration                  = "CMD_DEADLINE_DURATION"
	DefaultHealthProbeBindAddressEnvName = "HEALTH_PROBE_BIND_ADDRESS"
	DefaultHealthProbeBindAddress        = ":4228"
	NetlinkBlockDeviceDiscovery          = "ENABLE_NETLINK_BLOCK_DEVICE_DISCOVERY"
	FileDevicesDirectory                 = "FILE_DEVICES_DIRECTORY"
	// DefaultFileDevicesDirectory is the base directory backing files are
	// confined to when the module config does not override it. It lives under
	// the module's own /opt/deckhouse/sds tree so a stray fileDevices entry
	// cannot fill an arbitrary host path. Keep in sync with the
	// fileDevicesDirectory default in openapi/config-values.yaml.
	DefaultFileDevicesDirectory = "/opt/deckhouse/sds/file-devices"

	FileDevicesMinFreeSpacePercent = "FILE_DEVICES_MIN_FREE_SPACE_PERCENT"
	// DefaultFileDevicesMinFreeSpacePercent is the share of the backing-file
	// filesystem the agent refuses to allocate into.
	//
	// It exists because "the file fits" and "the node survives" are different
	// questions. Backing files are preallocated, the default directory lives on
	// the node's root filesystem, and kubelet starts evicting pods at
	// nodefs.available<10% — so allowing an allocation down to the last free byte
	// causes exactly the node-level outage the free-space check is there to
	// prevent. 15% matches kubelet's stricter imagefs.available default and
	// leaves a margin above the nodefs one.
	//
	// Keep in sync with the fileDevicesMinFreeSpacePercent default in
	// openapi/config-values.yaml.
	DefaultFileDevicesMinFreeSpacePercent = 15
	// MaxFileDevicesMinFreeSpacePercent bounds the setting. Above this a
	// misconfiguration stops being a reserve and starts being "file devices do
	// not work"; the apiserver rejects it too, and this is the second line.
	MaxFileDevicesMinFreeSpacePercent = 90
)

type Features struct {
	NetlinkBlockDeviceDiscovery bool
}

type Config struct {
	MachineID               string
	NodeName                string
	Loglevel                logger.Verbosity
	MetricsPort             string
	BlockDeviceScanInterval time.Duration
	VolumeGroupScanInterval time.Duration
	LLVRequeueInterval      time.Duration
	LLVSRequeueInterval     time.Duration
	ThrottleInterval        time.Duration
	CmdDeadlineDuration     time.Duration
	HealthProbeBindAddress  string
	Features                Features
	FileDevicesDirectory    string
	// FileDevicesMinFreeSpacePercent is the share of the backing-file filesystem
	// that must stay free after a backing file is created or grown. Zero disables
	// the reserve, which is the right setting only for a filesystem the node does
	// not otherwise depend on.
	FileDevicesMinFreeSpacePercent int
}

func NewConfig() (*Config, error) {
	var cfg Config

	cfg.NodeName = os.Getenv(NodeName)
	if cfg.NodeName == "" {
		return nil, fmt.Errorf("[NewConfig] required %s env variable is not specified", NodeName)
	}

	loglevel := os.Getenv(LogLevel)
	if loglevel == "" {
		cfg.Loglevel = logger.DebugLevel
	} else {
		cfg.Loglevel = logger.Verbosity(loglevel)
	}

	machID, err := getMachineID()
	if err != nil {
		return nil, fmt.Errorf("[NewConfig] unable to get %s, error: %w", MachineID, err)
	}
	cfg.MachineID = machID

	cfg.MetricsPort = os.Getenv(MetricsPort)
	if cfg.MetricsPort == "" {
		cfg.MetricsPort = ":4202"
	}

	cfg.HealthProbeBindAddress = os.Getenv(DefaultHealthProbeBindAddressEnvName)
	if cfg.HealthProbeBindAddress == "" {
		cfg.HealthProbeBindAddress = DefaultHealthProbeBindAddress
	}

	cfg.FileDevicesDirectory = os.Getenv(FileDevicesDirectory)
	if cfg.FileDevicesDirectory == "" {
		cfg.FileDevicesDirectory = DefaultFileDevicesDirectory
	}
	if err := validateFileDevicesDirectory(cfg.FileDevicesDirectory); err != nil {
		return nil, err
	}

	// An unparseable or out-of-range value falls back to the default rather than
	// to "no reserve": the failure mode of the reserve being absent is a node
	// evicting its pods, and a typo in a module setting must not reach it.
	cfg.FileDevicesMinFreeSpacePercent = DefaultFileDevicesMinFreeSpacePercent
	if raw := os.Getenv(FileDevicesMinFreeSpacePercent); raw != "" {
		parsed, parseErr := strconv.Atoi(raw)
		switch {
		case parseErr != nil:
			return nil, fmt.Errorf("[NewConfig] %s must be an integer, got %q: %w", FileDevicesMinFreeSpacePercent, raw, parseErr)
		case parsed < 0 || parsed > MaxFileDevicesMinFreeSpacePercent:
			return nil, fmt.Errorf("[NewConfig] %s must be between 0 and %d, got %d", FileDevicesMinFreeSpacePercent, MaxFileDevicesMinFreeSpacePercent, parsed)
		default:
			cfg.FileDevicesMinFreeSpacePercent = parsed
		}
	}

	scanInt := os.Getenv(ScanInterval)
	if scanInt == "" {
		cfg.BlockDeviceScanInterval = 5 * time.Second
		cfg.VolumeGroupScanInterval = 5 * time.Second
		cfg.LLVRequeueInterval = 5 * time.Second
		cfg.LLVSRequeueInterval = 5 * time.Second
	} else {
		interval, err := strconv.Atoi(scanInt)
		if err != nil {
			return nil, fmt.Errorf("[NewConfig] unable to get %s, error: %w", ScanInterval, err)
		}
		cfg.BlockDeviceScanInterval = time.Duration(interval) * time.Second
		cfg.VolumeGroupScanInterval = time.Duration(interval) * time.Second
		cfg.LLVRequeueInterval = time.Duration(interval) * time.Second
		cfg.LLVSRequeueInterval = time.Duration(interval) * time.Second
	}

	thrInt := os.Getenv(ThrottleInterval)
	if thrInt == "" {
		cfg.ThrottleInterval = 3 * time.Second
	} else {
		interval, err := strconv.Atoi(scanInt)
		if err != nil {
			return nil, fmt.Errorf("[NewConfig] unable to get %s, error: %w", ThrottleInterval, err)
		}

		cfg.ThrottleInterval = time.Duration(interval) * time.Second
	}

	cmdDur := os.Getenv(CmdDeadlineDuration)
	if cmdDur == "" {
		cfg.CmdDeadlineDuration = 30 * time.Second
	} else {
		duration, err := strconv.Atoi(cmdDur)
		if err != nil {
			return nil, fmt.Errorf("[NewConfig] unable to get %s, error: %w", CmdDeadlineDuration, err)
		}

		cfg.CmdDeadlineDuration = time.Duration(duration) * time.Second
	}

	netlinkBlockDeviceDiscovery, err := getBoolEnv(NetlinkBlockDeviceDiscovery, false)
	if err != nil {
		return nil, err
	}
	cfg.Features = Features{
		NetlinkBlockDeviceDiscovery: netlinkBlockDeviceDiscovery,
	}

	return &cfg, nil
}

func getBoolEnv(name string, def bool) (bool, error) {
	val := os.Getenv(name)
	if val == "" {
		return def, nil
	}

	parsed, err := strconv.ParseBool(val)
	if err != nil {
		return false, fmt.Errorf("[NewConfig] invalid value for %s: %q", name, val)
	}

	return parsed, nil
}

func getMachineID() (string, error) {
	id := os.Getenv(MachineID)
	if id == "" {
		args := []string{"-m", "-u", "-i", "-n", "-p", "-t", "1", "cat", "/etc/machine-id"}

		var stdout bytes.Buffer
		cmd := exec.Command(internal.NSENTERCmd, args...)
		cmd.Stdout = &stdout
		err := cmd.Run()
		if err != nil {
			return "", err
		}

		id = strings.TrimSpace(stdout.String())
		fmt.Println("MACHINE ID " + id)
	}

	return id, nil
}

// validateFileDevicesDirectory rejects a base directory that cannot serve as a
// confinement boundary for spec.fileDevices backing files.
//
// The module config schema already refuses a relative path and the filesystem
// root, but it cannot refuse a `..` component: a config/CRD pattern is an RE2
// regexp and RE2 has no lookahead. That gap matters because the boundary is
// enforced lexically — isWithinBaseDir does a filepath.Clean and a prefix
// compare, it deliberately does not resolve symlinks — so `/opt/../etc` would
// silently confine every backing file to `/etc` while the module config still
// read as the default. Failing loudly at startup is the only place left to say
// so, and it is the same treatment FILE_DEVICES_MIN_FREE_SPACE_PERCENT already
// gets for an out-of-range value.
func validateFileDevicesDirectory(dir string) error {
	if !filepath.IsAbs(dir) {
		return fmt.Errorf("[NewConfig] %s must be an absolute path, got %q", FileDevicesDirectory, dir)
	}
	for _, part := range strings.Split(dir, string(filepath.Separator)) {
		if part == ".." {
			return fmt.Errorf("[NewConfig] %s must not contain '..' segments, got %q", FileDevicesDirectory, dir)
		}
	}
	if filepath.Clean(dir) == string(filepath.Separator) {
		return fmt.Errorf("[NewConfig] %s must not be the filesystem root: every absolute path would be a valid subdirectory of it, which disables the confinement it exists to provide", FileDevicesDirectory)
	}
	return nil
}
