/*
Copyright 2023 Flant JSC

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
	"strconv"
	"strings"
	"time"

	"agent/internal"
	"agent/internal/logger"
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
)

type Config struct {
	MachineID               string
	NodeName                string
	Loglevel                logger.Verbosity
	MetricsPort             string
	BlockDeviceScanInterval time.Duration
	VolumeGroupScanInterval time.Duration
	LLVRequeueInterval      time.Duration
	ThrottleInterval        time.Duration
	CmdDeadlineDuration     time.Duration
	HealthProbeBindAddress  string
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

	scanInt := os.Getenv(ScanInterval)
	if scanInt == "" {
		cfg.BlockDeviceScanInterval = 5 * time.Second
		cfg.VolumeGroupScanInterval = 5 * time.Second
		cfg.LLVRequeueInterval = 5 * time.Second
	} else {
		interval, err := strconv.Atoi(scanInt)
		if err != nil {
			return nil, fmt.Errorf("[NewConfig] unable to get %s, error: %w", ScanInterval, err)
		}
		cfg.BlockDeviceScanInterval = time.Duration(interval) * time.Second
		cfg.VolumeGroupScanInterval = time.Duration(interval) * time.Second
		cfg.LLVRequeueInterval = time.Duration(interval) * time.Second
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

	return &cfg, nil
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
