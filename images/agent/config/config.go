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
	"agent/internal"
	"agent/pkg/logger"
	"bytes"
	"fmt"
	"os"
	"os/exec"

	"strconv"
	"strings"
	"time"
)

const (
	ScanInterval        = "SCAN_INTERVAL"
	NodeName            = "NODE_NAME"
	LogLevel            = "LOG_LEVEL"
	MetricsPort         = "METRICS_PORT"
	MachineID           = "MACHINE_ID"
	ThrottleInterval    = "THROTTLER_INTERVAL"
	CmdDeadlineDuration = "CMD_DEADLINE_DURATION"
)

type Options struct {
	MachineId                  string
	NodeName                   string
	Loglevel                   logger.Verbosity
	MetricsPort                string
	BlockDeviceScanIntervalSec time.Duration
	VolumeGroupScanIntervalSec time.Duration
	LLVRequeueIntervalSec      time.Duration
	ThrottleIntervalSec        time.Duration
	CmdDeadlineDurationSec     time.Duration
}

func NewConfig() (*Options, error) {
	var opts Options

	opts.NodeName = os.Getenv(NodeName)
	if opts.NodeName == "" {
		return nil, fmt.Errorf("[NewConfig] required %s env variable is not specified", NodeName)
	}

	loglevel := os.Getenv(LogLevel)
	if loglevel == "" {
		opts.Loglevel = logger.DebugLevel
	} else {
		opts.Loglevel = logger.Verbosity(loglevel)
	}

	machId, err := getMachineId()
	if err != nil {
		return nil, fmt.Errorf("[NewConfig] unable to get %s, error: %w", MachineID, err)
	}
	opts.MachineId = machId

	opts.MetricsPort = os.Getenv(MetricsPort)
	if opts.MetricsPort == "" {
		opts.MetricsPort = ":9695"
	}

	scanInt := os.Getenv(ScanInterval)
	if scanInt == "" {
		opts.BlockDeviceScanIntervalSec = 5 * time.Second
		opts.VolumeGroupScanIntervalSec = 5 * time.Second
		opts.LLVRequeueIntervalSec = 5 * time.Second
	} else {
		interval, err := strconv.Atoi(scanInt)
		if err != nil {
			return nil, fmt.Errorf("[NewConfig] unable to get %s, error: %w", ScanInterval, err)
		}
		opts.BlockDeviceScanIntervalSec = time.Duration(interval) * time.Second
		opts.VolumeGroupScanIntervalSec = time.Duration(interval) * time.Second
		opts.LLVRequeueIntervalSec = time.Duration(interval) * time.Second
	}

	thrInt := os.Getenv(ThrottleInterval)
	if thrInt == "" {
		opts.ThrottleIntervalSec = 3 * time.Second
	} else {
		interval, err := strconv.Atoi(scanInt)
		if err != nil {
			return nil, fmt.Errorf("[NewConfig] unable to get %s, error: %w", ThrottleInterval, err)
		}

		opts.ThrottleIntervalSec = time.Duration(interval) * time.Second
	}

	cmdDur := os.Getenv(CmdDeadlineDuration)
	if cmdDur == "" {
		opts.CmdDeadlineDurationSec = 30 * time.Second
	} else {
		duration, err := strconv.Atoi(cmdDur)
		if err != nil {
			return nil, fmt.Errorf("[NewConfig] unable to get %s, error: %w", CmdDeadlineDuration, err)
		}

		opts.CmdDeadlineDurationSec = time.Duration(duration) * time.Second
	}

	return &opts, nil
}

func getMachineId() (string, error) {
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
