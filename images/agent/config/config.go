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
	"sds-node-configurator/pkg/logger"
	"time"
)

const (
	ScanInterval = "SCAN_INTERVAL"
	NodeName     = "NODE_NAME"
	LogLevel     = "LOG_LEVEL"
	MetricsPort  = "METRICS_PORT"
	MachineID    = "MACHINE_ID"
)

type Options struct {
	MachineId               string
	NodeName                string
	Loglevel                logger.Verbosity
	MetricsPort             string
	BlockDeviceScanInterval time.Duration
	VolumeGroupScanInterval time.Duration
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
		opts.MetricsPort = ":8080"
	}

	opts.BlockDeviceScanInterval = 5
	opts.VolumeGroupScanInterval = 5

	return &opts, nil
}

func getMachineId() (string, error) {
	id := os.Getenv(MachineID)
	if id == "" {
		args := []string{"-m", "-u", "-i", "-n", "-p", "-t", "1", "cat", "/etc/machine-id"}

		var stdout bytes.Buffer
		cmd := exec.Command("/usr/local/bin/flant/nsenter.static", args...)
		cmd.Stdout = &stdout
		err := cmd.Run()
		if err != nil {
			return "", err
		}

		id = stdout.String()
		fmt.Println("MACHINE ID " + id)

	}

	return id, nil
}
