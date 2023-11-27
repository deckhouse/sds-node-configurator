package config

import (
	"fmt"
	"os"
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
	}

	machId, err := getMachineId()
	if err != nil {
		return nil, fmt.Errorf("[NewConfig] required %s env variable is not specified, error: %w",
			MachineID, err)
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
	id, err := os.ReadFile("/host-root/etc/machine-id")
	if err != nil {
		return "", err
	}

	return string(id), nil
}
