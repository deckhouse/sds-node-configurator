package config

import (
	"fmt"
	"os"
	"time"
)

const (
	ScanInterval = "SCAN_INTERVAL"
	NodeName     = "NODE_NAME"
	MetricsPort  = "METRICS_PORT"
	MachineID    = "MACHINE_ID"
)

type Options struct {
	MachineId    string
	NodeName     string
	MetricsPort  string
	ScanInterval time.Duration
}

func NewConfig() (*Options, error) {
	var opts Options

	opts.NodeName = os.Getenv(NodeName)
	if opts.NodeName == "" {
		return nil, fmt.Errorf("[NewConfig] required %s env variable is not specified", NodeName)
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

	opts.ScanInterval = 5

	return &opts, nil
}

func getMachineId() (string, error) {
	id, err := os.ReadFile("host-root/etc/machine-id")
	if err != nil {
		return "", err
	}

	return string(id), nil
}
