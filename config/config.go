package config

import (
	"fmt"
	"os"
)

// ScanInterval Scan block device interval seconds
const (
	ScanInterval = 10
	NodeName     = "NODE_NAME"
)

type Options struct {
	ScanInterval int
	NodeName     string
}

func NewConfig() (*Options, error) {
	var opts Options
	opts.ScanInterval = ScanInterval
	opts.NodeName = os.Getenv(NodeName)
	if opts.NodeName == "" {
		return nil, fmt.Errorf("required NODE_NAME env variable is not specified")
	}
	return &opts, nil
}
