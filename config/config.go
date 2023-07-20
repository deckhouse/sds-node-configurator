package config

import (
	"fmt"
	"os"
	"storage-configurator/pkg/utils/errors/scerror"
)

// ScanInterval Scan block device interval seconds
const (
	ScanInterval = 3
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
		return nil, fmt.Errorf(scerror.ParseConfigParamsError)
	}
	return &opts, nil
}
