package config

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewConfig(t *testing.T) {
	t.Setenv("NODE_NAME", "test-node")
	minLen := 3
	options, err := NewConfig()
	if err != nil {
		t.Errorf(err.Error())
	}

	if len(options.NodeName) < minLen {
		t.Errorf("NODE_NAME is empty")
	}

	assert.Equal(t, options.ScanInterval, ScanInterval)

	if len(options.MetricsPort) < minLen {
		t.Errorf("METRICS_PORT is empty")
	}

}
