package config

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestNewConfig(t *testing.T) {
	t.Run("AllValuesSet_ReturnsNoError", func(t *testing.T) {
		machineIdFile := "./host-root/etc/machine-id"
		expNodeName := "test-node"
		expMetricsPort := ":0000"
		expMachineId := "test-id"

		t.Setenv(NodeName, expNodeName)
		t.Setenv(MetricsPort, expMetricsPort)

		err := os.MkdirAll("./host-root/etc", 0750)
		if err != nil {
			t.Error(err)
		}

		file, err := os.Create(machineIdFile)
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

		_, err = file.Write([]byte(expMachineId))
		if err != nil {
			t.Error(err)
		}

		opts, err := NewConfig()

		if assert.NoError(t, err) {
			assert.Equal(t, expNodeName, opts.NodeName)
			assert.Equal(t, expMetricsPort, opts.MetricsPort)
			assert.Equal(t, expMachineId, opts.MachineId)
		}
	})

	t.Run("NodeNameNotSet_ReturnsError", func(t *testing.T) {
		machineIdFile := "./host-root/etc/machine-id"
		expMetricsPort := ":0000"
		expErrorMsg := fmt.Sprintf("[NewConfig] required %s env variable is not specified", NodeName)

		t.Setenv(MetricsPort, expMetricsPort)

		err := os.MkdirAll("./host-root/etc", 0750)
		if err != nil {
			t.Error(err)
		}

		file, err := os.Create(machineIdFile)
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

	t.Run("MachineIdNotSet_ReturnsError", func(t *testing.T) {
		expMetricsPort := ":0000"
		expNodeName := "test-node"
		expErrorMsg := fmt.Sprintf("[NewConfig] required %s env variable is not specified, error: %s",
			MachineID, "open host-root/etc/machine-id: no such file or directory")

		t.Setenv(MetricsPort, expMetricsPort)
		t.Setenv(NodeName, expNodeName)

		_, err := NewConfig()
		assert.EqualError(t, err, expErrorMsg)
	})

	t.Run("MetricsPortNotSet_ReturnsDefaultPort", func(t *testing.T) {
		machineIdFile := "./host-root/etc/machine-id"
		expNodeName := "test-node"
		expMetricsPort := ":8080"
		expMachineId := "test-id"

		t.Setenv(NodeName, expNodeName)

		err := os.MkdirAll("./host-root/etc", 0750)
		if err != nil {
			t.Error(err)
		}

		file, err := os.Create(machineIdFile)
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

		_, err = file.Write([]byte(expMachineId))
		if err != nil {
			t.Error(err)
		}

		opts, err := NewConfig()

		if assert.NoError(t, err) {
			assert.Equal(t, expNodeName, opts.NodeName)
			assert.Equal(t, expMetricsPort, opts.MetricsPort)
			assert.Equal(t, expMachineId, opts.MachineId)
		}
	})

}
