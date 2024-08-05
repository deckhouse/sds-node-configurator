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
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestNewConfig(t *testing.T) {
	t.Run("AllValuesSet_ReturnsNoError", func(t *testing.T) {
		expNodeName := "test-node"
		expMetricsPort := ":0000"
		expMachineId := "test-id"

		err := os.Setenv(NodeName, expNodeName)
		if err != nil {
			t.Error(err)
		}
		err = os.Setenv(MetricsPort, expMetricsPort)
		if err != nil {
			t.Error(err)
		}
		err = os.Setenv(MachineID, expMachineId)
		defer os.Clearenv()

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

		err := os.Setenv(MetricsPort, expMetricsPort)
		if err != nil {
			t.Error(err)
		}
		defer os.Clearenv()

		err = os.MkdirAll("./host-root/etc", 0750)
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
		expErrorMsg := fmt.Sprintf("[NewConfig] unable to get %s, error: %s",
			MachineID, "fork/exec /opt/deckhouse/sds/bin/nsenter.static: no such file or directory")

		err := os.Setenv(MetricsPort, expMetricsPort)
		if err != nil {
			t.Error(err)
		}
		err = os.Setenv(NodeName, expNodeName)
		if err != nil {
			t.Error(err)
		}
		defer os.Clearenv()

		_, err = NewConfig()
		assert.EqualError(t, err, expErrorMsg)
	})

	t.Run("MetricsPortNotSet_ReturnsDefaultPort", func(t *testing.T) {
		expNodeName := "test-node"
		expMetricsPort := ":4202"
		expMachineId := "test-id"

		err := os.Setenv(NodeName, expNodeName)
		if err != nil {
			t.Error(err)
		}
		err = os.Setenv(MachineID, expMachineId)
		if err != nil {
			t.Error(err)
		}

		defer os.Clearenv()

		opts, err := NewConfig()

		if assert.NoError(t, err) {
			assert.Equal(t, expNodeName, opts.NodeName)
			assert.Equal(t, expMetricsPort, opts.MetricsPort)
			assert.Equal(t, expMachineId, opts.MachineId)
		}
	})

}
