/*
Copyright 2025 Flant JSC

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
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/deckhouse/sds-common-lib/monitoring"
)

func TestNewConfig(t *testing.T) {
	t.Run("AllValuesSet_ReturnsNoError", func(t *testing.T) {
		expMetricsBindAddress := ":0000"

		err := os.Setenv(monitoring.EnvMetricsBindAddress, expMetricsBindAddress)
		if err != nil {
			t.Error(err)
		}
		defer os.Clearenv()

		opts, err := NewConfig()

		if assert.NoError(t, err) {
			assert.Equal(t, expMetricsBindAddress, opts.MetricsBindAddress)
		}
	})

	t.Run("MetricsBindAddressNotSet_ReturnsDefaultAddress", func(t *testing.T) {
		expMetricsBindAddress := DefaultMetricsBindAddress

		defer os.Clearenv()

		opts, err := NewConfig()

		if assert.NoError(t, err) {
			assert.Equal(t, expMetricsBindAddress, opts.MetricsBindAddress)
		}
	})

	t.Run("MissingBlockDeviceRetention", func(t *testing.T) {
		defer os.Clearenv()

		opts, err := NewConfig()
		if assert.NoError(t, err) {
			assert.Equal(t, DefaultMissingBlockDeviceRetentionHours*time.Hour, opts.MissingBlockDeviceRetention)
			assert.Equal(t, DefaultAgentNamespace, opts.AgentNamespace)
		}

		t.Setenv(MissingBlockDeviceRetentionHours, "0")
		opts, err = NewConfig()
		if assert.NoError(t, err) {
			assert.Zero(t, opts.MissingBlockDeviceRetention)
		}

		for _, bad := range []string{"-1", "8761", "day"} {
			t.Setenv(MissingBlockDeviceRetentionHours, bad)
			_, err = NewConfig()
			assert.Error(t, err, bad)
		}
	})

	t.Run("MissingNodeGracePeriod", func(t *testing.T) {
		defer os.Clearenv()
		opts, err := NewConfig()
		if assert.NoError(t, err) {
			assert.Equal(t, DefaultMissingNodeGracePeriodMinutes*time.Minute, opts.MissingNodeGracePeriod)
		}
		t.Setenv(MissingNodeGracePeriodMinutes, "0")
		opts, err = NewConfig()
		if assert.NoError(t, err) {
			assert.Zero(t, opts.MissingNodeGracePeriod)
		}
		t.Setenv(MissingNodeGracePeriodMinutes, "90")
		opts, err = NewConfig()
		if assert.NoError(t, err) {
			assert.Equal(t, 90*time.Minute, opts.MissingNodeGracePeriod)
		}
		for _, bad := range []string{"-1", "10081", "hour"} {
			t.Setenv(MissingNodeGracePeriodMinutes, bad)
			_, err = NewConfig()
			assert.Error(t, err, bad)
		}
	})
}
