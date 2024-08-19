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
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewConfig(t *testing.T) {
	t.Run("AllValuesSet_ReturnsNoError", func(t *testing.T) {
		expMetricsPort := ":0000"

		err := os.Setenv(MetricsPort, expMetricsPort)
		if err != nil {
			t.Error(err)
		}
		defer os.Clearenv()

		opts, err := NewConfig()

		if assert.NoError(t, err) {
			assert.Equal(t, expMetricsPort, opts.MetricsPort)
		}
	})

	t.Run("MetricsPortNotSet_ReturnsDefaultPort", func(t *testing.T) {
		expMetricsPort := ":8080"

		defer os.Clearenv()

		opts, err := NewConfig()

		if assert.NoError(t, err) {
			assert.Equal(t, expMetricsPort, opts.MetricsPort)
		}
	})
}
