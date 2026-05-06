/*
Copyright 2026 Flant JSC

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
package cfg

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewModeCreateNew(t *testing.T) {
	err := os.Setenv("TEST_CLUSTER_CREATE_MODE", ModeCreateNew)
	assert.NoError(t, err, "Failed to set env TEST_CLUSTER_CREATE_MODE")

	err = os.Setenv("TEST_CLUSTER_STORAGE_CLASS", "linstor-r1")
	assert.NoError(t, err, "Failed to set env TEST_CLUSTER_STORAGE_CLASS")

	err = os.Setenv("SSH_USER", "test")
	assert.NoError(t, err, "Failed to set env SSH_USER")

	err = os.Setenv("SSH_HOST", "localhost")
	assert.NoError(t, err, "Failed to set env SSH_HOST")

	err = os.Setenv("SSH_PRIVATE_KEY", "test-key")
	assert.NoError(t, err, "Failed to set env SSH_PRIVATE_KEY")

	err = os.Setenv("KUBE_CONFIG_PATH", "/path/to/kubeconfig")
	assert.NoError(t, err, "Failed to set env KUBE_CONFIG_PATH")

	err = os.Setenv("DKP_LICENSE_KEY", "test-key")
	assert.NoError(t, err, "Failed to set env DKP_LICENSE_KEY")

	got, err := New()
	assert.NoError(t, err)
	assert.Equal(t, &Config{
		TestCluster: TestCluster{
			CreateMode:   ModeCreateNew,
			Namespace:    "e2e-test-cluster",
			StorageClass: "linstor-r1",
			Cleanup:      false,
		},
		SSH: SSH{
			User:       "test",
			Host:       "localhost",
			PrivateKey: "test-key",
		},
		KubeConfigPath: "/path/to/kubeconfig",
		DKPLicenceKey:  "test-key",
		LogLevel:       "info",
	}, got)
}
