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

func TestLoad(t *testing.T) {
	for _, k := range []string{
		"TEST_CLUSTER_NAMESPACE",
		"TEST_CLUSTER_STORAGE_CLASS",
		"MODULES_MODULE_TAG",
	} {
		assert.NoError(t, os.Unsetenv(k))
	}

	assert.NoError(t, os.Setenv("TEST_CLUSTER_STORAGE_CLASS", "linstor-r1"))

	got, err := Load()
	assert.NoError(t, err)
	assert.Equal(t, &Config{
		TestCluster: TestCluster{
			Namespace:    "e2e-test-cluster",
			StorageClass: "linstor-r1",
		},
		ModulesImageTag: "main",
	}, got)
}

func TestLoadOverrides(t *testing.T) {
	for _, k := range []string{
		"TEST_CLUSTER_NAMESPACE",
		"TEST_CLUSTER_STORAGE_CLASS",
		"MODULES_MODULE_TAG",
	} {
		assert.NoError(t, os.Unsetenv(k))
	}

	assert.NoError(t, os.Setenv("TEST_CLUSTER_NAMESPACE", "custom-ns"))
	assert.NoError(t, os.Setenv("TEST_CLUSTER_STORAGE_CLASS", "linstor-r1"))
	assert.NoError(t, os.Setenv("MODULES_MODULE_TAG", "pr-123"))

	got, err := Load()
	assert.NoError(t, err)
	assert.Equal(t, &Config{
		TestCluster: TestCluster{
			Namespace:    "custom-ns",
			StorageClass: "linstor-r1",
		},
		ModulesImageTag: "pr-123",
	}, got)
}

func TestLoadStressDefaults(t *testing.T) {
	got, err := LoadStress()
	assert.NoError(t, err)
	assert.Equal(t, 15, got.Target)
	assert.Equal(t, 5, got.BatchSize)
	assert.Equal(t, "1Gi", got.DiskSize)
	assert.Equal(t, 15, got.MaxVMBlockDevices)
	assert.False(t, got.Strict)
	assert.Equal(t, 1, got.MinReady)
}

func TestLoadStressTargetZero(t *testing.T) {
	t.Setenv("E2E_STRESS_MAX_VG_TARGET", "0")
	_, err := LoadStress()
	assert.Error(t, err)
}

func TestLoadStressBatchZero(t *testing.T) {
	t.Setenv("E2E_STRESS_MAX_VG_BATCH_SIZE", "0")
	_, err := LoadStress()
	assert.Error(t, err)
}

func TestLoadStressBadDiskSize(t *testing.T) {
	t.Setenv("E2E_STRESS_MAX_VG_DISK_SIZE", "notaquantity")
	_, err := LoadStress()
	assert.Error(t, err)
}

func TestLoadStressBatchExceedsTarget(t *testing.T) {
	t.Setenv("E2E_STRESS_MAX_VG_TARGET", "3")
	t.Setenv("E2E_STRESS_MAX_VG_BATCH_SIZE", "5")
	_, err := LoadStress()
	assert.Error(t, err)
}

func TestLoadStressTargetClampedToMaxVM(t *testing.T) {
	t.Setenv("E2E_STRESS_MAX_VG_TARGET", "100")
	t.Setenv("E2E_STRESS_MAX_VM_BLOCK_DEVICES", "10")
	got, err := LoadStress()
	assert.NoError(t, err)
	assert.Equal(t, 10, got.Target)
	assert.Equal(t, 10, got.MaxVMBlockDevices)
}

func TestLoadStressMaxVMClampedToCeiling(t *testing.T) {
	t.Setenv("E2E_STRESS_MAX_VG_TARGET", "100")
	t.Setenv("E2E_STRESS_MAX_VM_BLOCK_DEVICES", "50")
	got, err := LoadStress()
	assert.NoError(t, err)
	assert.Equal(t, 16, got.MaxVMBlockDevices)
	assert.Equal(t, 16, got.Target)
}

func TestLoadStressStrictMinReady(t *testing.T) {
	t.Setenv("E2E_STRESS_MAX_VG_STRICT", "true")
	got, err := LoadStress()
	assert.NoError(t, err)
	assert.Equal(t, got.Target, got.MinReady)
}
