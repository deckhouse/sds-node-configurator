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

package udev

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestUdevDB(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "run", "udev", "data")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	return dir
}

// ================== ReadUdevDB ==================

func TestReadUdevDB_ValidFile(t *testing.T) {
	dir := newTestUdevDB(t)
	content := "N:sda\nS:disk/by-id/wwn-0x5000\nE:DEVNAME=/dev/sda\nE:ID_SERIAL_SHORT=WD-ABC\nE:ID_MODEL=WDC\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "b8:0"), []byte(content), 0o644))

	props, err := ReadUdevDB(dir, 8, 0)
	require.NoError(t, err)
	assert.Equal(t, "/dev/sda", props["DEVNAME"])
	assert.Equal(t, "WD-ABC", props["ID_SERIAL_SHORT"])
	assert.Equal(t, "WDC", props["ID_MODEL"])
}

func TestReadUdevDB_IgnoresNonELines(t *testing.T) {
	dir := newTestUdevDB(t)
	content := "N:sda\nS:disk/by-id/wwn\nI:12345\nE:DEVNAME=/dev/sda\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "b8:0"), []byte(content), 0o644))

	props, err := ReadUdevDB(dir, 8, 0)
	require.NoError(t, err)
	assert.Len(t, props, 1)
	assert.Equal(t, "/dev/sda", props["DEVNAME"])
}

func TestReadUdevDB_FileNotExist(t *testing.T) {
	dir := newTestUdevDB(t)
	_, err := ReadUdevDB(dir, 99, 99)
	assert.Error(t, err)
}

func TestReadUdevDB_EmptyFile(t *testing.T) {
	dir := newTestUdevDB(t)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "b8:0"), nil, 0o644))

	props, err := ReadUdevDB(dir, 8, 0)
	require.NoError(t, err)
	assert.Empty(t, props)
}

// ================== EnrichWithUdevDB ==================

func TestEnrichWithUdevDB_MergesAndEventWins(t *testing.T) {
	dir := newTestUdevDB(t)
	dbContent := "E:DEVNAME=/dev/sda\nE:ID_MODEL=OldModel\nE:ID_WWN=0xold\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "b8:0"), []byte(dbContent), 0o644))

	env := map[string]string{
		"MAJOR": "8", "MINOR": "0", "DEVNAME": "/dev/sda", "ID_MODEL": "NewModel",
	}
	merged, err := EnrichWithUdevDB(dir, env)
	require.NoError(t, err)
	assert.Equal(t, "NewModel", merged["ID_MODEL"], "event env wins over DB")
	assert.Equal(t, "0xold", merged["ID_WWN"], "DB value preserved when not in event")
}

func TestEnrichWithUdevDB_NoMajorMinor_ReturnsOriginal(t *testing.T) {
	env := map[string]string{"DEVNAME": "/dev/sda"}
	result, err := EnrichWithUdevDB("/nonexistent", env)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "MAJOR/MINOR missing")
	assert.Equal(t, env, result)
}

func TestEnrichWithUdevDB_InvalidMajor_ReturnsOriginal(t *testing.T) {
	env := map[string]string{"MAJOR": "abc", "MINOR": "0"}
	result, err := EnrichWithUdevDB("/nonexistent", env)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid MAJOR")
	assert.Equal(t, env, result)
}

func TestEnrichWithUdevDB_DBMissing_ReturnsOriginal(t *testing.T) {
	dir := newTestUdevDB(t)
	env := map[string]string{"MAJOR": "99", "MINOR": "99", "DEVNAME": "/dev/sda"}
	result, err := EnrichWithUdevDB(dir, env)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "merge failed")
	assert.Equal(t, env, result)
}

func TestEnrichWithUdevDB_DoesNotMutateOriginal(t *testing.T) {
	dir := newTestUdevDB(t)
	dbContent := "E:ID_WWN=0xabc\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "b8:0"), []byte(dbContent), 0o644))

	env := map[string]string{"MAJOR": "8", "MINOR": "0", "DEVNAME": "/dev/sda"}
	merged, err := EnrichWithUdevDB(dir, env)
	require.NoError(t, err)

	_, hasWWN := env["ID_WWN"]
	assert.False(t, hasWWN, "original env must not be mutated")
	assert.Equal(t, "0xabc", merged["ID_WWN"])
}
