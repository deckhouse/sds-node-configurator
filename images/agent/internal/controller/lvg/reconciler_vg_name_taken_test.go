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

package lvg

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
)

func TestClassifyVGCreateFailure(t *testing.T) {
	const stderr = "unable to run cmd: nsenter vgcreate vg-a /dev/sdb, err: exit status 5, stderr:   /dev/vg-a: already exists in filesystem\n  New volume group name \"vg-a\" is invalid\n"

	t.Run("left /dev/<vg> gets its own reason and instructions", func(t *testing.T) {
		reason, msg := classifyVGCreateFailure(fmt.Errorf("create: %w", errors.New(stderr)), "vg-a", "node-1")

		assert.Equal(t, internal.ReasonVGNameTakenOnNode, reason)
		assert.Contains(t, msg, "/dev/vg-a exists on node node-1 but LVM sees no volume group vg-a")
		assert.Contains(t, msg, `"ls -l /dev/vg-a"`)
		assert.Contains(t, msg, `"rm -r /dev/vg-a"`)
	})

	t.Run("another volume group's path is not matched", func(t *testing.T) {
		reason, msg := classifyVGCreateFailure(errors.New(stderr), "vg-b", "node-1")

		assert.Equal(t, "VGCreationFailed", reason)
		assert.Contains(t, msg, "unable to create VG, err: ")
	})

	t.Run("every other error keeps VGCreationFailed", func(t *testing.T) {
		reason, msg := classifyVGCreateFailure(errors.New("Device /dev/sdb excluded by a filter"), "vg-a", "node-1")

		assert.Equal(t, "VGCreationFailed", reason)
		assert.Equal(t, "unable to create VG, err: Device /dev/sdb excluded by a filter", msg)
	})
}
