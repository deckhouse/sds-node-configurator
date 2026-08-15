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

package utils_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

func TestAMissingToolIsNamedRatherThanDiscoveredLater(t *testing.T) {
	// Every reservation command runs out of the lock daemons' image — lvm2
	// executes /sbin/lvmpersist by a path compiled into it, and lvmpersist runs
	// sg_persist. A pool taken through the switch without them fails in the
	// middle of the one-way door, with the group already unusable.
	v := utils.PRReadinessFrom([]string{"/usr/sbin/lvmpersist"}, nil, true)
	assert.False(t, v.Ready)
	assert.Equal(t, utils.ReasonReservationToolsMissing, v.Reason)
	assert.Contains(t, v.Message, "/usr/sbin/lvmpersist")
}

func TestAChannelThatCannotBeReadIsNotAPass(t *testing.T) {
	// Silence about the channel is not a verdict that it works.
	v := utils.PRReadinessFrom(nil, nil, false)
	assert.False(t, v.Ready)
	assert.Equal(t, utils.ReasonChannelUnreachable, v.Reason)
}

func TestAMapWithoutAKeyIsNotReady(t *testing.T) {
	assert.False(t, utils.ReservationKeyConfigured(""))
	assert.False(t, utils.ReservationKeyConfigured("none"))
	assert.False(t, utils.ReservationKeyConfigured("mpathi: no configured reservation key"))
	assert.True(t, utils.ReservationKeyConfigured("0x100000000001002d"))
}

func TestTheVerdictNamesTheCheapestFixFirst(t *testing.T) {
	// An unreachable channel says nothing about anything else, so it is reported
	// on its own: a pod that cannot see the host's multipathd knows nothing
	// about the array at all.
	v := utils.PRReadinessFrom([]string{"/usr/bin/sg_persist"}, []string{"mpathi"}, false)
	assert.False(t, v.Ready)
	assert.Equal(t, utils.ReasonChannelUnreachable, v.Reason)
	assert.Contains(t, v.Message, "hostNetwork")

	// Tooling that cannot be fixed without a rebuild outranks a key that the
	// node configuration puts in place by itself.
	v = utils.PRReadinessFrom([]string{"/usr/bin/sg_persist"}, []string{"mpathi"}, true)
	assert.Equal(t, utils.ReasonReservationToolsMissing, v.Reason)
	assert.Contains(t, v.Message, "/usr/bin/sg_persist")

	v = utils.PRReadinessFrom(nil, []string{"mpathi", "mpathj"}, true)
	assert.Equal(t, utils.ReasonNoReservationKey, v.Reason)
	assert.Contains(t, v.Message, "mpathi, mpathj")
	assert.Contains(t, v.Message, "reservation_key file")

	v = utils.PRReadinessFrom(nil, nil, true)
	assert.True(t, v.Ready)
	assert.Empty(t, v.Reason)
}
