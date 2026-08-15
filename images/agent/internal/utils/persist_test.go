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

func TestTheVersionsThatBreakReservationsAreTheOnesMeasured(t *testing.T) {
	// Measured across versions on a real array: `--out --reserve --prout-type=7`
	// returns 0 on 0.9.4, 0.9.7 and 0.9.9, and 1 on 0.10.0, 0.11.1 and 0.14.3.
	// The reservation is taken on the broken ones and only the exit code lies —
	// but lvmpersist reads that code, undoes its own registration, and the last
	// registrant leaving takes the reservation with it.
	for _, good := range []string{"multipath-tools v0.9.4 (03/28, 2023)", "v0.9.7", "0.9.9"} {
		ok, known := utils.MultipathToolsVersionAtMostCeiling(good)
		assert.True(t, known, "%q should parse", good)
		assert.True(t, ok, "%q is at or below the ceiling", good)
	}

	for _, bad := range []string{"v0.10.0", "multipath-tools v0.11.1", "0.14.3"} {
		ok, known := utils.MultipathToolsVersionAtMostCeiling(bad)
		assert.True(t, known, "%q should parse", bad)
		assert.False(t, ok, "%q returns the wrong exit code for reserve", bad)
	}
}

func TestAVersionThatCannotBeReadIsNotAPass(t *testing.T) {
	// The whole point of checking is that a wrong answer is discovered after the
	// door has closed behind the pool.
	ok, known := utils.MultipathToolsVersionAtMostCeiling("mpathpersist: command not found")
	assert.False(t, known)
	assert.False(t, ok)
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
	v := utils.PRReadinessFrom("v0.9.4", true, []string{"mpathi"}, false)
	assert.False(t, v.Ready)
	assert.Equal(t, utils.ReasonChannelUnreachable, v.Reason)
	assert.Contains(t, v.Message, "hostNetwork")

	// A version that cannot be fixed without a rebuild outranks a key that can be
	// fixed with a line in a drop-in.
	v = utils.PRReadinessFrom("v0.14.3", true, []string{"mpathi"}, true)
	assert.Equal(t, utils.ReasonMultipathToolsTooNew, v.Reason)
	assert.Contains(t, v.Message, "0.9.9")

	v = utils.PRReadinessFrom("v0.9.9", true, []string{"mpathi", "mpathj"}, true)
	assert.Equal(t, utils.ReasonNoReservationKey, v.Reason)
	assert.Contains(t, v.Message, "mpathi, mpathj")
	assert.Contains(t, v.Message, "reservation_key file")

	v = utils.PRReadinessFrom("v0.9.9", true, nil, true)
	assert.True(t, v.Ready)
	assert.Empty(t, v.Reason)
}
