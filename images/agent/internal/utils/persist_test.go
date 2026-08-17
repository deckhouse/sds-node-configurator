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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

func TestAMissingToolIsNamedRatherThanDiscoveredLater(t *testing.T) {
	// Every reservation command runs out of the lock daemons' image — lvm2
	// executes /sbin/lvmpersist by a path compiled into it, and lvmpersist runs
	// sg_persist. A pool taken through the switch without them fails in the
	// middle of the one-way door, with the group already unusable.
	v := utils.PRReadinessFrom([]string{"/usr/sbin/lvmpersist"}, false, true)
	assert.False(t, v.Ready)
	assert.Equal(t, utils.ReasonReservationToolsMissing, v.Reason)
	assert.Contains(t, v.Message, "/usr/sbin/lvmpersist")
}

func TestAChannelThatCannotBeReadIsNotAPass(t *testing.T) {
	// Silence about the channel is not a verdict that it works.
	v := utils.PRReadinessFrom(nil, false, false)
	assert.False(t, v.Ready)
	assert.Equal(t, utils.ReasonChannelUnreachable, v.Reason)
}

func TestTheKeyIsCheckedInTheConfigurationAndNotOnTheMap(t *testing.T) {
	// `getprkey` answers "none" for every map until something registers a key,
	// so a check on the map could never pass before the switch — which is what
	// it exists to gate. What must be true beforehand is that multipathd knows
	// where the key lives, because multipathd is what re-registers a path that
	// comes back.
	assert.True(t, utils.ReservationKeyConfigured("\tprkeys_file \"/etc/multipath/prkeys\"\n\treservation_key \"file\"\n"))
	assert.True(t, utils.ReservationKeyConfigured("\treservation_key 0x123\n"))
	assert.False(t, utils.ReservationKeyConfigured("\tno_path_retry 4\n"))
	assert.False(t, utils.ReservationKeyConfigured("\treservation_key \"none\"\n"))

	// And a map that has not registered anything is not a fault, it is silence.
	assert.Empty(t, utils.KeyOfMap("none"))
	assert.Empty(t, utils.KeyOfMap(""))
	assert.Equal(t, "0x100000000001002d", utils.KeyOfMap(" 0x100000000001002d\n"))
}

func TestTheVerdictNamesTheCheapestFixFirst(t *testing.T) {
	// An unreachable channel says nothing about anything else, so it is reported
	// on its own: a pod that cannot see the host's multipathd knows nothing
	// about the array at all.
	v := utils.PRReadinessFrom([]string{"/usr/bin/sg_persist"}, false, false)
	assert.False(t, v.Ready)
	assert.Equal(t, utils.ReasonChannelUnreachable, v.Reason)
	assert.Contains(t, v.Message, "hostNetwork")

	// Tooling that cannot be fixed without a rebuild outranks a key that the
	// node configuration puts in place by itself.
	v = utils.PRReadinessFrom([]string{"/usr/bin/sg_persist"}, false, true)
	assert.Equal(t, utils.ReasonReservationToolsMissing, v.Reason)
	assert.Contains(t, v.Message, "/usr/bin/sg_persist")

	v = utils.PRReadinessFrom(nil, false, true)
	assert.Equal(t, utils.ReasonNoReservationKey, v.Reason)
	assert.Contains(t, v.Message, "reservation_key file")

	v = utils.PRReadinessFrom(nil, true, true)
	assert.True(t, v.Ready)
	assert.Empty(t, v.Reason)
}

func TestMultipathdAcceptanceIsTheWordOKAndNothingElse(t *testing.T) {
	// multipathd prints its refusal on stdout and exits zero, so the exit status
	// proves nothing. It answered a wrongly-spelled command with "not found"
	// followed by its whole CLI reference — which reads like a version
	// difference and is a missing word in the command.
	assert.True(t, utils.MultipathdAccepted("ok\n"))
	assert.True(t, utils.MultipathdAccepted("OK"))
	assert.False(t, utils.MultipathdAccepted(""))
	assert.False(t, utils.MultipathdAccepted("setprkey map mpathi 0x1 \n: not found\nmultipath-tools v0.9.4"))
}

func TestOnlySanlockKnowsWhetherTheLeaseIsAlive(t *testing.T) {
	// lvmlockd goes on listing a lockspace it registered long after sanlock has
	// stopped renewing it: on the stand a node whose registration had been taken
	// off the array still had "LS sanlock lvm_vghw" in lvmlockctl --info, while
	// lvm answered "lock skipped: storage errors for sanlock leases" and sanlock
	// listed no lockspace at all.
	held := "daemon abc.node\np -1 helper\np 1471677 lvmlockd\ns lvm_vghw:3:/dev/mapper/vghw-lvmlock:0\n"
	assert.True(t, utils.SanlockHoldsLockspace(held, "vghw"))

	lost := "daemon abc.node\np -1 helper\np 1060640 lvmlockd\np -1 status\n"
	assert.False(t, utils.SanlockHoldsLockspace(lost, "vghw"))

	// Another group's lockspace is not this one's.
	assert.False(t, utils.SanlockHoldsLockspace("s lvm_vgother:3:/dev/x:0\n", "vghw"))
	// And a prefix is not a name: "vghw" must not match "vghw2".
	assert.False(t, utils.SanlockHoldsLockspace("s lvm_vghw2:3:/dev/x:0\n", "vghw"))
}

func TestEverySharedCommandCarriesTheHostID(t *testing.T) {
	// For a sanlock group the lvm client resolves "our key" from local/host_id
	// and ignores local/pr_key entirely, so a command without it is answered
	// "Persistent reservation is not started. Cannot access VG" — with the node
	// registered on the array and its lockspace running. Found on the stand,
	// where every lvcreate failed on a pool that was otherwise healthy.
	dir := t.TempDir()

	// The file is written for lvmlockd's --host-id-file, in lvm's own syntax
	// rather than as a number — reading it as a number gets zero, and zero
	// leaves the option off every command.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "host-id"), []byte("host_id = 7\n"), 0o644))
	assert.Equal(t, 7, utils.HostIDFromStateDir(dir))
	assert.Equal(t, 7, utils.ParseHostIDFile("7\n"))
	assert.Equal(t, 7, utils.ParseHostIDFile("# written by the agent\nhost_id = 7\n"))
	assert.Zero(t, utils.ParseHostIDFile("host_id = 0\n"))
	assert.Zero(t, utils.ParseHostIDFile("nothing here\n"))

	// A node that has none answers zero, which leaves the option off the
	// command rather than putting a wrong id on it.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "host-id"), []byte("\n"), 0o644))
	assert.Zero(t, utils.HostIDFromStateDir(dir))
	require.NoError(t, os.Remove(filepath.Join(dir, "host-id")))
	assert.Zero(t, utils.HostIDFromStateDir(dir))
}

func TestALeaseMappingOfAnotherIncarnationIsRecognised(t *testing.T) {
	// A pool removed and created again under the same name gives its lease
	// volume the same device-mapper name and a different UUID. A member that
	// still has the old mapping cannot start the new lockspace at all: the
	// create fails with "Device or resource busy" once a minute, for as long as
	// the mapping stands. Found on the stand, on a pool healthy on its other two
	// nodes.
	root := t.TempDir()
	old := utils.SysBlockRoot
	utils.SysBlockRoot = root
	t.Cleanup(func() { utils.SysBlockRoot = old })

	base := filepath.Join(root, "dm-9")
	require.NoError(t, os.MkdirAll(filepath.Join(base, "dm"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(base, "dm", "name"), []byte("e2e--shared--pool-lvmlock\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(base, "dm", "uuid"),
		[]byte("LVM-5YmDCDfccS4MpuV0sCnE76fJPWdQoZUQ7rVgFI1Rv3pufbJZx05kQeP00ENOeLxU\n"), 0o644))

	// The group that has that name now is a different one.
	name, stale := utils.LeaseMappingOfOtherIncarnation("e2e-shared-pool", "KANMFg-8e8g-0tAj-THJE-whs0-cQj3-r7UStc")
	assert.True(t, stale)
	assert.Equal(t, "e2e--shared--pool-lvmlock", name)

	// The group it belongs to keeps it: device-mapper carries the volume
	// group's UUID with the dashes removed.
	_, stale = utils.LeaseMappingOfOtherIncarnation("e2e-shared-pool", "5YmDCD-fccS-4Mpu-V0sC-nE76-fJPW-dQoZUQ")
	assert.False(t, stale)

	// And an answer that cannot be read is not a stale mapping.
	_, stale = utils.LeaseMappingOfOtherIncarnation("other-pool", "5YmDCD-fccS")
	assert.False(t, stale)
}
