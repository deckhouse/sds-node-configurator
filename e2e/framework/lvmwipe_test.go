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

package framework

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The script is run rather than read. Asserting on its text can only catch a
// clumsy edit — it cannot tell whether the prefix filter actually holds, how many
// removal passes a three-level device-mapper chain really gets, or whether the
// sudo probe stops anything. Running it against stand-in lvm/dmsetup/sudo
// binaries answers all three, and it is the arguments they were called with that
// the assertions look at.
//
// The shims are ordinary shell scripts on a PATH of their own, so nothing here
// touches the machine running the tests.

// wipeHarness is a PATH with stand-in lvm, dmsetup and sudo on it, plus the file
// every invocation appends its argv to.
type wipeHarness struct {
	binDir   string
	callsLog string
	stateDir string
}

func newWipeHarness(t *testing.T) *wipeHarness {
	t.Helper()

	if _, err := exec.LookPath("bash"); err != nil {
		t.Skipf("bash is not available: %v", err)
	}

	dir := t.TempDir()
	h := &wipeHarness{
		binDir:   filepath.Join(dir, "bin"),
		callsLog: filepath.Join(dir, "calls"),
		stateDir: filepath.Join(dir, "state"),
	}
	require.NoError(t, os.MkdirAll(h.binDir, 0o755))
	require.NoError(t, os.MkdirAll(h.stateDir, 0o755))

	// sudo -n <cmd...> runs the command, so the shims below see the real argv.
	// "sudo -n true" therefore succeeds, which is the has-privileges case; the
	// no-privileges case replaces this one shim.
	h.writeShim(t, "sudo", `[ "$1" = "-n" ] && shift
exec "$@"`)

	return h
}

func (h *wipeHarness) writeShim(t *testing.T, name, body string) {
	t.Helper()

	script := "#!/usr/bin/env bash\n" +
		"printf '%s' \"" + name + "\" >> \"$CALLS\"\n" +
		"for a in \"$@\"; do printf ' %s' \"$a\" >> \"$CALLS\"; done\n" +
		"printf '\\n' >> \"$CALLS\"\n" +
		body + "\n"

	path := filepath.Join(h.binDir, name)
	require.NoError(t, os.WriteFile(path, []byte(script), 0o755))
}

// vgs makes the lvm shim report these Volume Group names, indented the way
// `lvm vgs --noheadings` indents them.
func (h *wipeHarness) vgs(t *testing.T, names ...string) {
	t.Helper()

	listing := ""
	for _, name := range names {
		listing += "  " + name + "\n"
	}
	require.NoError(t, os.WriteFile(filepath.Join(h.stateDir, "vgs"), []byte(listing), 0o644))

	// vgremove drops the name from the listing, so the report at the end of the
	// script shows what actually survived rather than what it started with.
	h.writeShim(t, "lvm", `case "$1" in
  vgs) cat "$STATE/vgs" ;;
  vgremove)
    for arg in "$@"; do :; done
    grep -v -x "  $arg" "$STATE/vgs" > "$STATE/vgs.new" || true
    mv "$STATE/vgs.new" "$STATE/vgs"
    ;;
esac`)
}

// dmNode is one device-mapper entry and the devices holding it open.
type dmNode struct {
	name string
	// heldBy names the devices that have this one open. It is removable only once
	// none of them is listed any more, which is what makes clearing a chain take one
	// pass per level.
	heldBy []string
}

// dm makes the dmsetup shim report these entries, in the order given.
//
// Ordered rather than a map, and the order is the point: `dmsetup ls` is not in
// dependency order, and how many passes a chain needs depends entirely on which
// end of it the listing starts from. A map would randomise that per run, so the
// test would pass against a script that only sometimes finishes the job.
func (h *wipeHarness) dm(t *testing.T, nodes ...dmNode) {
	t.Helper()

	listing, deps := "", ""
	for _, node := range nodes {
		listing += node.name + "\t(253:0)\n"
		deps += node.name + ":" + strings.Join(node.heldBy, ",") + "\n"
	}
	require.NoError(t, os.WriteFile(filepath.Join(h.stateDir, "dm"), []byte(listing), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(h.stateDir, "deps"), []byte(deps), 0o644))

	h.writeShim(t, "dmsetup", `case "$1" in
  ls) cat "$STATE/dm" ;;
  remove)
    for arg in "$@"; do :; done
    holders=$(grep "^$arg:" "$STATE/deps" | cut -d: -f2)
    for holder in ${holders//,/ }; do
      # Still listed means still holding this one open: refuse, the way dmsetup
      # refuses a device something has open.
      if cut -f1 "$STATE/dm" | grep -q -x "$holder"; then exit 1; fi
    done
    grep -v -P "^\Q$arg\E\t" "$STATE/dm" > "$STATE/dm.new" || true
    mv "$STATE/dm.new" "$STATE/dm"
    ;;
esac`)
}

func (h *wipeHarness) run(t *testing.T, keep ...string) (stdout string, exitCode int) {
	t.Helper()

	cmd := exec.Command("bash", "-c", keepPrelude(keep)+wipeE2ELVMScript)
	cmd.Env = append(os.Environ(),
		"PATH="+h.binDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"CALLS="+h.callsLog,
		"STATE="+h.stateDir,
	)

	out, err := cmd.Output()
	if exitErr := new(exec.ExitError); err != nil {
		require.ErrorAs(t, err, &exitErr, "the script must fail by exiting, not by failing to start")
		exitCode = exitErr.ExitCode()
	}

	return string(out), exitCode
}

func (h *wipeHarness) calls(t *testing.T) []string {
	t.Helper()

	raw, err := os.ReadFile(h.callsLog)
	if os.IsNotExist(err) {
		return nil
	}
	require.NoError(t, err)

	var lines []string
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if line != "" {
			lines = append(lines, line)
		}
	}

	return lines
}

// The property that matters is not what the wipe removes but what it refuses to
// touch: it runs as root on a cluster that carries storage the suite did not
// create. Asserting it on the text of the script proves the filter is spelled
// there; running it proves the filter holds.
func TestWipeE2ELVM_LeavesForeignStorageAlone(t *testing.T) {
	h := newWipeHarness(t)
	h.vgs(t, E2EVGNamePrefix+"restart-1", "prod-data", "vg-system")
	h.dm(t,
		dmNode{name: E2EDMNamePrefix + "restart--1-pool"},
		dmNode{name: "prod--data-lv"},
	)

	_, code := h.run(t)
	require.Zero(t, code)

	for _, call := range h.calls(t) {
		if !strings.Contains(call, "vgremove") && !strings.Contains(call, "remove") {
			continue
		}
		assert.NotContains(t, call, "prod", "a destructive command reached storage outside the suite's prefix: %s", call)
		assert.NotContains(t, call, "vg-system", "a destructive command reached storage outside the suite's prefix: %s", call)
	}

	assert.Contains(t, h.calls(t), "lvm vgremove -ff -y "+E2EVGNamePrefix+"restart-1",
		"and the suite's own group is still removed")
}

// A thin volume holds its pool and the pool holds its _tdata, so the worst
// ordering of `dmsetup ls` needs one removal pass per level. Two fixed passes
// cleared the pool over its _tdata and left the level below it on the node —
// which is the orphan this whole sweep exists to prevent.
func TestWipeE2ELVM_ClearsAThreeLevelDeviceMapperChain(t *testing.T) {
	h := newWipeHarness(t)
	h.vgs(t)

	var (
		pool  = E2EDMNamePrefix + "restart--1-pool"
		tdata = pool + "_tdata"
		thin  = E2EDMNamePrefix + "restart--1-volume"
	)
	// Listed worst-first: each device is visited before the one holding it open, so
	// every pass can free exactly one level and no more.
	h.dm(t,
		dmNode{name: tdata, heldBy: []string{pool}},
		dmNode{name: pool, heldBy: []string{thin}},
		dmNode{name: thin},
	)

	stdout, code := h.run(t)
	require.Zero(t, code)

	assert.Empty(t, strings.TrimSpace(stdout),
		"every level of the chain has to be gone, so nothing is reported as a leftover")
}

// A device nothing can remove must not spin the loop forever, and must be
// reported rather than silently tolerated.
func TestWipeE2ELVM_ReportsADeviceItCannotRemove(t *testing.T) {
	h := newWipeHarness(t)
	h.vgs(t, E2EVGNamePrefix+"stuck")

	stuck := E2EDMNamePrefix + "stuck-pool"
	// Held open by a device outside the suite's prefix. The wipe will not touch that
	// one, so the pool never becomes removable however many passes it gets.
	h.dm(t,
		dmNode{name: stuck, heldBy: []string{"someone--elses-lv"}},
		dmNode{name: "someone--elses-lv"},
	)
	// vgremove cannot take the group either.
	h.writeShim(t, "lvm", `case "$1" in
  vgs) cat "$STATE/vgs" ;;
  vgremove) exit 5 ;;
esac`)

	stdout, code := h.run(t)
	require.Zero(t, code, "the wipe is best-effort: it reports leftovers, it does not fail")

	assert.Contains(t, stdout, "vg:"+E2EVGNamePrefix+"stuck",
		"a Volume Group vgremove could not take has to be reported, or a failed wipe looks like a clean node")
	assert.Contains(t, stdout, "dm:"+stuck,
		"and so does a device-mapper node that survived")
}

// Every command in the script needs root and every one of them swallows its own
// failure, so a node where sudo does not work would otherwise report an empty
// leftovers list — indistinguishable from a node that is genuinely clean. The
// probe has to come first and it has to be fatal, or the wipe can silently not
// happen for a whole run.
func TestWipeE2ELVM_RefusesToRunWithoutSudo(t *testing.T) {
	h := newWipeHarness(t)
	h.vgs(t, E2EVGNamePrefix+"restart-1")
	h.dm(t, dmNode{name: E2EDMNamePrefix + "restart--1-pool"})

	// Passwordless sudo unavailable: every invocation fails, including the probe.
	h.writeShim(t, "sudo", `exit 1`)

	_, code := h.run(t)
	assert.Equal(t, wipeCannotRunExitCode, code,
		"the caller tells 'cannot be swept' from 'swept and something went wrong' by this code, not by the message")

	for _, call := range h.calls(t) {
		assert.NotContains(t, call, "vgremove", "nothing destructive may run after a failed privilege check: %s", call)
		assert.NotContains(t, call, "dmsetup remove", "nothing destructive may run after a failed privilege check: %s", call)
	}
}

// The other way a wipe cannot run, and the one that does not announce itself: the
// command is simply not there. `lvm` is not a given on a Deckhouse node — this
// module ships its own lvm.static under /opt/deckhouse/sds/bin and the host bundle
// need not carry one on PATH — and every listing in the script discards stderr and
// ends in `|| true`, so a missing binary produced an empty leftovers list and an
// exit code of zero. The caller reads that as "the node is clean", which is the
// state this whole sweep exists to stop accumulating unnoticed.
func TestWipeE2ELVM_RefusesToRunWithoutItsTools(t *testing.T) {
	for _, missing := range wipeE2ELVMTools {
		t.Run("no "+missing, func(t *testing.T) {
			h := newWipeHarness(t)
			h.vgs(t, E2EVGNamePrefix+"restart-1")
			h.dm(t, dmNode{name: E2EDMNamePrefix + "restart--1-pool"})

			// A shim that fails the way a missing command does, rather than deleting
			// the shim: the harness keeps the real PATH behind its own so that grep,
			// awk and the rest still work, and on a machine that happens to have lvm2
			// installed a deleted shim would simply fall through to the host's binary.
			// What the script sees either way is a probe that exits non-zero.
			h.writeShim(t, missing, `exit 127`)

			stdout, code := h.run(t)
			assert.Equal(t, wipeCannotRunExitCode, code,
				"a wipe that cannot run has to look different from a wipe that found nothing, and say so in the code WipeE2ELVM matches on")
			assert.Empty(t, strings.TrimSpace(stdout),
				"and it must not report a leftovers list it never gathered")

			for _, call := range h.calls(t) {
				assert.NotContains(t, call, "vgremove", "nothing destructive may run after a failed probe: %s", call)
				assert.NotContains(t, call, "dmsetup remove", "nothing destructive may run after a failed probe: %s", call)
			}
		})
	}
}

// Device-mapper doubles every hyphen in a Volume Group name, so the two prefixes
// have to stay in step: e2e-vg- appears as e2e--vg-- in dmsetup output. Deriving
// one from the other is what catches a hand edit of just one.
func TestE2EDMNamePrefixMatchesTheVGPrefix(t *testing.T) {
	assert.Equal(t, strings.ReplaceAll(E2EVGNamePrefix, "-", "--"), E2EDMNamePrefix)
}

// The suite's prefix separates it from everything else on the node, but not one
// run of it from another. A second run sweeping by prefix alone would take a
// running suite's Volume Groups out from under it, mid-spec, and that run would
// report it as the agent losing storage. An LVMVolumeGroup describing the group is
// what says somebody still wants it — so a kept name has to survive the sweep with
// its device-mapper nodes, and not be reported as a leftover either.
func TestWipeE2ELVM_LeavesAKeptVolumeGroupAlone(t *testing.T) {
	const (
		live  = E2EVGNamePrefix + "mine-1"
		stale = E2EVGNamePrefix + "stale-2"
	)

	h := newWipeHarness(t)
	h.vgs(t, live, stale)
	h.dm(t,
		dmNode{name: dmNameForVG(live) + "-pool"},
		dmNode{name: dmNameForVG(stale) + "-pool"},
	)

	out, code := h.run(t, live)
	require.Zero(t, code)

	for _, call := range h.calls(t) {
		if !strings.Contains(call, "remove") {
			continue
		}
		assert.NotContains(t, call, "mine", "a destructive command reached a Volume Group something still describes: %s", call)
	}

	assert.Contains(t, h.calls(t), "lvm vgremove -ff -y "+stale,
		"the leftover is still removed")
	assert.Contains(t, h.calls(t), "dmsetup remove --retry -f "+dmNameForVG(stale)+"-pool",
		"and so are its device-mapper nodes")

	assert.NotContains(t, out, live, "a Volume Group somebody still describes is not a leftover")
	assert.NotContains(t, out, dmNameForVG(live), "nor are its device-mapper nodes")
}

// A kept Volume Group must protect its own logical volumes and nothing else.
// Device-mapper spells <vg>-<lv> with a single hyphen and doubles every hyphen
// inside either half, so a name that continues past the kept one with a SECOND
// hyphen is a longer Volume Group name — a leftover — rather than a logical
// volume of the kept group.
//
// Keying the keep list on the separator alone did not distinguish the two, and
// the cost was not merely a leftover left behind: e2e_dms feeds the report at the
// end of the script as well as the removal loop, so the entry disappeared from
// both. WipeE2ELVM then returned nothing and both callers read that as "the node
// is clean" — the leftover survived the run and was invisible while it did.
//
// The names here are the ones from the incident: a live e2e-vg-restart beside the
// e2e-vg-restart-<runID> whose disk had already been detached.
func TestWipeE2ELVM_DoesNotMistakeALongerVGNameForALogicalVolume(t *testing.T) {
	const (
		live  = E2EVGNamePrefix + "restart"
		stale = E2EVGNamePrefix + "restart-1786799692"
	)

	h := newWipeHarness(t)
	// No Volume Groups left: the stale group's disk is gone, so LVM sees no PV and
	// only its device-mapper nodes survive. That is the case the report is the last
	// line of defence for.
	h.vgs(t)
	h.dm(t,
		dmNode{name: dmNameForVG(live) + "-pool"},
		dmNode{name: dmNameForVG(stale) + "-e2e--thin--pool"},
	)

	out, code := h.run(t, live)
	require.Zero(t, code)

	assert.Contains(t, h.calls(t), "dmsetup remove --retry -f "+dmNameForVG(stale)+"-e2e--thin--pool",
		"a different Volume Group's device-mapper node is not a logical volume of the kept one")

	for _, call := range h.calls(t) {
		if strings.Contains(call, "remove") {
			assert.NotContains(t, call, dmNameForVG(live)+"-pool",
				"a logical volume of the kept Volume Group must still be left alone: %s", call)
		}
	}

	assert.NotContains(t, out, dmNameForVG(live),
		"the kept group's own nodes are not leftovers")
}

// The keep list is the one place a value from the cluster reaches a shell command
// running as root. A name is data an LVMVolumeGroup's author chooses, so a name
// that could not be a Volume Group's is dropped rather than escaped — it cannot be
// protecting one.
func TestKeepPrelude_DropsANameThatCouldNotBeAVolumeGroup(t *testing.T) {
	prelude := keepPrelude([]string{
		"e2e-vg-good",
		"'; touch /tmp/pwned; echo '",
		"has space",
		"new\nline",
		"",
		"-leading-hyphen",
	})

	assert.Contains(t, prelude, "e2e-vg-good", "a usable name is kept")
	assert.Contains(t, prelude, dmNameForVG("e2e-vg-good"))

	for _, rejected := range []string{"touch", "has space", "new\nline", "-leading-hyphen"} {
		assert.NotContains(t, prelude, rejected, "an unusable name must not reach the script")
	}

	// And what is left is still two single-quoted assignments, one per list.
	assert.Equal(t, 4, strings.Count(prelude, "'"),
		"exactly the two pairs of quotes the prelude opens itself")
}

// The names go into the script as device-mapper spells them, which is what makes
// E2EDMNamePrefix the E2EVGNamePrefix of the dm world.
func TestDMNameForVG(t *testing.T) {
	assert.Equal(t, E2EDMNamePrefix, dmNameForVG(E2EVGNamePrefix))
	assert.Equal(t, "e2e--vg--restart--1786799692", dmNameForVG("e2e-vg-restart-1786799692"))
	assert.Equal(t, "nohyphens", dmNameForVG("nohyphens"))
}
