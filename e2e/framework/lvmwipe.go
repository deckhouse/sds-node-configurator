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
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/storage-e2e/pkg/e2e"
)

// E2EVGNamePrefix is the prefix every Volume Group the suite creates on a node
// carries. Device-mapper doubles the hyphens in a VG name, so the same groups
// appear in `dmsetup ls` under E2EDMNamePrefix.
const (
	E2EVGNamePrefix = "e2e-vg-"
	E2EDMNamePrefix = "e2e--vg--"
)

// dmNameForVG is how device-mapper spells a Volume Group's name: every hyphen
// doubled, which is what makes E2EDMNamePrefix the E2EVGNamePrefix of the dm
// world.
func dmNameForVG(vgName string) string {
	return strings.ReplaceAll(vgName, "-", "--")
}

// vgNamePattern is what LVM accepts in a Volume Group name.
//
// It is applied to every name that reaches the script, which is the one place a
// value from the cluster is interpolated into a shell command that runs as root.
// A name is data an LVMVolumeGroup's author chooses, so it is not this file's to
// trust — and a name that could not be a Volume Group's cannot be protecting one
// either, so dropping it loses nothing.
var vgNamePattern = regexp.MustCompile(`^[A-Za-z0-9+_.][A-Za-z0-9+_.-]*$`)

// wipeE2ELVMScript removes the suite's Volume Groups from a node and then the
// device-mapper nodes that survived them.
//
// It is prefixed with the KEEP_VGS and KEEP_DMS definitions keepPrelude builds,
// and every listing goes through e2e_vgs/e2e_dms, so the prefix filter and the
// keep list cannot be applied in three places and forgotten in the fourth.
//
// The second half is the one that matters. A device-mapper node outlives the
// disk underneath it: detach a disk carrying a Volume Group and the pool's dm
// entries stay, now pointing at a device that no longer exists. vgremove cannot
// clear those — LVM sees no PV to work with — so they have to be removed
// directly.
//
// The dm half loops until a pass removes nothing, rather than a fixed number of
// times. `dmsetup ls` is not in dependency order and the chain is deeper than it
// looks: a thin volume holds its pool, and the pool holds its _tdata and _tmeta,
// so the worst ordering needs one pass per level. Two passes covered the pool over
// its _tdata and left the rest — including any thin volume the suite created,
// whose dm name carries the same e2e--vg-- prefix — for nobody.
//
// --retry as well as -f. Without it, `dmsetup remove -f` on a device something
// still holds open replaces its table with one that fails all I/O and then fails
// to remove it, so the entry stays in `dmsetup ls` with its mapping destroyed:
// the loop sees no progress and the node keeps the orphan. The iteration cap is
// what keeps a device nothing can remove from spinning here forever; it is
// reported as a leftover instead.
//
// Every removal is best-effort. This runs in cleanup paths where the interesting
// failure has usually already happened, and failing the wipe would replace a
// useful error with a confusing one. What the script does report is whatever
// survived, so the caller can say the node is still dirty — both kinds, because
// it removes both. A vgremove that could not run leaves an e2e Volume Group on
// the node, and listing only the device-mapper nodes made that look clean the
// moment the dm entries went.
//
// Best-effort ends at the preflight checks keepPrelude puts in front of this.
// Every command here needs root and a binary to run, and every one of them is
// closed with `|| true` and a discarded stderr — so without those probes a node
// where `sudo -n` does not work, or where `lvm` is not on PATH, produces an empty
// leftovers list and an exit code of zero, which the caller reads as "the node is
// clean". A wipe that cannot run has to look different from a wipe that found
// nothing: this one exists precisely because leftovers accumulated unnoticed.
const wipeE2ELVMScript = `
e2e_vgs() {
  sudo -n lvm vgs --noheadings -o vg_name 2>/dev/null | tr -d ' ' | grep '^` + E2EVGNamePrefix + `' | grep -vxF "$KEEP_VGS" || true
}

e2e_dms() {
  sudo -n dmsetup ls 2>/dev/null | awk '{print $1}' | grep '^` + E2EDMNamePrefix + `' | awk -v keep="$KEEP_DMS" '
    BEGIN { n = split(keep, kept, "\n") }
    {
      for (i = 1; i <= n; i++) {
        if (kept[i] == "") continue
        if ($0 == kept[i]) next
        # A logical volume of the kept group, and only that. Device-mapper spells
        # <vg>-<lv> with a single hyphen and doubles every hyphen inside either
        # half, so one more hyphen after the separator is a longer VG NAME rather
        # than an LV of this one. Matching on the separator alone kept
        # e2e--vg--restart--<runID> under the kept e2e--vg--restart, which meant
        # a leftover was neither removed nor reported as one.
        if (index($0, kept[i] "-") == 1 && substr($0, length(kept[i]) + 2, 1) != "-") next
      }
      print
    }' || true
}

for vg in $(e2e_vgs); do
  sudo -n lvm vgremove -ff -y "$vg" >/dev/null 2>&1 || true
done

for _ in 1 2 3 4 5 6 7 8; do
  before=$(e2e_dms | wc -l)
  [ "$before" -eq 0 ] && break
  for dm in $(e2e_dms); do
    sudo -n dmsetup remove --retry -f "$dm" >/dev/null 2>&1 || true
  done
  after=$(e2e_dms | wc -l)
  [ "$after" -eq "$before" ] && break
done

e2e_vgs | sed 's/^/vg:/'
e2e_dms | sed 's/^/dm:/'
`

// wipeE2ELVMTools are the commands the script cannot do its job without. Both are
// probed before anything else runs — see keepPrelude.
var wipeE2ELVMTools = []string{"lvm", "dmsetup"}

// wipeCannotRunExitCode is what keepPrelude's probes exit with, and it is a
// distinct code rather than a bare failure so the caller can tell "this node
// cannot be swept" from "the sweep ran and something went wrong on this node".
const wipeCannotRunExitCode = 2

// ErrWipeCannotRun reports that the preflight probes refused: no passwordless
// sudo, or one of wipeE2ELVMTools missing. It is a property of how the run is
// configured rather than of the node, so every node of the cluster gives the same
// answer — which is why the caller is expected to say it once for the sweep
// instead of once per node.
var ErrWipeCannotRun = errors.New("the wipe cannot run on this cluster")

// keepPrelude defines the two lists the script filters by — the Volume Group names
// to leave alone and the same names as device-mapper spells them — and refuses to
// go on unless the wipe can actually be carried out.
//
// The lists are newline-separated and single-quoted, which is safe because
// vgNamePattern admits neither a newline nor a quote — a name that fails it is
// dropped rather than escaped, since it could not be naming a Volume Group in the
// first place.
//
// The probes exist because every command in the script is closed with `|| true`
// and a discarded stderr, so a wipe that cannot run produces an empty leftovers
// list and an exit code of zero — which the caller reads as "the node is clean".
// A wipe that cannot run has to look different from a wipe that found nothing,
// and there are two ways it cannot run:
//
//   - sudo does not work without a password;
//   - a command is not there. `lvm` in particular is not a given: this module
//     ships its own lvm.static under /opt/deckhouse/sds/bin and the host bundle
//     need not carry one on PATH.
//
// Both are checked through sudo, since that is how the script invokes them: a
// binary root can run and the calling user cannot would otherwise pass the probe
// and fail every real command afterwards.
func keepPrelude(keep []string) string {
	var vgs, dms []string
	for _, name := range keep {
		if !vgNamePattern.MatchString(name) {
			continue
		}

		vgs = append(vgs, name)
		dms = append(dms, dmNameForVG(name))
	}

	prelude := "set -u\n" +
		"if ! sudo -n true 2>/dev/null; then\n" +
		`  echo "cannot wipe: passwordless sudo is unavailable on this node" >&2` + "\n" +
		fmt.Sprintf("  exit %d\n", wipeCannotRunExitCode) +
		"fi\n"

	for _, tool := range wipeE2ELVMTools {
		prelude += "if ! sudo -n " + tool + " --version >/dev/null 2>&1; then\n" +
			`  echo "cannot wipe: ` + tool + ` is unavailable to sudo on this node" >&2` + "\n" +
			fmt.Sprintf("  exit %d\n", wipeCannotRunExitCode) +
			"fi\n"
	}

	return prelude +
		"KEEP_VGS='" + strings.Join(vgs, "\n") + "'\n" +
		"KEEP_DMS='" + strings.Join(dms, "\n") + "'\n"
}

// LiveVolumeGroupNames returns the Volume Group name on the node of every
// LVMVolumeGroup the cluster currently has.
//
// This is what tells a leftover from a Volume Group somebody is still using, and
// it is the reason WipeE2ELVM does not have to be told which run it belongs to. A
// Volume Group the suite created while a spec is running has an LVMVolumeGroup
// describing it; a leftover, by definition, does not — cleanupLVMVolumeGroups
// strips the finalizers off the ones the agent could not delete, which removes the
// resource and leaves the group on the node.
func LiveVolumeGroupNames(ctx context.Context, cl client.Client) ([]string, error) {
	var list v1alpha1.LVMVolumeGroupList
	if err := cl.List(ctx, &list); err != nil {
		return nil, fmt.Errorf("listing LVMVolumeGroups: %w", err)
	}

	names := make([]string, 0, len(list.Items))
	for i := range list.Items {
		if name := list.Items[i].Spec.ActualVGNameOnTheNode; name != "" {
			names = append(names, name)
		}
	}

	return names, nil
}

// WipeE2ELVM removes the suite's Volume Groups and any leftover device-mapper
// nodes from one node, and returns what survived — Volume Groups as "vg:<name>",
// device-mapper nodes as "dm:<name>".
//
// Both, because the script removes both and the caller reads an empty result as
// "the node is clean". Reporting only the device-mapper half made a failed
// vgremove — an open logical volume, a thin pool still in use — look like
// success as soon as the dm entries went, which leaves on the node exactly the
// artefact this whole sweep is about: an e2e Volume Group that outlives its run
// and strands the next one.
//
// keep names the Volume Groups it must not touch, with their device-mapper nodes.
// Pass LiveVolumeGroupNames: the suite's own prefix separates it from everything
// else on the node but not one run of it from another, so a sweep matching on the
// prefix alone would take a concurrently running suite's Volume Groups out from
// under it — mid-spec, and reported by that run as a bug in the agent, since all
// it sees is storage that vanished. The cluster lock the SDK takes does not close
// that: this suite releases it between specs on purpose, so another run's
// BeforeSuite can hold it while this one is between two specs of its own. An
// LVMVolumeGroup describing the group is what says somebody still wants it, and
// unlike a naming convention it is checked rather than agreed to.
//
// A kept Volume Group is not reported as a leftover. It is not one: something in
// the cluster still describes it, and the caller supplied that list, so it already
// knows.
//
// Call it before detaching a disk that carried a Volume Group. Skipping that is
// what leaves an orphan behind: the agent used to abort its whole block-device
// scan over one dm node whose parent disk was missing, which cost the node every
// BlockDevice it had. That is fixed, but an orphan is still a device the agent
// cannot describe, and the tidy state is the one worth leaving.
//
// It never removes anything outside the suite's own prefix, so it is safe on a
// cluster that carries storage the suite did not create.
func WipeE2ELVM(ctx context.Context, cl *e2e.Cluster, node string, keep []string) ([]string, error) {
	out, err := NodeExecChecked(ctx, cl, node, keepPrelude(keep)+wipeE2ELVMScript)
	if err != nil {
		var exit *ExitCodeError
		if errors.As(err, &exit) && exit.ExitCode == wipeCannotRunExitCode {
			return nil, fmt.Errorf("%w on node %s: %s", ErrWipeCannotRun, node, exit.Stderr)
		}

		return nil, fmt.Errorf("wiping e2e LVM on node %s: %w", node, err)
	}

	var leftovers []string
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if name := strings.TrimSpace(line); name != "" {
			leftovers = append(leftovers, name)
		}
	}

	return leftovers, nil
}
