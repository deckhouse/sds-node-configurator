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

package utils

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// The reservation channel is checked, never assumed, and it is checked by
// reading. Switching a pool to SCSI persistent reservations is a one-way door in
// the middle of its own procedure: `vgchange --setpersist require` makes the
// volume group unusable — every command answers "Persistent reservation is not
// started" — and it stays that way until `vgchange --persist start` succeeds. A
// pool taken through that on the assumption that the channel works, and finding
// out afterwards that it does not, is a pool nobody can use and nobody can
// easily undo.
//
// What this file establishes is only what a node can learn without writing to
// the array. It is not a promise that reservations will work; it is a refusal to
// find out the expensive way.

const (
	// MultipathToolsPRCeiling is the last version whose `reserve` returns the
	// truth.
	//
	// Measured across versions on a real array: 0.9.4, 0.9.7 and 0.9.9 return 0
	// from `--out --reserve --prout-type=7`; 0.10.0, 0.11.1 and 0.14.3 return 1.
	// The reservation is actually taken on the broken ones — only the exit code
	// is wrong — but lvmpersist reads that code, calls undo_register, and takes
	// the last registrant away with it. The reservation disappears, and from
	// outside the whole mechanism looks broken.
	//
	// The version cannot be worked around at run time: lvmpersist prepends the
	// system directories to PATH, so it always runs the binary from the image.
	MultipathToolsPRCeiling = "0.9.9"

	// ReasonMultipathToolsTooNew and the others are the machine-readable causes
	// a node publishes when the channel is not ready.
	ReasonMultipathToolsTooNew = "MultipathToolsTooNew"
	ReasonNoReservationKey     = "NoReservationKey"
	ReasonChannelUnreachable   = "ChannelUnreachable"
)

// reMultipathVersion matches what `mpathpersist -V` and friends print, e.g.
// "multipath-tools v0.9.4 (03/28, 2023)".
var reMultipathVersion = regexp.MustCompile(`v?(\d+)\.(\d+)\.(\d+)`)

// PRReadiness is a node's verdict on the reservation channel.
type PRReadiness struct {
	Ready   bool
	Reason  string
	Message string
}

// MultipathToolsVersionAtMostCeiling reports whether the version string names a
// release at or below the ceiling. An unparseable version is not a pass: the
// point of the check is that a wrong answer here is discovered only after the
// door has closed behind the pool.
func MultipathToolsVersionAtMostCeiling(version string) (bool, bool) {
	got := parseVersion(version)
	if got == nil {
		return false, false
	}
	ceiling := parseVersion(MultipathToolsPRCeiling)

	for i := range got {
		switch {
		case got[i] < ceiling[i]:
			return true, true
		case got[i] > ceiling[i]:
			return false, true
		}
	}
	return true, true
}

func parseVersion(s string) []int {
	m := reMultipathVersion.FindStringSubmatch(s)
	if m == nil {
		return nil
	}
	out := make([]int, 3)
	for i := range out {
		n, err := strconv.Atoi(m[i+1])
		if err != nil {
			return nil
		}
		out[i] = n
	}
	return out
}

// ReservationKeyConfigured reports whether multipathd has a reservation key for
// the map.
//
// Without one every reservation command is refused before it reaches the array —
// "<map>: no configured reservation key" — and the refusal reads like a broken
// mechanism rather than a missing setting. The key belongs in the pool's
// multipath drop-in, alongside no_path_retry, and it is written by whoever
// prepares the node: lvmpersist tries to repair the config itself by calling
// mpathconf, and mpathconf refuses to run in a container ("Running in chroot,
// ignoring request"), so self-healing from here is impossible by construction.
func ReservationKeyConfigured(getprkeyOutput string) bool {
	answer := strings.TrimSpace(getprkeyOutput)
	if answer == "" {
		return false
	}
	// multipathd answers "none" when the map has no key, and the key itself
	// otherwise.
	if strings.EqualFold(answer, "none") || strings.Contains(strings.ToLower(answer), "no configured reservation key") {
		return false
	}
	return true
}

// PRReadinessFrom turns what a node could read into the verdict it publishes.
//
// The order of the checks is the order in which their failures are cheapest to
// fix: a version in the image is a rebuild, a missing key is a line in a drop-in,
// and an unreachable channel is a pod that cannot see multipathd at all.
func PRReadinessFrom(version string, versionKnown bool, mapsWithoutKey []string, channelReadable bool) PRReadiness {
	if !channelReadable {
		return PRReadiness{
			Reason: ReasonChannelUnreachable,
			Message: "the reservation channel cannot be read from this node: mpathpersist talks to multipathd " +
				"over an abstract unix socket, which lives in a network namespace, so a pod without hostNetwork " +
				"never reaches the host's multipathd. Nothing about the array is known from here until that is fixed",
		}
	}

	if ok, known := MultipathToolsVersionAtMostCeiling(version); !known || !ok {
		got := version
		if !versionKnown || got == "" {
			got = "unknown"
		}
		return PRReadiness{
			Reason: ReasonMultipathToolsTooNew,
			Message: fmt.Sprintf("multipath-tools %s in the lock daemons' image cannot be used for reservations: "+
				"from 0.10.0 onwards `reserve` returns a non-zero exit code even though the reservation is taken, "+
				"and lvmpersist responds by undoing its own registration — which removes the last registrant and "+
				"with it the reservation. The last usable version is %s, and it cannot be substituted at run time "+
				"because lvmpersist prepends the system directories to PATH", got, MultipathToolsPRCeiling),
		}
	}

	if len(mapsWithoutKey) > 0 {
		return PRReadiness{
			Reason: ReasonNoReservationKey,
			Message: fmt.Sprintf("no reservation key is configured for %s. Every reservation command is refused "+
				"before it reaches the array while that is so. The key belongs in the pool's multipath drop-in "+
				"(`reservation_key file`), next to no_path_retry, and it has to be put there from outside: "+
				"lvmpersist repairs this by calling mpathconf, and mpathconf refuses to run in a container",
				strings.Join(mapsWithoutKey, ", ")),
		}
	}

	return PRReadiness{Ready: true}
}
