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
	// ReasonReservationToolsMissing and the others are the machine-readable
	// causes a node publishes when the channel is not ready.
	//
	// There is no version ceiling among them any more. The branch used to carry
	// one — multipath-tools 0.9.9, because `mpathpersist --out --reserve`
	// returns 1 from 0.10.0 onwards even though the reservation is taken, and
	// lvmpersist undoes its own registration on that code — but nothing here
	// calls mpathpersist now. The reservation commands go through sg_persist,
	// one path at a time, which is what the array requires of a preempt anyway.
	ReasonReservationToolsMissing = "ReservationToolsMissing"
	ReasonNoReservationKey        = "NoReservationKey"
	ReasonChannelUnreachable      = "ChannelUnreachable"
)

// PRReadiness is a node's verdict on the reservation channel.
type PRReadiness struct {
	Ready   bool
	Reason  string
	Message string

	// Key is this node's reservation key, and it is set only when every LUN of
	// the pool reports the same one. See NodePersistentReservations.Key.
	Key string
}

// SingleReservationKey returns the key the maps agree on, and nothing when they
// do not.
//
// Disagreement is not expected — lvm2 derives one key per host from its sanlock
// host id and writes it for every map of the group — but "not expected" is not a
// reason to publish one of several keys as though it were the node's. The
// published key is what a neighbour would fence with.
func SingleReservationKey(keys []string) string {
	var single string
	for _, key := range keys {
		if !ReservationKeyConfigured(key) {
			return ""
		}
		if single == "" {
			single = key
			continue
		}
		if !SameRegistrationKey(single, key) {
			return ""
		}
	}
	return single
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
func PRReadinessFrom(toolsPresent []string, mapsWithoutKey []string, channelReadable bool) PRReadiness {
	if !channelReadable {
		return PRReadiness{
			Reason: ReasonChannelUnreachable,
			Message: "the reservation channel cannot be read from this node: multipathd is reached over an " +
				"abstract unix socket, which lives in a network namespace, so a pod without hostNetwork never " +
				"reaches the host's multipathd. Nothing about the array is known from here until that is fixed",
		}
	}

	if len(toolsPresent) > 0 {
		return PRReadiness{
			Reason: ReasonReservationToolsMissing,
			Message: fmt.Sprintf("the lock daemons' image is missing %s. Every reservation command runs from that "+
				"image — lvm2 executes /sbin/lvmpersist by a path compiled into it, and lvmpersist runs sg_persist "+
				"— so nothing of this can work until the image carries them. This is a build regression rather "+
				"than anything to be fixed on the node", strings.Join(toolsPresent, ", ")),
		}
	}

	if len(mapsWithoutKey) > 0 {
		return PRReadiness{
			Reason: ReasonNoReservationKey,
			Message: fmt.Sprintf("no reservation key is configured for %s. Every reservation command is refused "+
				"before it reaches the array while that is so, and a path that comes back is not re-registered. "+
				"The key belongs in the node's multipath configuration (`reservation_key file`), which the module "+
				"writes there itself — a map without one means that configuration did not reach this node",
				strings.Join(mapsWithoutKey, ", ")),
		}
	}

	return PRReadiness{Ready: true}
}

// ParseRegistrationKeys reads the keys out of what sg_persist prints.
//
// Its output is a header and then one key per line, indented:
//
//	PR generation=0x2, 3 registered reservation keys follow:
//	    0x100000000001002d
//	    0x100000000002002e
//
// Anything that is not a hexadecimal key is skipped rather than guessed at: this
// list decides which node gets its access to a LUN taken away, and a
// misread line there is a node evicted for nothing.
func ParseRegistrationKeys(output string) []string {
	var keys []string
	for _, line := range strings.Split(output, "\n") {
		field := strings.TrimSpace(line)
		if !strings.HasPrefix(field, "0x") || strings.ContainsAny(field, " \t") {
			continue
		}
		if _, err := strconv.ParseUint(strings.TrimPrefix(field, "0x"), 16, 64); err != nil {
			continue
		}
		keys = append(keys, field)
	}
	return keys
}

// SameRegistrationKey compares keys as numbers, because the array and the tools
// do not agree on spelling: 0x100000000001002D and 0x100000000001002d are one
// key, and a string comparison would evict a node that is already gone while
// leaving the one that is still writing.
func SameRegistrationKey(a, b string) bool {
	na, erra := strconv.ParseUint(strings.TrimPrefix(strings.ToLower(a), "0x"), 16, 64)
	nb, errb := strconv.ParseUint(strings.TrimPrefix(strings.ToLower(b), "0x"), 16, 64)
	if erra != nil || errb != nil {
		return false
	}
	return na == nb
}
