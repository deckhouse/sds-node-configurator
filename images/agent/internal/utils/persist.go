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
		if key == "" {
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

// ReservationKeyConfigured reports whether the host's multipathd is configured
// to keep a reservation key at all.
//
// It reads the merged configuration rather than a map's current key, and the
// difference is the whole point: `getprkey` answers "none" for every map until
// something registers one, so a check on the key itself could never pass before
// the switch — which is exactly what it exists to gate. What has to be true
// beforehand is that multipathd knows where the key lives, because it is
// multipathd that re-registers a path when it comes back.
func ReservationKeyConfigured(config string) bool {
	for _, line := range strings.Split(config, "\n") {
		field := strings.TrimSpace(line)
		if !strings.HasPrefix(field, "reservation_key") {
			continue
		}
		value := strings.Trim(strings.TrimSpace(strings.TrimPrefix(field, "reservation_key")), `"`)
		if value != "" && value != "none" {
			return true
		}
	}
	return false
}

// KeyOfMap reads a map's current reservation key, which exists only once
// something has registered one. An empty or "none" answer is not a fault before
// a pool is switched — it is what an unregistered map says.
func KeyOfMap(answer string) string {
	answer = strings.TrimSpace(answer)
	// Case-insensitively: the tools do not agree on the spelling of a key, and
	// reading "0X…" as "no key" would have this module rewrite a key multipathd
	// already has, on every pass.
	if answer == "" || strings.EqualFold(answer, "none") || !strings.HasPrefix(strings.ToLower(answer), "0x") {
		return ""
	}
	return answer
}

// PRReadinessFrom turns what a node could read into the verdict it publishes.
//
// The order of the checks is the order in which their failures are cheapest to
// fix: a version in the image is a rebuild, a missing key is a line in a drop-in,
// and an unreachable channel is a pod that cannot see multipathd at all.
func PRReadinessFrom(toolsPresent []string, keyConfigured bool, channelReadable bool) PRReadiness {
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

	if !keyConfigured {
		return PRReadiness{
			Reason: ReasonNoReservationKey,
			Message: "this node's multipathd has no `reservation_key` in its configuration. Every reservation " +
				"command is refused before it reaches the array while that is so, and a path that comes back is " +
				"not re-registered — which leaves the node writing through a path the array no longer knows. " +
				"The module writes `reservation_key file` into /etc/multipath/conf.d itself, so a node without " +
				"it is a node that configuration has not reached yet",
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

// MultipathdAccepted reports whether multipathd took a command.
//
// It answers "ok" and nothing else on success, and prints its refusal — up to
// and including its entire CLI reference — with exit status 0. The exit status
// is not an answer.
func MultipathdAccepted(answer string) bool {
	return strings.EqualFold(strings.TrimSpace(answer), "ok")
}
