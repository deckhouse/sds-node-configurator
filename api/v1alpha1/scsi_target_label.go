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

package v1alpha1

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	scsiTargetLabelHashLength   = 16
	scsiTargetLabelPrefixLength = validation.LabelValueMaxLength - scsiTargetLabelHashLength - 1
)

// SCSITargetLabelValue turns the identity of a SCSI target into the value of
// BlockDeviceSCSITargetLabelKey. The identity is an iSCSI name (iqn., eui. or
// naa. form) or the WWPN of a Fibre Channel target port.
//
// The agent writes the label with it and a CSI driver that hides the LUNs of
// its targets selects by it, so both sides must call this function rather than
// derive the value themselves.
//
// A WWPN in any of its usual spellings (0x500a0981891b8dc5,
// 50:0a:09:81:89:1b:8d:c5) becomes its 16 lower-case hex digits. An iSCSI name
// is lower-cased, since iSCSI names are case-insensitive, and its colons become
// underscores: a label value cannot hold a colon, and an iSCSI name cannot hold
// an underscore, so the mapping stays one-to-one. A name that is still not a
// valid label value after that — longer than 63 characters, or ending in
// something other than a letter or digit — is cut to its first 46 valid
// characters, followed by a dash and the first 16 hex digits of the SHA-256 of
// the whole lower-cased name.
func SCSITargetLabelValue(identity string) string {
	name := strings.ToLower(strings.TrimSpace(identity))
	if name == "" {
		return ""
	}

	if wwpn, ok := canonicalWWPN(name); ok {
		return wwpn
	}

	value := strings.ReplaceAll(name, ":", "_")
	if len(validation.IsValidLabelValue(value)) == 0 {
		return value
	}

	sum := sha256.Sum256([]byte(name))
	hash := hex.EncodeToString(sum[:])[:scsiTargetLabelHashLength]

	prefix := strings.Map(func(r rune) rune {
		if isAlphanumeric(r) || r == '-' || r == '_' || r == '.' {
			return r
		}
		return -1
	}, value)
	if len(prefix) > scsiTargetLabelPrefixLength {
		prefix = prefix[:scsiTargetLabelPrefixLength]
	}
	prefix = strings.TrimFunc(prefix, func(r rune) bool { return !isAlphanumeric(r) })
	if prefix == "" {
		return hash
	}

	return prefix + "-" + hash
}

// canonicalWWPN reports whether name is a WWPN and returns its 16 hex digits.
// An iSCSI name always carries a dot, so it is never taken for one.
func canonicalWWPN(name string) (string, bool) {
	digits := strings.ReplaceAll(strings.TrimPrefix(name, "0x"), ":", "")
	if len(digits) != 16 {
		return "", false
	}
	for _, r := range digits {
		if !strings.ContainsRune("0123456789abcdef", r) {
			return "", false
		}
	}
	return digits, true
}

func isAlphanumeric(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
}
