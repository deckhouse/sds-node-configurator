/*
Copyright 2025 Flant JSC

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

package udev

// normalizeWhitespace collapses runs of ASCII whitespace and trims leading /
// trailing space, matching util-linux __normalize_whitespace() / normalize_whitespace()
// in include/strutils.h (used by lsblk after udev properties).
func normalizeWhitespace(src []byte) []byte {
	sz := len(src)
	if sz == 0 {
		return nil
	}
	dst := make([]byte, sz+1)
	var i, x int
	nsp := 0
	intext := 0
	for i < sz && x < len(dst)-1 {
		b := src[i]
		isSpace := b == ' ' || b == '\t' || b == '\n' || b == '\v' || b == '\f' || b == '\r'
		if isSpace {
			nsp++
		} else {
			nsp = 0
			intext = 1
		}
		if nsp > 1 || (nsp > 0 && intext == 0) {
			i++
			continue
		}
		dst[x] = src[i]
		x++
		i++
	}
	if nsp > 0 && x > 0 {
		x--
	}
	return dst[:x]
}

func fromHex(c byte) byte {
	switch {
	case c >= '0' && c <= '9':
		return c - '0'
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10
	default:
		return 0
	}
}

func isXdigit(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

// unhexmangle decodes udev \\xHH sequences in place of the four-byte escape,
// matching unhexmangle_to_buffer() in util-linux lib/mangle.c (unhexmangle_string).
func unhexmangle(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	dst := make([]byte, 0, len(src))
	i := 0
	for i < len(src) {
		if i+3 < len(src) && src[i] == '\\' && src[i+1] == 'x' &&
			isXdigit(src[i+2]) && isXdigit(src[i+3]) {
			dst = append(dst, fromHex(src[i+2])<<4|fromHex(src[i+3]))
			i += 4
		} else {
			dst = append(dst, src[i])
			i++
		}
	}
	return dst
}

// normalizeUdevSerialOrModel applies lsblk's normalize_whitespace to a property string.
func normalizeUdevSerialOrModel(s string) string {
	if s == "" {
		return ""
	}
	out := normalizeWhitespace([]byte(s))
	return string(out)
}

// modelFromUdevEnv matches lsblk get_properties_by_udev: ID_MODEL_ENC (unhexmangle + normalize)
// or ID_MODEL + normalize.
func modelFromUdevEnv(env map[string]string) string {
	if enc := env["ID_MODEL_ENC"]; enc != "" {
		return string(normalizeWhitespace(unhexmangle([]byte(enc))))
	}
	if m := env["ID_MODEL"]; m != "" {
		return normalizeUdevSerialOrModel(m)
	}
	return ""
}

// serialFromUdevEnv matches lsblk: SCSI_IDENT_SERIAL → ID_SCSI_SERIAL → ID_SERIAL_SHORT → ID_SERIAL,
// then normalize_whitespace.
func serialFromUdevEnv(env map[string]string) string {
	var raw string
	switch {
	case env["SCSI_IDENT_SERIAL"] != "":
		raw = env["SCSI_IDENT_SERIAL"]
	case env["ID_SCSI_SERIAL"] != "":
		raw = env["ID_SCSI_SERIAL"]
	case env["ID_SERIAL_SHORT"] != "":
		raw = env["ID_SERIAL_SHORT"]
	default:
		raw = env["ID_SERIAL"]
	}
	return normalizeUdevSerialOrModel(raw)
}

// wwnFromUdevEnv matches lsblk: ID_WWN_WITH_EXTENSION, else ID_WWN (no normalization).
func wwnFromUdevEnv(env map[string]string) string {
	if env["ID_WWN_WITH_EXTENSION"] != "" {
		return env["ID_WWN_WITH_EXTENSION"]
	}
	return env["ID_WWN"]
}
