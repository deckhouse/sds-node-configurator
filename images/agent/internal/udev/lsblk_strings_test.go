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

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNormalizeWhitespace(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"", ""},
		{"a", "a"},
		{"  a", "a"},
		{"a  b", "a b"},
		{"a \t b", "a b"},
		{"a b ", "a b"},
		{"  a  b  ", "a b"},
		{"   ", ""},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			var got string
			if tt.in != "" {
				got = string(normalizeWhitespace([]byte(tt.in)))
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestUnhexmangle(t *testing.T) {
	assert.Equal(t, "ST330006 50NS", string(unhexmangle([]byte(`ST330006\x2050NS`))))
	assert.Equal(t, `no\xhex`, string(unhexmangle([]byte(`no\xhex`))))
	assert.Equal(t, "A", string(unhexmangle([]byte(`\x41`))))
}

func TestSerialFromUdevEnv_Chain(t *testing.T) {
	base := map[string]string{
		"ID_SERIAL_SHORT": "short",
		"ID_SERIAL":       "long",
	}
	assert.Equal(t, "short", serialFromUdevEnv(base))

	env := map[string]string{
		"ID_SCSI_SERIAL":  "scsi",
		"ID_SERIAL_SHORT": "short",
	}
	assert.Equal(t, "scsi", serialFromUdevEnv(env))

	env2 := map[string]string{
		"SCSI_IDENT_SERIAL": "sg",
		"ID_SCSI_SERIAL":    "scsi",
	}
	assert.Equal(t, "sg", serialFromUdevEnv(env2))
}

func TestWwnFromUdevEnv_PrefersExtension(t *testing.T) {
	env := map[string]string{
		"ID_WWN":                  "0x5000",
		"ID_WWN_WITH_EXTENSION":   "0x5000abc",
	}
	assert.Equal(t, "0x5000abc", wwnFromUdevEnv(env))
}

func TestModelFromUdevEnv_EncVersusPlain(t *testing.T) {
	env := map[string]string{
		"ID_MODEL":     "Plain",
		"ID_MODEL_ENC": `Foo\x20Bar`,
	}
	assert.Equal(t, "Foo Bar", modelFromUdevEnv(env))

	env2 := map[string]string{
		"ID_MODEL": "  two  spaces  ",
	}
	assert.Equal(t, "two spaces", modelFromUdevEnv(env2))
}
