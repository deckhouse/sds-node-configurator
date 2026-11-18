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

package udev

import (
	"fmt"
	"strings"
	"testing"
)

func hexmangle(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '\\' || c <= ' ' || c >= 0x7f {
			fmt.Fprintf(&b, `\x%02x`, c)
			continue
		}
		b.WriteByte(c)
	}
	return b.String()
}

func FuzzUnhexmangle(f *testing.F) {
	for _, seed := range []string{
		`ST330006\x2050NS`,
		`Vendor\x20Disk\x20Name`,
		"no escapes",
		"",
		`\xZZ`,
		`\x4`,
		`\x5c\x5c`,
		`\\x41`,
		`\x+1\x-1\x_1`,
		`QEMU\x20HARDDISK\x20\x20\x20`,
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, s string) {
		decoded := unhexmangle(s)

		if len(decoded) > len(s) {
			t.Fatalf("unhexmangle(%q) = %q is longer than its input", s, decoded)
		}
		if !strings.Contains(s, `\x`) && decoded != s {
			t.Fatalf("unhexmangle(%q) = %q, want the input back: it has no escapes", s, decoded)
		}

		mangled := hexmangle(s)
		if got := unhexmangle(mangled); got != s {
			t.Fatalf("unhexmangle(%q) = %q, want %q: udev encoded the value into it", mangled, got, s)
		}
	})
}
