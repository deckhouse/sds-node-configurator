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
	"strings"
	"testing"
)

func mangleMountInfoPath(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case ' ', '\t', '\n', '\\':
			fmt.Fprintf(&b, `\%03o`, c)
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

func FuzzDecodeMountInfoOctal(f *testing.F) {
	for _, seed := range []string{
		"/mnt/data",
		`\040`,
		`\011`,
		`\012`,
		`\134`,
		`path\`,
		`path\ab`,
		`/mnt/my\040data`,
		`/mnt/a\040b\011c\134d`,
		`\\134`,
		`\400\777`,
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, s string) {
		decoded := decodeMountInfoOctal(s)

		if len(decoded) > len(s) {
			t.Fatalf("decodeMountInfoOctal(%q) = %q is longer than its input", s, decoded)
		}
		if !strings.Contains(s, `\`) && decoded != s {
			t.Fatalf("decodeMountInfoOctal(%q) = %q, want the input back: it has no escapes", s, decoded)
		}

		mangled := mangleMountInfoPath(s)
		if got := decodeMountInfoOctal(mangled); got != s {
			t.Fatalf("decodeMountInfoOctal(%q) = %q, want %q: the kernel mangled the path into it", mangled, got, s)
		}
	})
}
