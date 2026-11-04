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

package main

import (
	"os"
	"path/filepath"
	"testing"
)

func writeFile(t *testing.T, path, content string, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
}

func symlink(t *testing.T, target, path string) {
	t.Helper()
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
}

func assertSymlink(t *testing.T, path, want string) {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("%s is %s, want a symlink to %s", path, info.Mode(), want)
	}
	got, err := os.Readlink(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("%s -> %s, want -> %s", path, got, want)
	}
}

func newTree(t *testing.T) (src, dst string) {
	t.Helper()
	src, dst = t.TempDir(), t.TempDir()
	writeFile(t, filepath.Join(src, "bin", "pdata_tools"), "binary", 0o755)
	symlink(t, "pdata_tools", filepath.Join(src, "bin", "thin_check"))
	return src, dst
}

func TestCopyFilesRecursiveKeepsSymlinks(t *testing.T) {
	src, dst := newTree(t)

	if err := copyFilesRecursive(src, dst); err != nil {
		t.Fatal(err)
	}

	assertSymlink(t, filepath.Join(dst, "bin", "thin_check"), "pdata_tools")
	got, err := os.ReadFile(filepath.Join(dst, "bin", "thin_check"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "binary" {
		t.Fatalf("thin_check resolves to %q, want the content of pdata_tools", got)
	}
}

func TestCopyFilesRecursiveIsIdempotentForSymlinks(t *testing.T) {
	src, dst := newTree(t)

	for range 2 {
		if err := copyFilesRecursive(src, dst); err != nil {
			t.Fatal(err)
		}
	}

	assertSymlink(t, filepath.Join(dst, "bin", "thin_check"), "pdata_tools")
	if _, err := os.Lstat(filepath.Join(dst, "bin", "thin_check.tmp")); !os.IsNotExist(err) {
		t.Fatalf("thin_check.tmp is left behind: %v", err)
	}
}

func TestCopyFilesRecursiveReplacesWhatIsInTheWay(t *testing.T) {
	tests := map[string]func(t *testing.T, path string){
		"regular file": func(t *testing.T, path string) {
			writeFile(t, path, "a copy from an older installer", 0o755)
		},
		"symlink elsewhere": func(t *testing.T, path string) {
			symlink(t, "/usr/sbin/thin_check", path)
		},
		"dangling temporary link": func(t *testing.T, path string) {
			symlink(t, "nowhere", path+".tmp")
		},
	}

	for name, prepare := range tests {
		t.Run(name, func(t *testing.T) {
			src, dst := newTree(t)
			if err := os.MkdirAll(filepath.Join(dst, "bin"), 0o755); err != nil {
				t.Fatal(err)
			}
			prepare(t, filepath.Join(dst, "bin", "thin_check"))

			if err := copyFilesRecursive(src, dst); err != nil {
				t.Fatal(err)
			}

			assertSymlink(t, filepath.Join(dst, "bin", "thin_check"), "pdata_tools")
		})
	}
}
