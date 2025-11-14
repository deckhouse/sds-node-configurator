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

//go:generate go tool mockgen -write_source_comment -destination=../mock_utils/$GOFILE -source=$GOFILE -copyright_file=../../../../hack/boilerplate.txt
package utils

import (
	"errors"
	"fmt"
	"unsafe"

	"golang.org/x/sys/unix"
)

//nolint:revive
type Stat_t = unix.Stat_t
type Errno = unix.Errno

type SysCall interface {
	Fstat(fd int, stat *Stat_t) (err error)
	Syscall(trap, a1, a2, a3 uintptr) (r1, r2 uintptr, err Errno)
	Blkdiscard(fd uintptr, start, count uint64) error
}

type osSyscall struct {
}

var theSysCall = osSyscall{}

//nolint:revive
func OsSysCall() osSyscall {
	return theSysCall
}

func (osSyscall) Fstat(fd int, stat *Stat_t) (err error) {
	return unix.Fstat(fd, stat)
}

func (osSyscall) Syscall(trap, a1, a2, a3 uintptr) (r1, r2 uintptr, err Errno) {
	return unix.Syscall(trap, a1, a2, a3)
}

func (osSyscall) Blkdiscard(fd uintptr, start, count uint64) error {
	rng := struct {
		start, count uint64
	}{
		start: start,
		count: count,
	}
	_, _, errno := unix.Syscall(
		unix.SYS_IOCTL,
		fd,
		BLKDISCARD,
		uintptr(unsafe.Pointer(&rng)))

	if errno != 0 {
		err := errors.New(errno.Error())
		return fmt.Errorf("calling ioctl BLKDISCARD: %w", err)
	}
	return nil
}

/* To find these constant missing from unix module:
gcc -o test -x c - <<EOF
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <linux/fs.h>
#include <stdio.h>

#define PRINT_CONSTANT(name, fmt) printf(#name " = " fmt "\n", name)

int main() {
    PRINT_CONSTANT(BLKDISCARD, "0x%x");
    return 0;
}
EOF
*/

// TODO: It will be nice to figure them out during compilation or maybe runtime?
//
//nolint:revive
const (
	BLKDISCARD = uintptr(0x1277)

	S_IFMT  = unix.S_IFMT  /* type of file mask */
	S_IFBLK = unix.S_IFBLK /* block special */
)
