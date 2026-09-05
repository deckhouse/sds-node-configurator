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

//go:generate go tool mockgen -copyright_file ../../../../hack/boilerplate.mockgen.txt -write_source_comment -destination=../mock_utils/$GOFILE -source=$GOFILE
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

// SysCall is the syscall surface the agent needs, one method per operation.
//
// There is deliberately no generic Syscall(trap, a1, a2, a3 uintptr) here. An
// ioctl that reports back through a pointer has to pass that pointer as a
// uintptr, and the rule that keeps the pointed-to variable alive across the
// call (unsafe, "Pointer to uintptr, in the argument list of a call to
// syscall.Syscall") holds only inside the argument list of the syscall itself.
// Converted a call earlier - at a caller of this interface - the value is just
// a number: the variable stays on the stack, nothing keeps it in place, and a
// stack growth during the call moves it out from under the kernel's write. So
// every operation that hands the kernel a pointer is a method here, and does
// its own conversion below, next to unix.Syscall.
type SysCall interface {
	Fstat(fd int, stat *Stat_t) (err error)
	Blkgetsize64(fd uintptr) (size uint64, err error)
	Blksszget(fd uintptr) (blockSize int, err error)
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

func (osSyscall) Blkgetsize64(fd uintptr) (uint64, error) {
	var size uint64
	_, _, errno := unix.Syscall(
		unix.SYS_IOCTL,
		fd,
		unix.BLKGETSIZE64,
		uintptr(unsafe.Pointer(&size)))

	if errno != 0 {
		return 0, fmt.Errorf("calling ioctl BLKGETSIZE64: %w", errors.New(errno.Error()))
	}
	return size, nil
}

func (osSyscall) Blksszget(fd uintptr) (int, error) {
	var blockSize int
	_, _, errno := unix.Syscall(
		unix.SYS_IOCTL,
		fd,
		unix.BLKSSZGET,
		uintptr(unsafe.Pointer(&blockSize)))

	if errno != 0 {
		return 0, fmt.Errorf("calling ioctl BLKSSZGET: %w", errors.New(errno.Error()))
	}
	return blockSize, nil
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
