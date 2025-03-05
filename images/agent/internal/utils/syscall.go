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
		uintptr(BLKDISCARD),
		uintptr(unsafe.Pointer(&rng)))

	if errno != 0 {
		err := errors.New(errno.Error())
		return fmt.Errorf("calling ioctl BLKDISCARD: %w", err)
	}
	return nil
}

/* To find these constant run:
gcc -o test -x c - <<EOF
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <linux/fs.h>
#include <stdio.h>

#define PRINT_CONSTANT(name, fmt) printf(#name " = " fmt "\n", name)

int main() {
    PRINT_CONSTANT(S_IFMT, "0x%x");
    PRINT_CONSTANT(S_IFBLK, "0x%x");
    PRINT_CONSTANT(BLKGETSIZE64, "0x%lx");
    PRINT_CONSTANT(BLKDISCARD, "0x%x");
    return 0;
}
EOF
*/

// TODO: It will be nice to figure them out during compilation or maybe runtime?
//
//nolint:revive
const (
	BLKDISCARD = 0x1277

	BLKGETSIZE64 = uintptr(0x80081272)

	S_IFMT  = 0xf000 /* type of file mask */
	S_IFBLK = 0x6000 /* block special */
)
