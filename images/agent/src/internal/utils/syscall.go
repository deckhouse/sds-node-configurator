package utils

import (
	"errors"
	"fmt"
	"syscall"
	"unsafe"
)

type Stat_t syscall.Stat_t
type Errno syscall.Errno

type rangeUI64 struct {
	start, count uint64
}

func (e Errno) Error() string {
	return syscall.Errno(e).Error()
}

type SysCall interface {
	Fstat(fd int, stat *Stat_t) (err error)
	Syscall(trap, a1, a2, a3 uintptr) (r1, r2 uintptr, err Errno)
	Blkdiscard(fd uintptr, start, count uint64) error
}

type _syscall struct {
}

var theSysCall = _syscall{}

func DefaultSysCall() SysCall {
	return &theSysCall
}

func (_syscall) Fstat(fd int, stat *Stat_t) (err error) {
	return syscall.Fstat(fd, (*syscall.Stat_t)(stat))
}

func (_syscall) Syscall(trap, a1, a2, a3 uintptr) (r1, r2 uintptr, err Errno) {
	r1, r2, err_raw := syscall.Syscall(trap, a1, a2, a3)
	err = Errno(err_raw)
	return r1, r2, err
}

func (_syscall) Blkdiscard(fd uintptr, start, count uint64) error {
	rng := rangeUI64{
		start: start,
		count: count,
	}
	_, _, errno := syscall.Syscall(
		syscall.SYS_IOCTL,
		fd,
		uintptr(BLKDISCARD),
		uintptr(unsafe.Pointer(&rng)))

	if errno != 0 {
		err := errors.New(errno.Error())
		return fmt.Errorf("calling ioctl BLKDISCARD: %s", err)
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
    PRINT_CONSTANT(BLKSSZGET, "0x%x");
    PRINT_CONSTANT(BLKDISCARD, "0x%x");
    PRINT_CONSTANT(BLKDISCARDZEROES, "0x%x");
    PRINT_CONSTANT(BLKSECDISCARD, "0x%x");
    return 0;
}
EOF
*/

// TODO: It will be nice to figure them out during compilation or maybe runtime?
//
//nolint:revive
const (
	BLKDISCARD       = 0x1277
	BLKDISCARDZEROES = 0x127c
	BLKSECDISCARD    = 0x127d

	BLKGETSIZE64 = 0x80081272
	BLKSSZGET    = 0x1268

	S_IFMT  = 0xf000 /* type of file mask */
	S_IFBLK = 0x6000 /* block special */
)
