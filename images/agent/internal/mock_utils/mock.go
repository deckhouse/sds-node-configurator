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

// Package mock_utils is a generated GoMock package.
package mock_utils

import (
	fs "io/fs"
	reflect "reflect"
	syscall "syscall"

	gomock "go.uber.org/mock/gomock"
	unix "golang.org/x/sys/unix"

	utils "github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

//go:generate mockgen -write_generate_directive -typed github.com/deckhouse/sds-node-configurator/images/agent/internal/utils SysCall,BlockDevice,File,FileOpener,BlockDeviceOpener

// MockSysCall is a mock of SysCall interface.
type MockSysCall struct {
	ctrl     *gomock.Controller
	recorder *MockSysCallMockRecorder
	isgomock struct{}
}

// MockSysCallMockRecorder is the mock recorder for MockSysCall.
type MockSysCallMockRecorder struct {
	mock *MockSysCall
}

// NewMockSysCall creates a new mock instance.
func NewMockSysCall(ctrl *gomock.Controller) *MockSysCall {
	mock := &MockSysCall{ctrl: ctrl}
	mock.recorder = &MockSysCallMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockSysCall) EXPECT() *MockSysCallMockRecorder {
	return m.recorder
}

// Blkdiscard mocks base method.
func (m *MockSysCall) Blkdiscard(fd uintptr, start, count uint64) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Blkdiscard", fd, start, count)
	ret0, _ := ret[0].(error)
	return ret0
}

// Blkdiscard indicates an expected call of Blkdiscard.
func (mr *MockSysCallMockRecorder) Blkdiscard(fd, start, count any) *MockSysCallBlkdiscardCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Blkdiscard", reflect.TypeOf((*MockSysCall)(nil).Blkdiscard), fd, start, count)
	return &MockSysCallBlkdiscardCall{Call: call}
}

// MockSysCallBlkdiscardCall wrap *gomock.Call
type MockSysCallBlkdiscardCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockSysCallBlkdiscardCall) Return(arg0 error) *MockSysCallBlkdiscardCall {
	c.Call = c.Call.Return(arg0)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockSysCallBlkdiscardCall) Do(f func(uintptr, uint64, uint64) error) *MockSysCallBlkdiscardCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockSysCallBlkdiscardCall) DoAndReturn(f func(uintptr, uint64, uint64) error) *MockSysCallBlkdiscardCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// Fstat mocks base method.
func (m *MockSysCall) Fstat(fd int, stat *unix.Stat_t) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Fstat", fd, stat)
	ret0, _ := ret[0].(error)
	return ret0
}

// Fstat indicates an expected call of Fstat.
func (mr *MockSysCallMockRecorder) Fstat(fd, stat any) *MockSysCallFstatCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Fstat", reflect.TypeOf((*MockSysCall)(nil).Fstat), fd, stat)
	return &MockSysCallFstatCall{Call: call}
}

// MockSysCallFstatCall wrap *gomock.Call
type MockSysCallFstatCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockSysCallFstatCall) Return(err error) *MockSysCallFstatCall {
	c.Call = c.Call.Return(err)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockSysCallFstatCall) Do(f func(int, *unix.Stat_t) error) *MockSysCallFstatCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockSysCallFstatCall) DoAndReturn(f func(int, *unix.Stat_t) error) *MockSysCallFstatCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// Syscall mocks base method.
func (m *MockSysCall) Syscall(trap, a1, a2, a3 uintptr) (uintptr, uintptr, syscall.Errno) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Syscall", trap, a1, a2, a3)
	ret0, _ := ret[0].(uintptr)
	ret1, _ := ret[1].(uintptr)
	ret2, _ := ret[2].(syscall.Errno)
	return ret0, ret1, ret2
}

// Syscall indicates an expected call of Syscall.
func (mr *MockSysCallMockRecorder) Syscall(trap, a1, a2, a3 any) *MockSysCallSyscallCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Syscall", reflect.TypeOf((*MockSysCall)(nil).Syscall), trap, a1, a2, a3)
	return &MockSysCallSyscallCall{Call: call}
}

// MockSysCallSyscallCall wrap *gomock.Call
type MockSysCallSyscallCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockSysCallSyscallCall) Return(r1, r2 uintptr, err syscall.Errno) *MockSysCallSyscallCall {
	c.Call = c.Call.Return(r1, r2, err)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockSysCallSyscallCall) Do(f func(uintptr, uintptr, uintptr, uintptr) (uintptr, uintptr, syscall.Errno)) *MockSysCallSyscallCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockSysCallSyscallCall) DoAndReturn(f func(uintptr, uintptr, uintptr, uintptr) (uintptr, uintptr, syscall.Errno)) *MockSysCallSyscallCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// MockBlockDevice is a mock of BlockDevice interface.
type MockBlockDevice struct {
	ctrl     *gomock.Controller
	recorder *MockBlockDeviceMockRecorder
	isgomock struct{}
}

// MockBlockDeviceMockRecorder is the mock recorder for MockBlockDevice.
type MockBlockDeviceMockRecorder struct {
	mock *MockBlockDevice
}

// NewMockBlockDevice creates a new mock instance.
func NewMockBlockDevice(ctrl *gomock.Controller) *MockBlockDevice {
	mock := &MockBlockDevice{ctrl: ctrl}
	mock.recorder = &MockBlockDeviceMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockBlockDevice) EXPECT() *MockBlockDeviceMockRecorder {
	return m.recorder
}

// BlockSize mocks base method.
func (m *MockBlockDevice) BlockSize() (int, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "BlockSize")
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// BlockSize indicates an expected call of BlockSize.
func (mr *MockBlockDeviceMockRecorder) BlockSize() *MockBlockDeviceBlockSizeCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "BlockSize", reflect.TypeOf((*MockBlockDevice)(nil).BlockSize))
	return &MockBlockDeviceBlockSizeCall{Call: call}
}

// MockBlockDeviceBlockSizeCall wrap *gomock.Call
type MockBlockDeviceBlockSizeCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockBlockDeviceBlockSizeCall) Return(arg0 int, arg1 error) *MockBlockDeviceBlockSizeCall {
	c.Call = c.Call.Return(arg0, arg1)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockBlockDeviceBlockSizeCall) Do(f func() (int, error)) *MockBlockDeviceBlockSizeCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockBlockDeviceBlockSizeCall) DoAndReturn(f func() (int, error)) *MockBlockDeviceBlockSizeCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// Close mocks base method.
func (m *MockBlockDevice) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockBlockDeviceMockRecorder) Close() *MockBlockDeviceCloseCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockBlockDevice)(nil).Close))
	return &MockBlockDeviceCloseCall{Call: call}
}

// MockBlockDeviceCloseCall wrap *gomock.Call
type MockBlockDeviceCloseCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockBlockDeviceCloseCall) Return(arg0 error) *MockBlockDeviceCloseCall {
	c.Call = c.Call.Return(arg0)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockBlockDeviceCloseCall) Do(f func() error) *MockBlockDeviceCloseCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockBlockDeviceCloseCall) DoAndReturn(f func() error) *MockBlockDeviceCloseCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// Discard mocks base method.
func (m *MockBlockDevice) Discard(start, count uint64) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Discard", start, count)
	ret0, _ := ret[0].(error)
	return ret0
}

// Discard indicates an expected call of Discard.
func (mr *MockBlockDeviceMockRecorder) Discard(start, count any) *MockBlockDeviceDiscardCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Discard", reflect.TypeOf((*MockBlockDevice)(nil).Discard), start, count)
	return &MockBlockDeviceDiscardCall{Call: call}
}

// MockBlockDeviceDiscardCall wrap *gomock.Call
type MockBlockDeviceDiscardCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockBlockDeviceDiscardCall) Return(arg0 error) *MockBlockDeviceDiscardCall {
	c.Call = c.Call.Return(arg0)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockBlockDeviceDiscardCall) Do(f func(uint64, uint64) error) *MockBlockDeviceDiscardCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockBlockDeviceDiscardCall) DoAndReturn(f func(uint64, uint64) error) *MockBlockDeviceDiscardCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// Fd mocks base method.
func (m *MockBlockDevice) Fd() uintptr {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Fd")
	ret0, _ := ret[0].(uintptr)
	return ret0
}

// Fd indicates an expected call of Fd.
func (mr *MockBlockDeviceMockRecorder) Fd() *MockBlockDeviceFdCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Fd", reflect.TypeOf((*MockBlockDevice)(nil).Fd))
	return &MockBlockDeviceFdCall{Call: call}
}

// MockBlockDeviceFdCall wrap *gomock.Call
type MockBlockDeviceFdCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockBlockDeviceFdCall) Return(arg0 uintptr) *MockBlockDeviceFdCall {
	c.Call = c.Call.Return(arg0)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockBlockDeviceFdCall) Do(f func() uintptr) *MockBlockDeviceFdCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockBlockDeviceFdCall) DoAndReturn(f func() uintptr) *MockBlockDeviceFdCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// Name mocks base method.
func (m *MockBlockDevice) Name() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Name")
	ret0, _ := ret[0].(string)
	return ret0
}

// Name indicates an expected call of Name.
func (mr *MockBlockDeviceMockRecorder) Name() *MockBlockDeviceNameCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Name", reflect.TypeOf((*MockBlockDevice)(nil).Name))
	return &MockBlockDeviceNameCall{Call: call}
}

// MockBlockDeviceNameCall wrap *gomock.Call
type MockBlockDeviceNameCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockBlockDeviceNameCall) Return(arg0 string) *MockBlockDeviceNameCall {
	c.Call = c.Call.Return(arg0)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockBlockDeviceNameCall) Do(f func() string) *MockBlockDeviceNameCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockBlockDeviceNameCall) DoAndReturn(f func() string) *MockBlockDeviceNameCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// Read mocks base method.
func (m *MockBlockDevice) Read(p []byte) (int, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Read", p)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Read indicates an expected call of Read.
func (mr *MockBlockDeviceMockRecorder) Read(p any) *MockBlockDeviceReadCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Read", reflect.TypeOf((*MockBlockDevice)(nil).Read), p)
	return &MockBlockDeviceReadCall{Call: call}
}

// MockBlockDeviceReadCall wrap *gomock.Call
type MockBlockDeviceReadCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockBlockDeviceReadCall) Return(n int, err error) *MockBlockDeviceReadCall {
	c.Call = c.Call.Return(n, err)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockBlockDeviceReadCall) Do(f func([]byte) (int, error)) *MockBlockDeviceReadCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockBlockDeviceReadCall) DoAndReturn(f func([]byte) (int, error)) *MockBlockDeviceReadCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// ReadAt mocks base method.
func (m *MockBlockDevice) ReadAt(p []byte, off int64) (int, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReadAt", p, off)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ReadAt indicates an expected call of ReadAt.
func (mr *MockBlockDeviceMockRecorder) ReadAt(p, off any) *MockBlockDeviceReadAtCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReadAt", reflect.TypeOf((*MockBlockDevice)(nil).ReadAt), p, off)
	return &MockBlockDeviceReadAtCall{Call: call}
}

// MockBlockDeviceReadAtCall wrap *gomock.Call
type MockBlockDeviceReadAtCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockBlockDeviceReadAtCall) Return(n int, err error) *MockBlockDeviceReadAtCall {
	c.Call = c.Call.Return(n, err)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockBlockDeviceReadAtCall) Do(f func([]byte, int64) (int, error)) *MockBlockDeviceReadAtCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockBlockDeviceReadAtCall) DoAndReturn(f func([]byte, int64) (int, error)) *MockBlockDeviceReadAtCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// Seek mocks base method.
func (m *MockBlockDevice) Seek(offset int64, whence int) (int64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Seek", offset, whence)
	ret0, _ := ret[0].(int64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Seek indicates an expected call of Seek.
func (mr *MockBlockDeviceMockRecorder) Seek(offset, whence any) *MockBlockDeviceSeekCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Seek", reflect.TypeOf((*MockBlockDevice)(nil).Seek), offset, whence)
	return &MockBlockDeviceSeekCall{Call: call}
}

// MockBlockDeviceSeekCall wrap *gomock.Call
type MockBlockDeviceSeekCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockBlockDeviceSeekCall) Return(arg0 int64, arg1 error) *MockBlockDeviceSeekCall {
	c.Call = c.Call.Return(arg0, arg1)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockBlockDeviceSeekCall) Do(f func(int64, int) (int64, error)) *MockBlockDeviceSeekCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockBlockDeviceSeekCall) DoAndReturn(f func(int64, int) (int64, error)) *MockBlockDeviceSeekCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// Size mocks base method.
func (m *MockBlockDevice) Size() (int64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Size")
	ret0, _ := ret[0].(int64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Size indicates an expected call of Size.
func (mr *MockBlockDeviceMockRecorder) Size() *MockBlockDeviceSizeCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Size", reflect.TypeOf((*MockBlockDevice)(nil).Size))
	return &MockBlockDeviceSizeCall{Call: call}
}

// MockBlockDeviceSizeCall wrap *gomock.Call
type MockBlockDeviceSizeCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockBlockDeviceSizeCall) Return(arg0 int64, arg1 error) *MockBlockDeviceSizeCall {
	c.Call = c.Call.Return(arg0, arg1)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockBlockDeviceSizeCall) Do(f func() (int64, error)) *MockBlockDeviceSizeCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockBlockDeviceSizeCall) DoAndReturn(f func() (int64, error)) *MockBlockDeviceSizeCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// WriteAt mocks base method.
func (m *MockBlockDevice) WriteAt(p []byte, off int64) (int, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "WriteAt", p, off)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// WriteAt indicates an expected call of WriteAt.
func (mr *MockBlockDeviceMockRecorder) WriteAt(p, off any) *MockBlockDeviceWriteAtCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "WriteAt", reflect.TypeOf((*MockBlockDevice)(nil).WriteAt), p, off)
	return &MockBlockDeviceWriteAtCall{Call: call}
}

// MockBlockDeviceWriteAtCall wrap *gomock.Call
type MockBlockDeviceWriteAtCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockBlockDeviceWriteAtCall) Return(n int, err error) *MockBlockDeviceWriteAtCall {
	c.Call = c.Call.Return(n, err)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockBlockDeviceWriteAtCall) Do(f func([]byte, int64) (int, error)) *MockBlockDeviceWriteAtCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockBlockDeviceWriteAtCall) DoAndReturn(f func([]byte, int64) (int, error)) *MockBlockDeviceWriteAtCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// MockFile is a mock of File interface.
type MockFile struct {
	ctrl     *gomock.Controller
	recorder *MockFileMockRecorder
	isgomock struct{}
}

// MockFileMockRecorder is the mock recorder for MockFile.
type MockFileMockRecorder struct {
	mock *MockFile
}

// NewMockFile creates a new mock instance.
func NewMockFile(ctrl *gomock.Controller) *MockFile {
	mock := &MockFile{ctrl: ctrl}
	mock.recorder = &MockFileMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockFile) EXPECT() *MockFileMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *MockFile) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockFileMockRecorder) Close() *MockFileCloseCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockFile)(nil).Close))
	return &MockFileCloseCall{Call: call}
}

// MockFileCloseCall wrap *gomock.Call
type MockFileCloseCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockFileCloseCall) Return(arg0 error) *MockFileCloseCall {
	c.Call = c.Call.Return(arg0)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockFileCloseCall) Do(f func() error) *MockFileCloseCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockFileCloseCall) DoAndReturn(f func() error) *MockFileCloseCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// Fd mocks base method.
func (m *MockFile) Fd() uintptr {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Fd")
	ret0, _ := ret[0].(uintptr)
	return ret0
}

// Fd indicates an expected call of Fd.
func (mr *MockFileMockRecorder) Fd() *MockFileFdCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Fd", reflect.TypeOf((*MockFile)(nil).Fd))
	return &MockFileFdCall{Call: call}
}

// MockFileFdCall wrap *gomock.Call
type MockFileFdCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockFileFdCall) Return(arg0 uintptr) *MockFileFdCall {
	c.Call = c.Call.Return(arg0)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockFileFdCall) Do(f func() uintptr) *MockFileFdCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockFileFdCall) DoAndReturn(f func() uintptr) *MockFileFdCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// Name mocks base method.
func (m *MockFile) Name() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Name")
	ret0, _ := ret[0].(string)
	return ret0
}

// Name indicates an expected call of Name.
func (mr *MockFileMockRecorder) Name() *MockFileNameCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Name", reflect.TypeOf((*MockFile)(nil).Name))
	return &MockFileNameCall{Call: call}
}

// MockFileNameCall wrap *gomock.Call
type MockFileNameCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockFileNameCall) Return(arg0 string) *MockFileNameCall {
	c.Call = c.Call.Return(arg0)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockFileNameCall) Do(f func() string) *MockFileNameCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockFileNameCall) DoAndReturn(f func() string) *MockFileNameCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// Read mocks base method.
func (m *MockFile) Read(p []byte) (int, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Read", p)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Read indicates an expected call of Read.
func (mr *MockFileMockRecorder) Read(p any) *MockFileReadCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Read", reflect.TypeOf((*MockFile)(nil).Read), p)
	return &MockFileReadCall{Call: call}
}

// MockFileReadCall wrap *gomock.Call
type MockFileReadCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockFileReadCall) Return(n int, err error) *MockFileReadCall {
	c.Call = c.Call.Return(n, err)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockFileReadCall) Do(f func([]byte) (int, error)) *MockFileReadCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockFileReadCall) DoAndReturn(f func([]byte) (int, error)) *MockFileReadCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// ReadAt mocks base method.
func (m *MockFile) ReadAt(p []byte, off int64) (int, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReadAt", p, off)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ReadAt indicates an expected call of ReadAt.
func (mr *MockFileMockRecorder) ReadAt(p, off any) *MockFileReadAtCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReadAt", reflect.TypeOf((*MockFile)(nil).ReadAt), p, off)
	return &MockFileReadAtCall{Call: call}
}

// MockFileReadAtCall wrap *gomock.Call
type MockFileReadAtCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockFileReadAtCall) Return(n int, err error) *MockFileReadAtCall {
	c.Call = c.Call.Return(n, err)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockFileReadAtCall) Do(f func([]byte, int64) (int, error)) *MockFileReadAtCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockFileReadAtCall) DoAndReturn(f func([]byte, int64) (int, error)) *MockFileReadAtCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// Seek mocks base method.
func (m *MockFile) Seek(offset int64, whence int) (int64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Seek", offset, whence)
	ret0, _ := ret[0].(int64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Seek indicates an expected call of Seek.
func (mr *MockFileMockRecorder) Seek(offset, whence any) *MockFileSeekCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Seek", reflect.TypeOf((*MockFile)(nil).Seek), offset, whence)
	return &MockFileSeekCall{Call: call}
}

// MockFileSeekCall wrap *gomock.Call
type MockFileSeekCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockFileSeekCall) Return(arg0 int64, arg1 error) *MockFileSeekCall {
	c.Call = c.Call.Return(arg0, arg1)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockFileSeekCall) Do(f func(int64, int) (int64, error)) *MockFileSeekCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockFileSeekCall) DoAndReturn(f func(int64, int) (int64, error)) *MockFileSeekCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// WriteAt mocks base method.
func (m *MockFile) WriteAt(p []byte, off int64) (int, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "WriteAt", p, off)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// WriteAt indicates an expected call of WriteAt.
func (mr *MockFileMockRecorder) WriteAt(p, off any) *MockFileWriteAtCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "WriteAt", reflect.TypeOf((*MockFile)(nil).WriteAt), p, off)
	return &MockFileWriteAtCall{Call: call}
}

// MockFileWriteAtCall wrap *gomock.Call
type MockFileWriteAtCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockFileWriteAtCall) Return(n int, err error) *MockFileWriteAtCall {
	c.Call = c.Call.Return(n, err)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockFileWriteAtCall) Do(f func([]byte, int64) (int, error)) *MockFileWriteAtCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockFileWriteAtCall) DoAndReturn(f func([]byte, int64) (int, error)) *MockFileWriteAtCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// MockFileOpener is a mock of FileOpener interface.
type MockFileOpener struct {
	ctrl     *gomock.Controller
	recorder *MockFileOpenerMockRecorder
	isgomock struct{}
}

// MockFileOpenerMockRecorder is the mock recorder for MockFileOpener.
type MockFileOpenerMockRecorder struct {
	mock *MockFileOpener
}

// NewMockFileOpener creates a new mock instance.
func NewMockFileOpener(ctrl *gomock.Controller) *MockFileOpener {
	mock := &MockFileOpener{ctrl: ctrl}
	mock.recorder = &MockFileOpenerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockFileOpener) EXPECT() *MockFileOpenerMockRecorder {
	return m.recorder
}

// Open mocks base method.
func (m *MockFileOpener) Open(name string, flag int, mode fs.FileMode) (utils.File, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Open", name, flag, mode)
	ret0, _ := ret[0].(utils.File)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Open indicates an expected call of Open.
func (mr *MockFileOpenerMockRecorder) Open(name, flag, mode any) *MockFileOpenerOpenCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Open", reflect.TypeOf((*MockFileOpener)(nil).Open), name, flag, mode)
	return &MockFileOpenerOpenCall{Call: call}
}

// MockFileOpenerOpenCall wrap *gomock.Call
type MockFileOpenerOpenCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockFileOpenerOpenCall) Return(arg0 utils.File, arg1 error) *MockFileOpenerOpenCall {
	c.Call = c.Call.Return(arg0, arg1)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockFileOpenerOpenCall) Do(f func(string, int, fs.FileMode) (utils.File, error)) *MockFileOpenerOpenCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockFileOpenerOpenCall) DoAndReturn(f func(string, int, fs.FileMode) (utils.File, error)) *MockFileOpenerOpenCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// MockBlockDeviceOpener is a mock of BlockDeviceOpener interface.
type MockBlockDeviceOpener struct {
	ctrl     *gomock.Controller
	recorder *MockBlockDeviceOpenerMockRecorder
	isgomock struct{}
}

// MockBlockDeviceOpenerMockRecorder is the mock recorder for MockBlockDeviceOpener.
type MockBlockDeviceOpenerMockRecorder struct {
	mock *MockBlockDeviceOpener
}

// NewMockBlockDeviceOpener creates a new mock instance.
func NewMockBlockDeviceOpener(ctrl *gomock.Controller) *MockBlockDeviceOpener {
	mock := &MockBlockDeviceOpener{ctrl: ctrl}
	mock.recorder = &MockBlockDeviceOpenerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockBlockDeviceOpener) EXPECT() *MockBlockDeviceOpenerMockRecorder {
	return m.recorder
}

// Open mocks base method.
func (m *MockBlockDeviceOpener) Open(name string, flag int) (utils.BlockDevice, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Open", name, flag)
	ret0, _ := ret[0].(utils.BlockDevice)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Open indicates an expected call of Open.
func (mr *MockBlockDeviceOpenerMockRecorder) Open(name, flag any) *MockBlockDeviceOpenerOpenCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Open", reflect.TypeOf((*MockBlockDeviceOpener)(nil).Open), name, flag)
	return &MockBlockDeviceOpenerOpenCall{Call: call}
}

// MockBlockDeviceOpenerOpenCall wrap *gomock.Call
type MockBlockDeviceOpenerOpenCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockBlockDeviceOpenerOpenCall) Return(arg0 utils.BlockDevice, arg1 error) *MockBlockDeviceOpenerOpenCall {
	c.Call = c.Call.Return(arg0, arg1)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockBlockDeviceOpenerOpenCall) Do(f func(string, int) (utils.BlockDevice, error)) *MockBlockDeviceOpenerOpenCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockBlockDeviceOpenerOpenCall) DoAndReturn(f func(string, int) (utils.BlockDevice, error)) *MockBlockDeviceOpenerOpenCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}
