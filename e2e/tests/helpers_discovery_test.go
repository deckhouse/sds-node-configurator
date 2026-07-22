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

package tests

// expectedDisk is the expected (node, VD name) for one created VirtualDisk.
// Serial: virtualization may use VirtualDisk.UID or VirtualMachineBlockDeviceAttachment.UID (hex MD5); we accept either.
type expectedDisk struct {
	Node                string
	VDDiskName          string
	ExpectedSerialVD    string // hex(MD5(VirtualDisk.UID))
	ExpectedSerialVMBDA string // hex(MD5(VirtualMachineBlockDeviceAttachment.UID))
	ExpectedBDName      string
}

// nameSerialCheckRow is one row of the BlockDevice name/serial check table (expected vs actual).
type nameSerialCheckRow struct {
	Node                string
	VDName              string
	BDName              string
	ExpectedSerialVD    string
	ExpectedSerialVMBDA string
	ActualSerial        string
	SerialMatch         bool
	ExpectedBDName      string
	ActualBDName        string
	NameMatch           bool
}

// discoveryTableRow is one row of the discovery test summary (VD + BD).
type discoveryTableRow struct {
	Node        string
	VDName      string
	BDName      string
	Path        string
	SerialBD    string
	SerialLsblk string
	SizeBD      string
	SizeLsblk   string
	Match       bool
}
