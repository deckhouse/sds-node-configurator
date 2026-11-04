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

// This file names the SCSI target a block device is reached through, so that a
// CSI driver can hide every LUN of a target it owns — including a LUN that
// appeared a moment ago and that the driver has not discovered yet, which no
// filter by WWN can name.
//
// Read from sysfs, the same for both ways the agent lists devices: lsblk and
// the udev database say nothing about the target, and the kernel puts a SCSI
// disk under the iSCSI session or the Fibre Channel remote port it came in by.

package utils

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

// sysRootForSCSITargets is a variable so tests can point the lookup at a fake
// tree.
var sysRootForSCSITargets = "/sys"

// scsiTargetStackDepth bounds the walk down a device stack (a partition of a
// crypt mapping of a multipath map of a path), so a sysfs tree that loops
// cannot spin here.
const scsiTargetStackDepth = 16

var (
	iscsiSessionComponent = regexp.MustCompile(`^session\d+$`)
	fcRemotePortComponent = regexp.MustCompile(`^rport-\d+:\d+-\d+$`)
)

// SCSITargetOf returns the identity of the SCSI target behind the device kName
// (/dev/sdb, /dev/dm-3, /dev/sdb1): the target name of the iSCSI session, or
// the WWPN of the Fibre Channel remote port, as sysfs spells them. A device
// stacked on others — a partition, a multipath map, a crypt mapping — takes it
// from the disks beneath; when its paths lead to more than one target, the
// smallest identity is the one returned, so every node names the same LUN the
// same way.
//
// It returns "" for a device that is not a LUN of an iSCSI or Fibre Channel
// target: a local disk, a virtual disk, an NVMe namespace.
//
// It returns an error when a path does sit under an iSCSI session or a remote
// port whose identity cannot be read. The caller must not then treat the device
// as one without a target: that would offer a LUN of a target a CSI driver owns
// as an ordinary disk.
func SCSITargetOf(kName string) (string, error) {
	identities, err := scsiTargetsOf(strings.TrimPrefix(kName, "/dev/"), scsiTargetStackDepth)
	if err != nil {
		return "", err
	}
	if len(identities) == 0 {
		return "", nil
	}

	return slices.Min(identities), nil
}

func scsiTargetsOf(name string, depth int) ([]string, error) {
	if depth <= 0 {
		return nil, fmt.Errorf("the device stack under %s is deeper than %d levels", name, scsiTargetStackDepth)
	}

	blockDir := filepath.Join(sysRootForSCSITargets, "class", "block", name)

	if _, err := os.Stat(filepath.Join(blockDir, "partition")); err == nil {
		resolved, err := filepath.EvalSymlinks(blockDir)
		if err != nil {
			return nil, fmt.Errorf("resolving the partition %s: %w", name, err)
		}
		return scsiTargetsOf(filepath.Base(filepath.Dir(resolved)), depth-1)
	}

	slaves, err := os.ReadDir(filepath.Join(blockDir, "slaves"))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("reading the slaves of %s: %w", name, err)
	}
	if len(slaves) > 0 {
		var identities []string
		for _, slave := range slaves {
			found, err := scsiTargetsOf(slave.Name(), depth-1)
			if err != nil {
				return nil, err
			}
			identities = append(identities, found...)
		}
		return identities, nil
	}

	device, err := filepath.EvalSymlinks(filepath.Join(blockDir, "device"))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("resolving the device of %s: %w", name, err)
	}

	identity, err := scsiTargetOfDevicePath(device)
	if err != nil {
		return nil, fmt.Errorf("reading the SCSI target of %s: %w", name, err)
	}
	if identity == "" {
		return nil, nil
	}
	return []string{identity}, nil
}

// scsiTargetOfDevicePath reads the identity of the target a SCSI device sits
// under. The kernel places an iSCSI LUN at .../hostH/sessionS/targetH:C:T/H:C:T:L
// and a Fibre Channel LUN at .../hostH/rport-H:C-R/targetH:C:T/H:C:T:L.
func scsiTargetOfDevicePath(devicePath string) (string, error) {
	for _, component := range strings.Split(devicePath, string(filepath.Separator)) {
		switch {
		case iscsiSessionComponent.MatchString(component):
			return readSysfsIdentity(filepath.Join(sysRootForSCSITargets, "class", "iscsi_session", component, "targetname"))
		case fcRemotePortComponent.MatchString(component):
			return readSysfsIdentity(filepath.Join(sysRootForSCSITargets, "class", "fc_remote_ports", component, "port_name"))
		}
	}

	return "", nil
}

func readSysfsIdentity(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}

	identity := strings.TrimSpace(string(data))
	if identity == "" || identity == "(null)" {
		return "", fmt.Errorf("%s is empty", path)
	}
	return strings.ToLower(identity), nil
}
