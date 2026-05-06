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

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/storage-e2e/pkg/cluster"
	"github.com/deckhouse/storage-e2e/pkg/kubernetes"
	"github.com/deckhouse/storage-e2e/pkg/ssh"
)

// clusterResumeState mirrors storage-e2e cluster-state.json (namespace after VMs are created).
// runLsblkViaDirectSSHWithRetry wraps runLsblkViaDirectSSH for transient SSH errors (EOF during handshake, reset).
func runLsblkViaDirectSSHWithRetry(ctx context.Context, testKubeconfig *rest.Config, nodeName, sshUser string, maxRetries int, retryInterval time.Duration) (map[string]lsblkLine, error) {
	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		lines, err := runLsblkViaDirectSSH(ctx, testKubeconfig, nodeName, sshUser)
		if err == nil {
			return lines, nil
		}
		lastErr = err
		if attempt < maxRetries {
			GinkgoWriter.Printf("      lsblk SSH to %s attempt %d/%d failed: %v; retry in %v\n", nodeName, attempt, maxRetries, err, retryInterval)
			time.Sleep(retryInterval)
		}
	}
	return nil, lastErr
}

// expectedDisk is the expected (node, VD name) for one created VirtualDisk (same order as e2eDiskAttachments).
// Serial: virtualization may use VirtualDisk.UID or VirtualMachineBlockDeviceAttachment.UID (hex MD5); we accept either.

// blockDeviceNameFromDiscoveryInput returns the BlockDevice name (same formula as agent createUniqDeviceName: dev-SHA1(nodeName+wwn+model+serial+partUUID)).
func blockDeviceNameFromDiscoveryInput(nodeName, wwn, model, serial, partUUID string) string {
	temp := nodeName + wwn + model + serial + partUUID
	s := sha1.Sum([]byte(temp))
	return fmt.Sprintf("dev-%x", s)
}

// nameSerialCheckRow is one row of the BlockDevice name/serial check table (expected vs actual).

func getE2ENodeName() string { return os.Getenv("E2E_NODE_NAME") }

func getE2EDevicePath() string { return os.Getenv("E2E_DEVICE_PATH") }

func formatBlockDevicesHint(items []v1alpha1.BlockDevice, expectedNode string) string {
	if len(items) == 0 {
		return "No BlockDevices in cluster."
	}
	var lines []string
	nodesSeen := make(map[string]bool)
	for _, bd := range items {
		n := bd.Status.NodeName
		if n == "" {
			n = "<no nodeName>"
		}
		nodesSeen[n] = true
		path := bd.Status.Path
		if path == "" {
			path = "<no path>"
		}
		lines = append(lines, fmt.Sprintf("%s: nodeName=%s path=%s size=%s", bd.Name, n, path, bd.Status.Size.String()))
	}
	hint := "Existing BlockDevices: " + strings.Join(lines, "; ")
	if expectedNode != "" && !nodesSeen[expectedNode] {
		var nodes []string
		for n := range nodesSeen {
			nodes = append(nodes, n)
		}
		hint += ". Expected nodeName=" + expectedNode + " but only found nodes: " + strings.Join(nodes, ", ")
	}
	return hint
}

// e2eWaitConsumableBlockDeviceForVirtualDisk finds the BlockDevice for this VirtualDisk attachment the same way
// as the discovery tests: Status.Serial must equal hex(md5(VirtualDisk.UID)) or hex(md5(VMBDA.UID)).
// This avoids picking another disk on the same node (leftover LVM, other e2e disks).

// runLsblkViaDirectSSH connects to the node by IP the same way we connect to the master (SSH_HOST / jump → node).
// Gets node IP from the test cluster API and uses the same SSH credentials (jump host if set, VM user, key).
func runLsblkViaDirectSSH(ctx context.Context, testKubeconfig *rest.Config, nodeName, sshUser string) (map[string]lsblkLine, error) {
	nodeIP, err := kubernetes.GetNodeInternalIP(ctx, testKubeconfig, nodeName)
	if err != nil {
		return nil, fmt.Errorf("get IP for node %s: %w", nodeName, err)
	}
	keyPath, err := cluster.GetSSHPrivateKeyPath()
	if err != nil {
		return nil, fmt.Errorf("get SSH key path: %w", err)
	}
	jumpKeyPath := e2eConfigSSHJumpKeyPath()
	if jumpKeyPath == "" {
		jumpKeyPath = keyPath
	}
	var sshClient ssh.Client
	if e2eConfigSSHJumpHost() != "" {
		jumpUser := e2eConfigSSHJumpUser()
		if jumpUser == "" {
			jumpUser = e2eConfigSSHUser()
		}
		sshClient, err = ssh.NewClientWithJumpHost(jumpUser, e2eConfigSSHJumpHost(), jumpKeyPath, sshUser, nodeIP, keyPath)
	} else {
		sshClient, err = ssh.NewClient(sshUser, nodeIP, keyPath)
	}
	if err != nil {
		return nil, fmt.Errorf("SSH to node %s (%s@%s): %w", nodeName, sshUser, nodeIP, err)
	}
	defer sshClient.Close()
	out, err := sshClient.Exec(ctx, "lsblk -b -P -o NAME,SIZE,SERIAL,PATH -n")
	if err != nil {
		return nil, fmt.Errorf("run lsblk on node %s (%s@%s): %w", nodeName, sshUser, nodeIP, err)
	}
	return parseLsblkOutput(out), nil
}

// e2eExecOnTestClusterNodeSSH runs a shell command on a test cluster node (same SSH path as lsblk: jump host + node IP).
//
// Each call builds a new storage-e2e SSH client and connects again. With SSH_JUMP_HOST, internal code may log
// DEBUG lines (SSH key loaded / ssh-agent) per hop, so a single Exec can produce several [DEBUG] lines.
// To reduce noise, run tests with a lower log level for storage-e2e if supported (e.g. LOG_LEVEL=info).

// e2eExecOnTestClusterNodeSSH runs a shell command on a test cluster node (same SSH path as lsblk: jump host + node IP).
//
// Each call builds a new storage-e2e SSH client and connects again. With SSH_JUMP_HOST, internal code may log
// DEBUG lines (SSH key loaded / ssh-agent) per hop, so a single Exec can produce several [DEBUG] lines.
// To reduce noise, run tests with a lower log level for storage-e2e if supported (e.g. LOG_LEVEL=info).
func e2eExecOnTestClusterNodeSSH(ctx context.Context, testKubeconfig *rest.Config, nodeName, sshUser, command string) (string, error) {
	nodeIP, err := kubernetes.GetNodeInternalIP(ctx, testKubeconfig, nodeName)
	if err != nil {
		return "", fmt.Errorf("get IP for node %s: %w", nodeName, err)
	}
	keyPath, err := cluster.GetSSHPrivateKeyPath()
	if err != nil {
		return "", fmt.Errorf("get SSH key path: %w", err)
	}
	jumpKeyPath := e2eConfigSSHJumpKeyPath()
	if jumpKeyPath == "" {
		jumpKeyPath = keyPath
	}
	var sshClient ssh.Client
	if e2eConfigSSHJumpHost() != "" {
		jumpUser := e2eConfigSSHJumpUser()
		if jumpUser == "" {
			jumpUser = e2eConfigSSHUser()
		}
		sshClient, err = ssh.NewClientWithJumpHost(jumpUser, e2eConfigSSHJumpHost(), jumpKeyPath, sshUser, nodeIP, keyPath)
	} else {
		sshClient, err = ssh.NewClient(sshUser, nodeIP, keyPath)
	}
	if err != nil {
		return "", fmt.Errorf("SSH to node %s (%s@%s): %w", nodeName, sshUser, nodeIP, err)
	}
	defer sshClient.Close()
	out, err := sshClient.Exec(ctx, command)
	if err != nil {
		return out, fmt.Errorf("exec on node %s: %w", nodeName, err)
	}
	return out, nil
}

// e2eVgNameListedInVgsOutput returns true if a line in vgs output (one VG name per line) equals vgName.

// e2eVgNameListedInVgsOutput returns true if a line in vgs output (one VG name per line) equals vgName.
func e2eVgNameListedInVgsOutput(vgsOutput, vgName string) bool {
	for _, line := range strings.Split(vgsOutput, "\n") {
		if strings.TrimSpace(line) == vgName {
			return true
		}
	}
	return false
}

// e2eShellRemoveThinPoolStackForVG returns a shell script run on the guest node via SSH: removes thin volumes that
// use the pool, then the pool LV, then any remaining LVs in the VG. Used only by e2e to avoid Terminating LVMVolumeGroup
// when agent-side delete ordering leaves thin-pool segments on the node.

// e2eShellRemoveThinPoolStackForVG returns a shell script run on the guest node via SSH: removes thin volumes that
// use the pool, then the pool LV, then any remaining LVs in the VG. Used only by e2e to avoid Terminating LVMVolumeGroup
// when agent-side delete ordering leaves thin-pool segments on the node.
func e2eShellRemoveThinPoolStackForVG(vgName, thinPoolName string) string {
	return fmt.Sprintf(`set +e
VG=%q
POOL=%q
runlv() { lvs "$@" 2>/dev/null || sudo -n lvs "$@" 2>/dev/null; }
runrm() { lvremove -fy "$@" 2>/dev/null || sudo -n lvremove -fy "$@" 2>/dev/null; }
for pass in 1 2 3 4 5 6 7 8 9 10; do
  runlv -a --noheadings -o lv_name,pool_lv "$VG" | while IFS= read -r line; do
    lv=$(echo "$line" | awk '{print $1}' | tr -d '[]')
    pl=$(echo "$line" | awk '{print $2}' | tr -d '[]')
    [ -z "$lv" ] && continue
    [ -n "$pl" ] && [ "$pl" = "$POOL" ] && [ "$lv" != "$POOL" ] && runrm "/dev/$VG/$lv"
  done
done
runrm "/dev/$VG/$POOL"
for pass in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do
  cnt=$(runlv -a --noheadings -o lv_name "$VG" | sed '/^$/d' | wc -l)
  cnt=$(echo "$cnt" | tr -cd '0-9')
  [ "${cnt:-0}" -eq 0 ] && break
  runlv -a --noheadings -o lv_name "$VG" | while IFS= read -r line; do
    lv=$(echo "$line" | awk '{print $1}' | tr -d '[]')
    [ -n "$lv" ] && runrm "/dev/$VG/$lv"
  done
done
`, vgName, thinPoolName)
}

// parseLsblkOutput parses lsblk -b -P -o NAME,SIZE,SERIAL,PATH output (KEY="value" per line).
// Returns map keyed by PATH.

// parseLsblkOutput parses lsblk -b -P -o NAME,SIZE,SERIAL,PATH output (KEY="value" per line).
// Returns map keyed by PATH.
func parseLsblkOutput(out string) map[string]lsblkLine {
	result := make(map[string]lsblkLine)
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var path, serial, sizeStr string
		for _, part := range strings.Split(line, " ") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			if idx := strings.Index(part, "="); idx >= 0 && len(part) > idx+2 {
				k, v := part[:idx], strings.Trim(part[idx+1:], "\"")
				switch k {
				case "PATH":
					path = v
				case "SERIAL":
					serial = v
				case "SIZE":
					sizeStr = v
				}
			}
		}
		if path == "" {
			continue
		}
		var sizeBytes int64
		if sizeStr != "" {
			sizeBytes, _ = strconv.ParseInt(sizeStr, 10, 64)
		}
		result[path] = lsblkLine{Path: path, Serial: serial, Size: sizeStr, SizeBytes: sizeBytes}
	}
	return result
}

func printBlockDeviceNameSerialTable(rows []nameSerialCheckRow) {
	if len(rows) == 0 {
		return
	}
	const (
		wNode   = 18
		wVD     = 32
		wBD     = 44
		wSerial = 34
		wMatch  = 5
	)
	pad := func(s string, w int) string {
		if len(s) > w {
			return s[:w-1] + "…"
		}
		return s + strings.Repeat(" ", w-len(s))
	}
	sep := " | "
	header := pad("NODE", wNode) + sep + pad("VD_NAME", wVD) + sep + pad("BD_NAME", wBD) + sep +
		pad("EXP_SERIAL_VD", wSerial) + sep + pad("EXP_SERIAL_VMBDA", wSerial) + sep + pad("ACT_SERIAL", wSerial) + sep + pad("SERIAL", wMatch) + sep +
		pad("EXP_BD_NAME", wBD) + sep + pad("ACT_BD_NAME", wBD) + sep + pad("NAME", wMatch)
	lineLen := len(header)

	GinkgoWriter.Println("\n========== BlockDevice name & serial check (expected vs actual) ==========")
	GinkgoWriter.Println(header)
	GinkgoWriter.Println(strings.Repeat("-", lineLen))
	for _, r := range rows {
		serialOk := "✓"
		if !r.SerialMatch {
			serialOk = "✗"
		}
		nameOk := "✓"
		if !r.NameMatch {
			nameOk = "✗"
		}
		GinkgoWriter.Println(
			pad(r.Node, wNode) + sep +
				pad(r.VDName, wVD) + sep +
				pad(r.BDName, wBD) + sep +
				pad(r.ExpectedSerialVD, wSerial) + sep +
				pad(r.ExpectedSerialVMBDA, wSerial) + sep +
				pad(r.ActualSerial, wSerial) + sep +
				pad(serialOk, wMatch) + sep +
				pad(r.ExpectedBDName, wBD) + sep +
				pad(r.ActualBDName, wBD) + sep +
				pad(nameOk, wMatch))
	}
	GinkgoWriter.Println(strings.Repeat("=", lineLen) + "\n")
}

func printDiscoveryTable(rows []discoveryTableRow) {
	if len(rows) == 0 {
		return
	}
	const (
		wNode   = 18
		wVD     = 32
		wBD     = 44
		wPath   = 10
		wSerial = 34
		wSize   = 12
		wMatch  = 5
	)
	pad := func(s string, w int) string {
		if len(s) > w {
			return s[:w-1] + "…"
		}
		return s + strings.Repeat(" ", w-len(s))
	}
	sep := " | "
	header := pad("NODE", wNode) + sep + pad("VD_NAME", wVD) + sep + pad("BD_NAME", wBD) + sep + pad("PATH", wPath) + sep + pad("SERIAL_BD", wSerial) + sep + pad("SERIAL_LSBLK", wSerial) + sep + pad("SIZE_BD", wSize) + sep + pad("SIZE_LSBLK", wSize) + sep + pad("MATCH", wMatch)
	lineLen := len(header)

	GinkgoWriter.Println("\n========== Discovery test summary (VD → BD → lsblk) ==========")
	GinkgoWriter.Println(header)
	GinkgoWriter.Println(strings.Repeat("-", lineLen))
	for _, r := range rows {
		matchStr := "—"
		if r.SerialLsblk != "" {
			if r.Match {
				matchStr = "✓"
			} else {
				matchStr = "✗"
			}
		}
		GinkgoWriter.Println(
			pad(r.Node, wNode) + sep +
				pad(r.VDName, wVD) + sep +
				pad(r.BDName, wBD) + sep +
				pad(r.Path, wPath) + sep +
				pad(r.SerialBD, wSerial) + sep +
				pad(r.SerialLsblk, wSerial) + sep +
				pad(r.SizeBD, wSize) + sep +
				pad(r.SizeLsblk, wSize) + sep +
				pad(matchStr, wMatch))
	}
	GinkgoWriter.Println(strings.Repeat("=", lineLen) + "\n")
}

func printBlockDeviceInfo(bd *v1alpha1.BlockDevice) {
	GinkgoWriter.Println("\n========== BlockDevice information ==========")
	GinkgoWriter.Printf("Name: %s\n", bd.Name)
	GinkgoWriter.Printf("NodeName: %s\n", bd.Status.NodeName)
	GinkgoWriter.Printf("Path: %s\n", bd.Status.Path)
	GinkgoWriter.Printf("Size: %s\n", bd.Status.Size.String())
	GinkgoWriter.Printf("Type: %s\n", bd.Status.Type)
	GinkgoWriter.Printf("Serial: %s\n", bd.Status.Serial)
	GinkgoWriter.Printf("WWN: %s\n", bd.Status.Wwn)
	GinkgoWriter.Printf("Model: %s\n", bd.Status.Model)
	GinkgoWriter.Printf("Consumable: %t\n", bd.Status.Consumable)
	GinkgoWriter.Printf("FSType: %s\n", bd.Status.FsType)
	GinkgoWriter.Printf("MachineID: %s\n", bd.Status.MachineID)
	GinkgoWriter.Printf("Rota: %t\n", bd.Status.Rota)
	GinkgoWriter.Printf("HotPlug: %t\n", bd.Status.HotPlug)
	GinkgoWriter.Println("=============================================\n")
}

// e2ePrintBlockDevicesConsumableSummary prints a compact table of BlockDevice status fields relevant to LVM validation.

// e2ePrintBlockDevicesConsumableSummary prints a compact table of BlockDevice status fields relevant to LVM validation.
func e2ePrintBlockDevicesConsumableSummary(ctx context.Context, cl client.Client, bdNames []string, title string) {
	if len(bdNames) == 0 {
		return
	}
	names := append([]string(nil), bdNames...)
	sort.Strings(names)
	GinkgoWriter.Printf("\n========== BlockDevices (%s) ==========\n", title)
	for _, name := range names {
		var bd v1alpha1.BlockDevice
		if err := cl.Get(ctx, client.ObjectKey{Name: name}, &bd); err != nil {
			GinkgoWriter.Printf("  %s: Get failed: %v\n", name, err)
			continue
		}
		GinkgoWriter.Printf("  %s: Consumable=%v  FsType=%q  PVUuid=%q  Path=%s  Size=%s  LVMVolumeGroupName=%q\n",
			bd.Name, bd.Status.Consumable, bd.Status.FsType, bd.Status.PVUuid, bd.Status.Path, bd.Status.Size.String(), bd.Status.LVMVolumeGroupName)
	}
	GinkgoWriter.Println("=================================================\n")
}

func blockDeviceSerialFromVirtualDiskUID(uid string) string {
	h := md5.Sum([]byte(uid))
	return hex.EncodeToString(h[:])
}
