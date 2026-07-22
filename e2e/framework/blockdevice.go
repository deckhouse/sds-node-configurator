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

package framework

import (
	"context"
	"crypto/sha1"
	"fmt"
	"time"

	"github.com/deckhouse/storage-e2e/pkg/e2e"
	"github.com/deckhouse/storage-e2e/pkg/kubernetes"
	"k8s.io/client-go/rest"
)

// BlockDeviceName mirrors the agent createUniqDeviceName: dev-SHA1(node+wwn+model+serial+partUUID).
// First source of truth: images/agent/internal/controller/bd/discoverer.go (createUniqDeviceName).
// NOTE: the agent hashes udev-normalized Wwn/Model/Serial; keep in sync until formula is extracted to api.DeviceName (follow-up).
func BlockDeviceName(nodeName, wwn, model, serial, partUUID string) string {
	temp := nodeName + wwn + model + serial + partUUID
	s := sha1.Sum([]byte(temp))
	return fmt.Sprintf("dev-%x", s)
}

// WaitNewConsumableBlockDevice polls the consumable BlockDevices on node and returns the single
// BlockDevice that is present now but was not in `before` (set-diff by Name). It errors if, at timeout,
// there are zero new devices, and errors immediately if more than one new device appears.
func WaitNewConsumableBlockDevice(ctx context.Context, restCfg *rest.Config, node string, before []kubernetes.BlockDevice, timeout time.Duration) (kubernetes.BlockDevice, error) {
	beforeSet := make(map[string]struct{}, len(before))
	for _, bd := range before {
		beforeSet[bd.Name] = struct{}{}
	}

	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		bds, err := kubernetes.GetConsumableBlockDevicesByNode(ctx, restCfg, node)
		if err != nil {
			lastErr = err
		} else {
			var found []kubernetes.BlockDevice
			for _, bd := range bds {
				if _, ok := beforeSet[bd.Name]; !ok {
					found = append(found, bd)
				}
			}
			switch len(found) {
			case 1:
				return found[0], nil
			case 0:
				lastErr = fmt.Errorf("no new consumable block device on node %s yet", node)
			default:
				names := make([]string, 0, len(found))
				for _, bd := range found {
					names = append(names, bd.Name)
				}
				return kubernetes.BlockDevice{}, fmt.Errorf(
					"expected exactly one new consumable block device on node %s, got %d: %v", node, len(found), names)
			}
		}

		if err := ctx.Err(); err != nil {
			return kubernetes.BlockDevice{}, err
		}
		if time.Now().After(deadline) {
			return kubernetes.BlockDevice{}, fmt.Errorf(
				"timeout waiting for a new consumable block device on node %s: %w", node, lastErr)
		}
		time.Sleep(5 * time.Second)
	}
}

// TriggerLVMDiscovery nudges the agent scanner (udev + pvscan) after on-node LVM changes.
func TriggerLVMDiscovery(ctx context.Context, cl *e2e.Cluster, node string) {
	script := `sudo -n pvscan --cache 2>&1 || true
sudo -n udevadm trigger --subsystem-match=block --action=change 2>&1 || true`
	_, _ = NodeExecChecked(ctx, cl, node, script)
}
