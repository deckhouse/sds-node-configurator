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
	"fmt"
	"strings"
	"time"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/e2e/framework"
	"github.com/deckhouse/storage-e2e/pkg/e2e"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	bdtypesDiskSize          = "2Gi"
	bdtypesDiscoveryTimeout  = 5 * time.Minute
	bdtypesFilterWaitTimeout = 3 * time.Minute
	bdtypesPollInterval      = 5 * time.Second
	// Fixed passphrase for ephemeral e2e LUKS volumes (never used outside the test node).
	bdtypesLUKSPassphrase = "e2e-sds-node-configurator-luks"
)

// bdtypesRequireCmd skips the current spec when cmd is missing on the node.
func bdtypesRequireCmd(ctx context.Context, cl *e2e.Cluster, node, cmd string) {
	GinkgoHelper()
	out, err := framework.NodeExecChecked(ctx, cl, node,
		fmt.Sprintf("command -v %s >/dev/null && echo ok", shellQuote(cmd)))
	if err != nil || !strings.Contains(out, "ok") {
		Skip(fmt.Sprintf("%s is not available on node %s (needed for this device-type scenario)", cmd, node))
	}
}

// bdtypesCreateMpath wraps backingPath in a single-path dm-multipath device
// (dmsetup linear table + UUID prefix mpath-). Returns the mapper path.
func bdtypesCreateMpath(ctx context.Context, cl *e2e.Cluster, node, backingPath, mapperName string) string {
	GinkgoHelper()
	bdtypesRequireCmd(ctx, cl, node, "dmsetup")

	script := fmt.Sprintf(`set -eu
DEV=%s
NAME=%s
UUID=mpath-e2e-%s
if [ ! -b "$DEV" ]; then echo "backing device missing: $DEV" >&2; exit 1; fi
if [ -e "/dev/mapper/$NAME" ]; then echo "mapper already exists: $NAME" >&2; exit 1; fi
SZ=$(sudo -n blockdev --getsz "$DEV")
echo "0 $SZ linear $DEV 0" | sudo -n dmsetup create "$NAME" --uuid "$UUID"
sudo -n udevadm settle || true
sudo -n udevadm trigger --action=change --subsystem-match=block || true
test -b "/dev/mapper/$NAME"
printf '%%s\n' "/dev/mapper/$NAME"
`, shellQuote(backingPath), shellQuote(mapperName), shellQuote(mapperName))

	out, err := framework.NodeExecChecked(ctx, cl, node, script)
	Expect(err).NotTo(HaveOccurred(), "dmsetup mpath create failed: %s", out)
	mapper := strings.TrimSpace(out)
	Expect(mapper).To(HavePrefix("/dev/mapper/"))
	return mapper
}

// bdtypesRemoveMpath removes a dmsetup mapper created by bdtypesCreateMpath.
func bdtypesRemoveMpath(ctx context.Context, cl *e2e.Cluster, node, mapperName string) {
	script := fmt.Sprintf(`set -eu
NAME=%s
if [ -e "/dev/mapper/$NAME" ] || sudo -n dmsetup info "$NAME" >/dev/null 2>&1; then
  sudo -n dmsetup remove -f "$NAME" || sudo -n dmsetup remove "$NAME" || true
fi
`, shellQuote(mapperName))
	if out, err := framework.NodeExecChecked(ctx, cl, node, script); err != nil {
		GinkgoWriter.Printf("bdtypesRemoveMpath %s: %v (%s)\n", mapperName, err, out)
	}
}

// bdtypesOpenLUKS formats backingPath as LUKS2 and opens it as mapperName.
// Returns the opened mapper path.
func bdtypesOpenLUKS(ctx context.Context, cl *e2e.Cluster, node, backingPath, mapperName string) string {
	GinkgoHelper()
	bdtypesRequireCmd(ctx, cl, node, "cryptsetup")

	script := fmt.Sprintf(`set -eu
DEV=%s
NAME=%s
PASS=%s
if [ ! -b "$DEV" ]; then echo "backing device missing: $DEV" >&2; exit 1; fi
if [ -e "/dev/mapper/$NAME" ]; then echo "mapper already exists: $NAME" >&2; exit 1; fi
printf '%%s' "$PASS" | sudo -n cryptsetup luksFormat --batch-mode --type luks2 "$DEV" -
printf '%%s' "$PASS" | sudo -n cryptsetup open --type luks2 "$DEV" "$NAME" -
sudo -n udevadm settle || true
sudo -n udevadm trigger --action=change --subsystem-match=block || true
test -b "/dev/mapper/$NAME"
printf '%%s\n' "/dev/mapper/$NAME"
`, shellQuote(backingPath), shellQuote(mapperName), shellQuote(bdtypesLUKSPassphrase))

	out, err := framework.NodeExecChecked(ctx, cl, node, script)
	Expect(err).NotTo(HaveOccurred(), "cryptsetup open failed: %s", out)
	mapper := strings.TrimSpace(out)
	Expect(mapper).To(HavePrefix("/dev/mapper/"))
	return mapper
}

// bdtypesFormatClosedLUKS formats backingPath as LUKS2 without opening it.
func bdtypesFormatClosedLUKS(ctx context.Context, cl *e2e.Cluster, node, backingPath string) {
	GinkgoHelper()
	bdtypesRequireCmd(ctx, cl, node, "cryptsetup")

	script := fmt.Sprintf(`set -eu
DEV=%s
PASS=%s
if [ ! -b "$DEV" ]; then echo "backing device missing: $DEV" >&2; exit 1; fi
printf '%%s' "$PASS" | sudo -n cryptsetup luksFormat --batch-mode --type luks2 "$DEV" -
sudo -n udevadm settle || true
sudo -n udevadm trigger --action=change --subsystem-match=block || true
`, shellQuote(backingPath), shellQuote(bdtypesLUKSPassphrase))

	out, err := framework.NodeExecChecked(ctx, cl, node, script)
	Expect(err).NotTo(HaveOccurred(), "cryptsetup luksFormat failed: %s", out)
}

// bdtypesCloseLUKS closes and best-effort wipes a LUKS mapper.
func bdtypesCloseLUKS(ctx context.Context, cl *e2e.Cluster, node, mapperName, backingPath string) {
	script := fmt.Sprintf(`set -eu
NAME=%s
DEV=%s
if [ -e "/dev/mapper/$NAME" ]; then
  sudo -n cryptsetup close "$NAME" || true
fi
if [ -n "$DEV" ] && [ -b "$DEV" ]; then
  sudo -n wipefs -a -f "$DEV" >/dev/null 2>&1 || true
fi
`, shellQuote(mapperName), shellQuote(backingPath))
	if out, err := framework.NodeExecChecked(ctx, cl, node, script); err != nil {
		GinkgoWriter.Printf("bdtypesCloseLUKS %s: %v (%s)\n", mapperName, err, out)
	}
}

// bdtypesCreateLoop creates a sparse file ≥2Gi and attaches it via losetup.
// Returns the loop device path and the backing file path.
func bdtypesCreateLoop(ctx context.Context, cl *e2e.Cluster, node, runID string) (loopPath, filePath string) {
	GinkgoHelper()
	bdtypesRequireCmd(ctx, cl, node, "losetup")

	filePath = fmt.Sprintf("/var/tmp/e2e-bdtypes-loop-%s.img", runID)
	script := fmt.Sprintf(`set -eu
FILE=%s
rm -f "$FILE"
# 2048 MiB sparse file — above BlockDeviceValidSize (1G)
sudo -n truncate -s 2048M "$FILE"
LOOP=$(sudo -n losetup --find --show "$FILE")
sudo -n udevadm settle || true
sudo -n udevadm trigger --action=change --subsystem-match=block || true
printf '%%s\n' "$LOOP"
`, shellQuote(filePath))

	out, err := framework.NodeExecChecked(ctx, cl, node, script)
	Expect(err).NotTo(HaveOccurred(), "losetup failed: %s", out)
	loopPath = strings.TrimSpace(out)
	Expect(loopPath).To(MatchRegexp(`^/dev/loop[0-9]+$`))
	return loopPath, filePath
}

// bdtypesRemoveLoop detaches a loop device and removes its backing file.
func bdtypesRemoveLoop(ctx context.Context, cl *e2e.Cluster, node, loopPath, filePath string) {
	script := fmt.Sprintf(`set -eu
LOOP=%s
FILE=%s
if [ -n "$LOOP" ] && [ -b "$LOOP" ]; then
  sudo -n losetup -d "$LOOP" || true
fi
if [ -n "$FILE" ]; then
  sudo -n rm -f "$FILE" || true
fi
`, shellQuote(loopPath), shellQuote(filePath))
	if out, err := framework.NodeExecChecked(ctx, cl, node, script); err != nil {
		GinkgoWriter.Printf("bdtypesRemoveLoop %s: %v (%s)\n", loopPath, err, out)
	}
}

// bdtypesAssertNoBDForPath asserts that no BlockDevice CR on node points at path
// for the duration of the wait window (filtered / never discovered).
func bdtypesAssertNoBDForPath(ctx context.Context, cl client.Client, node, path string) {
	GinkgoHelper()
	Consistently(func(g Gomega) {
		var list v1alpha1.BlockDeviceList
		g.Expect(cl.List(ctx, &list)).To(Succeed())
		for i := range list.Items {
			bd := &list.Items[i]
			if bd.Status.NodeName == node && bd.Status.Path == path {
				g.Expect(bd.Status.Path).NotTo(Equal(path),
					"device %s must not have a BlockDevice CR (type=%s consumable=%v)",
					path, bd.Status.Type, bd.Status.Consumable)
			}
		}
	}, bdtypesFilterWaitTimeout, 15*time.Second).Should(Succeed())
}

// bdtypesAssertNoConsumableOfType asserts no consumable BlockDevice of the given
// type appears as a new CR relative to beforeNames.
func bdtypesAssertNoConsumableOfType(
	ctx context.Context,
	cl client.Client,
	node, deviceType string,
	beforeNames map[string]struct{},
) {
	GinkgoHelper()
	Consistently(func(g Gomega) {
		var list v1alpha1.BlockDeviceList
		g.Expect(cl.List(ctx, &list)).To(Succeed())
		var offenders []string
		for i := range list.Items {
			bd := &list.Items[i]
			if bd.Status.NodeName != node || !bd.Status.Consumable {
				continue
			}
			if bd.Status.Type != deviceType {
				continue
			}
			if _, known := beforeNames[bd.Name]; known {
				continue
			}
			offenders = append(offenders, fmt.Sprintf("%s(%s)", bd.Name, bd.Status.Path))
		}
		g.Expect(offenders).To(BeEmpty(),
			"unsupported type %q must not yield new consumable BlockDevices; got %v", deviceType, offenders)
	}, bdtypesFilterWaitTimeout, 15*time.Second).Should(Succeed())
}

// bdtypesNodeSafe converts a node name into a DNS-1123-safe fragment.
func bdtypesNodeSafe(n string) string {
	return strings.ReplaceAll(strings.ReplaceAll(n, ".", "-"), "_", "-")
}

// bdtypesDeleteLVG best-effort deletes an LVMVolumeGroup CR.
func bdtypesDeleteLVG(ctx context.Context, cl client.Client, name string) {
	if cl == nil || name == "" {
		return
	}
	lvg := &v1alpha1.LVMVolumeGroup{}
	if err := cl.Get(ctx, client.ObjectKey{Name: name}, lvg); err != nil {
		return
	}
	if len(lvg.Finalizers) > 0 {
		lvg.Finalizers = nil
		_ = cl.Update(ctx, lvg)
	}
	_ = cl.Delete(ctx, lvg)
}
