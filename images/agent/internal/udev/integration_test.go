//go:build integration

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

package udev

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"testing"

	"github.com/pilebones/go-udev/crawler"
	"github.com/pilebones/go-udev/netlink"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
)

// TestSnapshotMatchesLsblk fills a DeviceMap from the udev crawler,
// takes a Snapshot, and compares the result with lsblk output.
// Run on a Linux VM with: go test -tags integration -run TestSnapshotMatchesLsblk -v ./internal/udev/
func TestSnapshotMatchesLsblk(t *testing.T) {
	matcher := &netlink.RuleDefinitions{
		Rules: []netlink.RuleDefinition{
			{Env: map[string]string{"SUBSYSTEM": "block"}},
		},
	}

	crawlerQueue := make(chan crawler.Device)
	crawlerErrors := make(chan error)
	crawlerQuit := crawler.ExistingDevices(crawlerQueue, crawlerErrors, matcher)

	var crawlerDevices []crawler.Device
	done := false
	for !done {
		select {
		case dev, open := <-crawlerQueue:
			if !open {
				done = true
			} else {
				crawlerDevices = append(crawlerDevices, dev)
			}
		case err := <-crawlerErrors:
			t.Logf("crawler error: %v", err)
		}
	}
	close(crawlerQuit)

	dm := NewDeviceMap()
	dm.FillFromCrawler(crawlerDevices)

	t.Logf("Crawler found %d raw devices, DeviceMap has %d entries", len(crawlerDevices), dm.Len())

	snapshot := dm.Snapshot()
	t.Logf("Snapshot produced %d devices", len(snapshot))
	require.NotEmpty(t, snapshot, "Snapshot should find at least one device on a real system")

	lsblkDevices := getLsblkDevices(t)
	lsblkMap := make(map[string]internal.Device, len(lsblkDevices))
	for _, d := range lsblkDevices {
		lsblkMap[d.KName] = d
	}

	for _, dev := range snapshot {
		t.Run(dev.KName, func(t *testing.T) {
			lsblkDev, found := lsblkMap[dev.KName]
			if !found {
				t.Logf("device %s from Snapshot not found in lsblk output (may be expected for some DM devices)", dev.KName)
				return
			}

			assert.Equal(t, lsblkDev.Type, dev.Type, "Type mismatch")
			assert.Equal(t, lsblkDev.Rota, dev.Rota, "Rota mismatch")
			assert.Equal(t, lsblkDev.FSType, dev.FSType, "FSType mismatch")

			sizeDiff := dev.Size.Value() - lsblkDev.Size.Value()
			if sizeDiff < 0 {
				sizeDiff = -sizeDiff
			}
			assert.LessOrEqual(t, sizeDiff, int64(4096), "Size difference too large")

			t.Logf("  Name=%s KName=%s Type=%s Size=%s Rota=%v Model=%s Serial=%s",
				dev.Name, dev.KName, dev.Type, dev.Size.String(), dev.Rota, dev.Model, dev.Serial)
		})
	}
}

func getLsblkDevices(t *testing.T) []internal.Device {
	t.Helper()
	out, err := exec.Command("lsblk", "-J", "-lpfb", "-no",
		"name,MOUNTPOINT,PARTUUID,HOTPLUG,MODEL,SERIAL,SIZE,FSTYPE,TYPE,WWN,KNAME,PKNAME,ROTA").Output()
	require.NoError(t, err, "lsblk command failed")

	var devices internal.Devices
	require.NoError(t, json.Unmarshal(out, &devices), "failed to unmarshal lsblk output")
	t.Logf("lsblk returned %d devices", len(devices.BlockDevices))

	for i, d := range devices.BlockDevices {
		fmt.Printf("lsblk[%d]: Name=%s KName=%s Type=%s Size=%s\n", i, d.Name, d.KName, d.Type, d.Size.String())
	}

	return devices.BlockDevices
}
