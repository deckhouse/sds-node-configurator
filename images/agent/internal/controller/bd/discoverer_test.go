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

package bd

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	errors2 "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/cache"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/monitoring"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/test_utils"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

//go:embed testdata/lsblk_output.json
var testLsblkOutput []byte

func setupDiscoverer() *Discoverer {
	opts := DiscovererConfig{
		NodeName:  "test-node",
		MachineID: "test-id",
	}
	cl := test_utils.NewFakeClient()
	metrics := monitoring.GetMetrics(opts.NodeName)
	log, _ := logger.NewLogger("1")
	sdsCache := cache.New()

	return NewDiscoverer(cl, log, metrics, sdsCache, opts)
}

func TestBlockDeviceCtrl(t *testing.T) {
	ctx := context.Background()

	t.Run("GetAPIBlockDevices", func(t *testing.T) {
		t.Run("bds_exist_match_labels_and_expressions_return_bds", func(t *testing.T) {
			const (
				name1    = "name1"
				name2    = "name2"
				name3    = "name3"
				hostName = "test-host"
			)

			d := setupDiscoverer()

			bds := []v1alpha1.BlockDevice{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: name1,
						Labels: map[string]string{
							"kubernetes.io/hostname":      hostName,
							"kubernetes.io/metadata.name": name1,
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: name2,
						Labels: map[string]string{
							"kubernetes.io/hostname":      hostName,
							"kubernetes.io/metadata.name": name2,
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: name3,
						Labels: map[string]string{
							"kubernetes.io/hostname":      hostName,
							"kubernetes.io/metadata.name": name3,
						},
					},
				},
			}

			for _, bd := range bds {
				err := d.cl.Create(ctx, &bd)
				if err != nil {
					t.Error(err)
				}
			}

			defer func() {
				for _, bd := range bds {
					err := d.cl.Delete(ctx, &bd)
					if err != nil {
						t.Error(err)
					}
				}
			}()

			lvg := &v1alpha1.LVMVolumeGroup{
				Spec: v1alpha1.LVMVolumeGroupSpec{
					BlockDeviceSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"kubernetes.io/hostname": hostName,
						},
						MatchExpressions: []metav1.LabelSelectorRequirement{
							{
								Key:      "kubernetes.io/metadata.name",
								Operator: metav1.LabelSelectorOpIn,
								Values:   []string{name1, name2},
							},
						},
					},
				},
			}

			actualBd, err := d.bdCl.GetAPIBlockDevices(ctx, DiscovererName, lvg.Spec.BlockDeviceSelector)
			if assert.NoError(t, err) {
				assert.Equal(t, 2, len(actualBd))

				_, ok := actualBd[name1]
				assert.True(t, ok)
				_, ok = actualBd[name2]
				assert.True(t, ok)
				_, ok = actualBd[name3]
				assert.False(t, ok)
			}
		})

		t.Run("bds_exist_only_match_labels_return_bds", func(t *testing.T) {
			const (
				name1    = "name11"
				name2    = "name22"
				name3    = "name33"
				hostName = "test-host"
			)

			d := setupDiscoverer()

			bds := []v1alpha1.BlockDevice{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: name1,
						Labels: map[string]string{
							"kubernetes.io/hostname":      hostName,
							"kubernetes.io/metadata.name": name1,
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: name2,
						Labels: map[string]string{
							"kubernetes.io/hostname":      hostName,
							"kubernetes.io/metadata.name": name2,
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: name3,
						Labels: map[string]string{
							"kubernetes.io/hostname":      "other-host",
							"kubernetes.io/metadata.name": name3,
						},
					},
				},
			}

			for _, bd := range bds {
				err := d.cl.Create(ctx, &bd)
				if err != nil {
					t.Error(err)
				}
			}

			defer func() {
				for _, bd := range bds {
					err := d.cl.Delete(ctx, &bd)
					if err != nil {
						t.Error(err)
					}
				}
			}()

			lvg := &v1alpha1.LVMVolumeGroup{
				Spec: v1alpha1.LVMVolumeGroupSpec{
					BlockDeviceSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"kubernetes.io/hostname": hostName},
					},
				},
			}

			actualBd, err := d.bdCl.GetAPIBlockDevices(ctx, DiscovererName, lvg.Spec.BlockDeviceSelector)
			if assert.NoError(t, err) {
				assert.Equal(t, 2, len(actualBd))

				_, ok := actualBd[name1]
				assert.True(t, ok)
				_, ok = actualBd[name2]
				assert.True(t, ok)
				_, ok = actualBd[name3]
				assert.False(t, ok)
			}
		})

		t.Run("bds_exist_only_match_expressions_return_bds", func(t *testing.T) {
			const (
				name1    = "name111"
				name2    = "name222"
				name3    = "name333"
				hostName = "test-host"
			)

			d := setupDiscoverer()

			bds := []v1alpha1.BlockDevice{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: name1,
						Labels: map[string]string{
							"kubernetes.io/hostname":      hostName,
							"kubernetes.io/metadata.name": name1,
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: name2,
						Labels: map[string]string{
							"kubernetes.io/hostname":      hostName,
							"kubernetes.io/metadata.name": name2,
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: name3,
						Labels: map[string]string{
							"kubernetes.io/hostname":      hostName,
							"kubernetes.io/metadata.name": name3,
						},
					},
				},
			}

			for _, bd := range bds {
				err := d.cl.Create(ctx, &bd)
				if err != nil {
					t.Error(err)
				}
			}

			defer func() {
				for _, bd := range bds {
					err := d.cl.Delete(ctx, &bd)
					if err != nil {
						t.Error(err)
					}
				}
			}()

			lvg := &v1alpha1.LVMVolumeGroup{
				Spec: v1alpha1.LVMVolumeGroupSpec{
					BlockDeviceSelector: &metav1.LabelSelector{
						MatchExpressions: []metav1.LabelSelectorRequirement{
							{
								Key:      "kubernetes.io/metadata.name",
								Operator: metav1.LabelSelectorOpIn,
								Values:   []string{name1, name2},
							},
						},
					},
				},
			}

			actualBd, err := d.bdCl.GetAPIBlockDevices(ctx, DiscovererName, lvg.Spec.BlockDeviceSelector)
			if assert.NoError(t, err) {
				assert.Equal(t, 2, len(actualBd))
				_, ok := actualBd[name1]
				assert.True(t, ok)
				_, ok = actualBd[name2]
				assert.True(t, ok)
				_, ok = actualBd[name3]
				assert.False(t, ok)
			}
		})
	})

	t.Run("shouldDeleteBlockDevice", func(t *testing.T) {
		t.Run("returns_true", func(t *testing.T) {
			bd := v1alpha1.BlockDevice{
				Status: v1alpha1.BlockDeviceStatus{
					NodeName:   "node",
					Consumable: true,
				},
			}
			actual := map[string]struct{}{}

			assert.True(t, shouldDeleteBlockDevice(bd, actual, "node"))
		})

		t.Run("returns_false_cause_of_dif_node", func(t *testing.T) {
			bd := v1alpha1.BlockDevice{
				Status: v1alpha1.BlockDeviceStatus{
					NodeName:   "node",
					Consumable: true,
				},
			}
			actual := map[string]struct{}{}

			assert.False(t, shouldDeleteBlockDevice(bd, actual, "dif-node"))
		})

		t.Run("returns_false_cause_of_not_consumable", func(t *testing.T) {
			bd := v1alpha1.BlockDevice{
				Status: v1alpha1.BlockDeviceStatus{
					NodeName:   "node",
					Consumable: false,
				},
			}
			actual := map[string]struct{}{}

			assert.False(t, shouldDeleteBlockDevice(bd, actual, "node"))
		})

		t.Run("returns_false_cause_of_not_deprecated", func(t *testing.T) {
			const name = "test"
			bd := v1alpha1.BlockDevice{
				ObjectMeta: metav1.ObjectMeta{
					Name: name,
				},
				Status: v1alpha1.BlockDeviceStatus{
					NodeName:   "node",
					Consumable: true,
				},
			}
			actual := map[string]struct{}{
				name: {},
			}

			assert.False(t, shouldDeleteBlockDevice(bd, actual, "node"))
		})
	})

	t.Run("RemoveDeprecatedAPIDevices", func(t *testing.T) {
		const (
			goodName = "test-candidate1"
			badName  = "test-candidate2"
		)

		d := setupDiscoverer()

		candidates := []internal.BlockDeviceCandidate{
			{
				NodeName:              d.cfg.NodeName,
				Consumable:            false,
				PVUuid:                "142412421",
				VGUuid:                "123123123",
				LVMVolumeGroupName:    "test-lvg",
				ActualVGNameOnTheNode: "test-vg",
				Wwn:                   "12414212",
				Serial:                "1412412412412",
				Path:                  "/dev/vdb",
				Size:                  resource.MustParse("1G"),
				Rota:                  false,
				Model:                 "124124-adf",
				Name:                  goodName,
				HotPlug:               false,
				MachineID:             "1245151241241",
			},
		}

		bds := map[string]v1alpha1.BlockDevice{
			goodName: {
				ObjectMeta: metav1.ObjectMeta{
					Name: goodName,
				},
			},
			badName: {
				ObjectMeta: metav1.ObjectMeta{
					Name: badName,
				},
				Status: v1alpha1.BlockDeviceStatus{
					Consumable: true,
					NodeName:   d.cfg.NodeName,
				},
			},
		}

		for _, bd := range bds {
			err := d.cl.Create(ctx, &bd)
			if err != nil {
				t.Error(err)
			}
		}

		defer func() {
			for _, bd := range bds {
				_ = d.cl.Delete(ctx, &bd)
			}
		}()

		for _, bd := range bds {
			createdBd := &v1alpha1.BlockDevice{}
			err := d.cl.Get(ctx, client.ObjectKey{
				Name: bd.Name,
			}, createdBd)
			if err != nil {
				t.Error(err)
			}
			assert.Equal(t, bd.Name, createdBd.Name)
		}

		d.removeDeprecatedAPIDevices(ctx, candidates, bds)

		_, ok := bds[badName]
		assert.False(t, ok)

		deleted := &v1alpha1.BlockDevice{}
		err := d.cl.Get(ctx, client.ObjectKey{
			Name: badName,
		}, deleted)
		if assert.True(t, errors2.IsNotFound(err)) {
			assert.Equal(t, "", deleted.Name)
		}
	})

	t.Run("GetBlockDeviceCandidates", func(t *testing.T) {
		devices := []internal.Device{
			{
				Name:   "valid1",
				KName:  "/dev/kname1",
				Size:   resource.MustParse("1G"),
				Serial: "131412",
			},
			{
				Name:   "valid2",
				KName:  "/dev/kname2",
				Size:   resource.MustParse("1G"),
				Serial: "12412412",
			},
			{
				Name:   "valid3",
				KName:  "/dev/kname3",
				Size:   resource.MustParse("1G"),
				Serial: "4214215",
			},
			{
				Name:   "invalid",
				KName:  "/dev/kname4",
				FSType: "ext4",
				Size:   resource.MustParse("1G"),
			},
		}

		d := setupDiscoverer()

		d.sdsCache.StoreDevices(devices, bytes.Buffer{})

		candidates, err := d.getBlockDeviceCandidates()
		assert.Equal(t, nil, err)
		assert.Equal(t, 3, len(candidates))
		for i := range candidates {
			assert.Equal(t, devices[i].Name, candidates[i].Path)
			assert.Equal(t, d.cfg.MachineID, candidates[i].MachineID)
			assert.Equal(t, d.cfg.NodeName, candidates[i].NodeName)
		}
	})

	t.Run("CreateUniqDeviceName", func(t *testing.T) {
		nodeName := "testNode"
		can := internal.BlockDeviceCandidate{
			NodeName: nodeName,
			Wwn:      "ZX128ZX128ZX128",
			Path:     "/dev/sda",
			Size:     resource.Quantity{},
			Model:    "HARD-DRIVE",
		}

		deviceName := createUniqDeviceName(can)
		assert.Equal(t, "dev-", deviceName[0:4], "device name does not start with dev-")
		assert.Equal(t, len(deviceName[4:]), 40, "device name does not contains sha1 sum")
	})

	t.Run("CheckTag", func(t *testing.T) {
		t.Run("Have tag_Returns true and tag", func(t *testing.T) {
			expectedName := "testName"
			tags := fmt.Sprintf("storage.deckhouse.io/enabled=true,storage.deckhouse.io/lvmVolumeGroupName=%s", expectedName)

			shouldBeTrue, actualName := utils.ReadValueFromTags(tags, internal.LVMVolumeGroupTag)
			if assert.True(t, shouldBeTrue) {
				assert.Equal(t, expectedName, actualName)
			}
		})

		t.Run("Haven't tag_Returns false and empty", func(t *testing.T) {
			tags := "someWeirdTags=oMGwtFIsThis"

			shouldBeFalse, actualName := utils.ReadValueFromTags(tags, internal.LVMVolumeGroupTag)
			if assert.False(t, shouldBeFalse) {
				assert.Equal(t, "", actualName)
			}
		})
	})

	t.Run("hasValidSize", func(t *testing.T) {
		sizes := []string{"2G", "1G", "1.5G", "0.9G", "100M"}
		expected := []bool{true, true, true, false, false}

		for i, size := range sizes {
			s, err := resource.ParseQuantity(size)
			if assert.NoError(t, err) {
				valid, err := hasValidSize(s)
				if assert.NoError(t, err) {
					assert.Equal(t, expected[i], valid)
				}
			}
		}
	})

	t.Run("validateTestLSBLKOutput", func(t *testing.T) {
		d := setupDiscoverer()
		devices, err := utils.NewCommands().UnmarshalDevices(testLsblkOutput)
		if assert.NoError(t, err) {
			assert.Equal(t, 31, len(devices))
		}
		devicesClone := slices.Clone(devices)
		filteredDevices, err := d.filterDevices(devices)
		assert.True(t, slices.Equal(devicesClone, devices), "filterDevices should not change original device list")

		for i, device := range filteredDevices {
			println("Filtered device: ", device.Name)
			candidate := internal.NewBlockDeviceCandidateByDevice(&device, "test-node", "test-machine")
			switch i {
			case 0:
				assert.Equal(t, "/dev/md1", device.Name)
				assert.False(t, candidate.Consumable)
			case 1:
				assert.Equal(t, "/dev/md127", device.Name)
				assert.False(t, candidate.Consumable)
			case 2:
				assert.Equal(t, "/dev/nvme4n1", device.Name)
				assert.True(t, candidate.Consumable)
				candidateName := d.createCandidateName(candidate, devices)
				assert.Equal(t, "dev-794d93d177d16bc9a85e2dd2ccbdc7325c287374", candidateName, "generated device name unstable with previous release. Don't fix the test. Fix the code.")
			case 3:
				assert.Equal(t, "/dev/nvme5n1", device.Name)
				assert.True(t, candidate.Consumable)
				candidateName := d.createCandidateName(candidate, devices)
				assert.Equal(t, "dev-3306e773ab3cde6d519ce8d7c3686bf17a124dcb", candidateName, "generated device name unstable with previous release. Don't fix the test. Fix the code.")
			case 4:
				assert.Equal(t, "/dev/sdb4", device.Name)
				assert.False(t, candidate.Consumable)
				candidateName := d.createCandidateName(candidate, devices)
				assert.Equal(t, "dev-377bc6adf33d84eb5932f5c89798bb6c5949ae2d", candidateName, "generated device name unstable with previous release. Don't fix the test. Fix the code.")
			case 5:
				assert.Equal(t, "/dev/vdc1", device.Name)
				assert.True(t, candidate.Consumable)
				candidateName := d.createCandidateName(candidate, devices)
				assert.Equal(t, "dev-a9d768213aaead8b42465ec859189de8779f96b7", candidateName, "generated device name unstable with previous release. Don't fix the test. Fix the code.")
			case 6:
				assert.Equal(t, "/dev/mapper/mpatha", device.Name)
				assert.True(t, candidate.Consumable)
				candidateName := d.createCandidateName(candidate, devices)
				assert.Equal(t, "dev-98ca88ddaaddec43b1c4894756f4856244985511", candidateName, "generated device name unstable with previous release. Don't fix the test. Fix the code.")
			}
		}

		if assert.NoError(t, err) {
			assert.Equal(t, 7, len(filteredDevices))
		}
	})
}
