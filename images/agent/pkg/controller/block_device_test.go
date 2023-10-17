package controller_test

import (
	"storage-configurator/internal"
	"testing"

	"github.com/stretchr/testify/assert"
	"storage-configurator/pkg/controller"
)

func TestGetCandidates(t *testing.T) {
	t.Run("CheckConsumable", func(t *testing.T) {
		t.Run("Good device returns true", func(t *testing.T) {
			goodDevice := internal.Device{
				Name:       "goodName",
				MountPoint: "",
				PartUUID:   "",
				HotPlug:    false,
				Model:      "",
				Serial:     "",
				Size:       "",
				Type:       "",
				Wwn:        "",
				KName:      "",
				PkName:     "",
				FSType:     "",
				Rota:       false,
			}

			shouldBeTrue := controller.CheckConsumable(goodDevice)
			assert.True(t, shouldBeTrue)
		})

		t.Run("Bad devices return false", func(t *testing.T) {
			badDevices := internal.Devices{BlockDevices: []internal.Device{
				{Name: internal.DRBDName + "badName",
					MountPoint: "",
					PartUUID:   "",
					HotPlug:    false,
					Model:      "",
					Serial:     "",
					Size:       "",
					Type:       "",
					Wwn:        "",
					KName:      "",
					PkName:     "",
					FSType:     "",
					Rota:       false},
				{
					Name:       "goodName",
					MountPoint: "badMountPoint",
					PartUUID:   "",
					HotPlug:    false,
					Model:      "",
					Serial:     "",
					Size:       "",
					Type:       "",
					Wwn:        "",
					KName:      "",
					PkName:     "",
					FSType:     "",
					Rota:       false,
				},
				{
					Name:       "goodName",
					MountPoint: "",
					PartUUID:   "",
					HotPlug:    false,
					Model:      "",
					Serial:     "",
					Size:       "",
					Type:       "",
					Wwn:        "",
					KName:      "",
					PkName:     "",
					FSType:     "badFSType",
					Rota:       false,
				},
				{
					Name:       "goodName",
					MountPoint: "",
					PartUUID:   "",
					HotPlug:    true,
					Model:      "",
					Serial:     "",
					Size:       "",
					Type:       "",
					Wwn:        "",
					KName:      "",
					PkName:     "",
					FSType:     "",
					Rota:       false,
				},
			}}

			for _, badDevice := range badDevices.BlockDevices {
				shouldBeFalse := controller.CheckConsumable(badDevice)
				assert.False(t, shouldBeFalse)
			}
		})
	})

	t.Run("CreateUniqDeviceName", func(t *testing.T) {
		nodeName := "testNode"
		can := internal.Candidate{
			NodeName: nodeName,
			Wwn:      "ZX128ZX128ZX128",
			Path:     "/dev/sda",
			Size:     "4Gb",
			Model:    "HARD-DRIVE",
		}

		deviceName := controller.CreateUniqDeviceName(can)
		assert.Equal(t, "dev-", deviceName[0:4], "device name does not start with dev-")
		assert.Equal(t, len(deviceName[4:]), 40, "device name does not contains sha1 sum")
	})

	t.Run("CheckTag", func(t *testing.T) {
		t.Run("Have tag_Returns true and tag", func(t *testing.T) {
			expectedName := "testName"
			tags := "storage.deckhouse.io/enabled=true,storage.deckhouse.io/lvmVolumeGroupName=" + expectedName

			shouldBeTrue, actualName := controller.CheckTag(tags)
			if assert.True(t, shouldBeTrue) {
				assert.Equal(t, expectedName, actualName)
			}
		})

		t.Run("Haven't tag_Returns false and empty", func(t *testing.T) {
			tags := "someWeirdTags=oMGwtFIsThis"

			shouldBeFalse, actualName := controller.CheckTag(tags)
			if assert.False(t, shouldBeFalse) {
				assert.Equal(t, "", actualName)
			}
		})
	})
}
