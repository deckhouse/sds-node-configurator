package controller

import (
	"encoding/json"
	"sds-node-configurator/api/v1alpha1"
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestLvmVolumeGroupAPIObjects(t *testing.T) {
	t.Run("Unmarshal_LvmVolumeGroup_json_to_struct", func(t *testing.T) {
		js := `{
    "apiVersion": "storage.deckhouse.io/v1alpha1",
    "kind": "LvmVolumeGroup",
    "metadata": {
        "name": "lvg-test-1"
    },
    "spec": {
        "actualVGNameOnTheNode": "testVGname",
        "blockDeviceNames": [
            "test-bd",
            "test-bd2"
        ],
        "thinPools": [
            {
                "name": "test-name",
                "size": "10G"
            },
            {
                "name": "test-name2",
                "size": "1G"
            }
        ],
        "type": "local"
    },
    "status": {
        "allocatedSize": "20G",
        "health": "operational",
        "message": "all-good",
        "nodes": [
            {
                "devices": [
                    {
                        "blockDevice": "test/BD",
                        "devSize": "1G",
                        "path": "test/path1",
                        "pvSize": "1G",
                        "pvUUID": "testPV1"
                    },
                    {
                        "blockDevice": "test/BD2",
                        "devSize": "1G",
                        "path": "test/path2",
                        "pvSize": "2G",
                        "pvUUID": "testPV2"
                    }
                ],
                "name": "node1"
            },
            {
                "devices": [
                    {
                        "blockDevice": "test/DB3",
                        "devSize": "2G",
                        "path": "test/path3",
                        "pvSize": "3G",
                        "pvUUID": "testPV3"
                    }
                ],
                "name": "node2"
            }
        ],
        "thinPools": [
			{
                "name": "test-name",
                "actualSize": "1G"
            }
		],
        "vgSize": "test-vg-size",
        "vgUUID": "test-vg-uuid"
    }
}`
		expected := v1alpha1.LvmVolumeGroup{
			TypeMeta: metav1.TypeMeta{
				Kind:       "LvmVolumeGroup",
				APIVersion: "storage.deckhouse.io/v1alpha1",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: "lvg-test-1",
			},
			Spec: v1alpha1.LvmVolumeGroupSpec{
				Type:                  "local",
				BlockDeviceNames:      []string{"test-bd", "test-bd2"},
				ActualVGNameOnTheNode: "testVGname",
				ThinPools: []v1alpha1.SpecThinPool{
					{
						Name: "test-name",
						Size: "10G",
					},
					{
						Name: "test-name2",
						Size: "1G",
					},
				},
			},
			Status: v1alpha1.LvmVolumeGroupStatus{
				Health:        "operational",
				Message:       "all-good",
				VGUuid:        "test-vg-uuid",
				VGSize:        "test-vg-size",
				AllocatedSize: "20G",
				ThinPools: []v1alpha1.StatusThinPool{
					{
						Name:       "test-name",
						ActualSize: "1G",
					},
				},
				Nodes: []v1alpha1.LvmVolumeGroupNode{
					{
						Name: "node1",
						Devices: []v1alpha1.LvmVolumeGroupDevice{
							{
								Path:        "test/path1",
								PVSize:      "1G",
								DevSize:     "1G",
								PVUuid:      "testPV1",
								BlockDevice: "test/BD",
							},
							{
								Path:        "test/path2",
								PVSize:      "2G",
								DevSize:     "1G",
								PVUuid:      "testPV2",
								BlockDevice: "test/BD2",
							},
						},
					},
					{
						Name: "node2",
						Devices: []v1alpha1.LvmVolumeGroupDevice{
							{
								Path:        "test/path3",
								PVSize:      "3G",
								DevSize:     "2G",
								PVUuid:      "testPV3",
								BlockDevice: "test/DB3",
							},
						},
					},
				},
			},
		}

		var actual v1alpha1.LvmVolumeGroup
		err := json.Unmarshal([]byte(js), &actual)

		if assert.NoError(t, err) {
			assert.Equal(t, expected, actual)
		}
	})

	t.Run("Marshal_LvmVolumeGroup_struct_to_json", func(t *testing.T) {
		expected := `{
    "apiVersion": "storage.deckhouse.io/v1alpha1",
    "kind": "LvmVolumeGroup",
    "metadata": {
        "creationTimestamp": null,
        "name": "lvg-test-1"
    },
    "spec": {
        "actualVGNameOnTheNode": "testVGname",
        "blockDeviceNames": [
            "test-bd",
            "test-bd2"
        ],
        "thinPools": [
            {
                "name": "test-name",
                "size": "10G"
            },
            {
                "name": "test-name2",
                "size": "1G"
            }
        ],
        "type": "local"
    },
    "status": {
        "allocatedSize": "20G",
        "health": "operational",
        "message": "all-good",
        "nodes": [
            {
                "devices": [
                    {
                        "blockDevice": "test/BD",
                        "devSize": "1G",
                        "path": "test/path1",
                        "pvSize": "1G",
                        "pvUUID": "testPV1"
                    },
                    {
                        "blockDevice": "test/BD2",
                        "devSize": "1G",
                        "path": "test/path2",
                        "pvSize": "2G",
                        "pvUUID": "testPV2"
                    }
                ],
                "name": "node1"
            },
            {
                "devices": [
                    {
                        "blockDevice": "test/DB3",
                        "devSize": "2G",
                        "path": "test/path3",
                        "pvSize": "3G",
                        "pvUUID": "testPV3"
                    }
                ],
                "name": "node2"
            }
        ],
        "thinPools": [
            {
                "name": "test-name",
                "actualSize": "1G",
				"usedSize": "500M"
            }
        ],
        "vgSize": "test-vg-size",
        "vgUUID": "test-vg-uuid"
    }
}`
		testObj := v1alpha1.LvmVolumeGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "lvg-test-1",
				CreationTimestamp: metav1.Time{},
			},
			TypeMeta: metav1.TypeMeta{
				Kind:       "LvmVolumeGroup",
				APIVersion: "storage.deckhouse.io/v1alpha1",
			},
			Spec: v1alpha1.LvmVolumeGroupSpec{
				ActualVGNameOnTheNode: "testVGname",
				BlockDeviceNames:      []string{"test-bd", "test-bd2"},
				ThinPools: []v1alpha1.SpecThinPool{
					{
						Name: "test-name",
						Size: "10G",
					},
					{
						Name: "test-name2",
						Size: "1G",
					},
				},
				Type: "local",
			},
			Status: v1alpha1.LvmVolumeGroupStatus{
				AllocatedSize: "20G",
				Health:        "operational",
				Message:       "all-good",
				Nodes: []v1alpha1.LvmVolumeGroupNode{
					{
						Devices: []v1alpha1.LvmVolumeGroupDevice{
							{
								BlockDevice: "test/BD",
								DevSize:     "1G",
								PVSize:      "1G",
								PVUuid:      "testPV1",
								Path:        "test/path1",
							},
							{
								BlockDevice: "test/BD2",
								DevSize:     "1G",
								PVSize:      "2G",
								PVUuid:      "testPV2",
								Path:        "test/path2",
							},
						},
						Name: "node1",
					},
					{
						Devices: []v1alpha1.LvmVolumeGroupDevice{
							{
								BlockDevice: "test/DB3",
								DevSize:     "2G",
								PVSize:      "3G",
								PVUuid:      "testPV3",
								Path:        "test/path3",
							},
						},
						Name: "node2",
					},
				},
				ThinPools: []v1alpha1.StatusThinPool{
					{
						Name:       "test-name",
						ActualSize: "1G",
						UsedSize:   "500M",
					},
				},
				VGSize: "test-vg-size",
				VGUuid: "test-vg-uuid",
			},
		}

		actual, err := json.Marshal(testObj)

		if assert.NoError(t, err) {
			assert.JSONEq(t, expected, string(actual))
		}
	})
}
