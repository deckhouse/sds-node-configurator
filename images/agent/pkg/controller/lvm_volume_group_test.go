/*
Copyright 2023 Flant JSC

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

package controller

import (
	"encoding/json"
	"k8s.io/apimachinery/pkg/api/resource"
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
       "vgSize": "30G",
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
				ThinPools: []v1alpha1.LVGSpecThinPool{
					{
						Name: "test-name",
						Size: *convertSize("10G", t),
					},
					{
						Name: "test-name2",
						Size: *convertSize("1G", t),
					},
				},
			},
			Status: v1alpha1.LvmVolumeGroupStatus{
				VGUuid:        "test-vg-uuid",
				VGSize:        resource.MustParse("30G"),
				AllocatedSize: resource.MustParse("20G"),
				ThinPools: []v1alpha1.LVGStatusThinPool{
					{
						Name:       "test-name",
						ActualSize: *convertSize("1G", t),
					},
				},
				Nodes: []v1alpha1.LvmVolumeGroupNode{
					{
						Name: "node1",
						Devices: []v1alpha1.LvmVolumeGroupDevice{
							{
								Path:        "test/path1",
								PVSize:      resource.MustParse("1G"),
								DevSize:     *convertSize("1G", t),
								PVUuid:      "testPV1",
								BlockDevice: "test/BD",
							},
							{
								Path:        "test/path2",
								PVSize:      resource.MustParse("2G"),
								DevSize:     *convertSize("1G", t),
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
								PVSize:      resource.MustParse("3G"),
								DevSize:     *convertSize("2G", t),
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
       "conditions": null,
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
       "phase": "",
       "thinPools": [
           {
               "name": "test-name",
               "actualSize": "1G",
				"usedSize": "500M",
               "ready": true,
               "message": ""
           }
       ],
       "vgSize": "30G",
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
				ThinPools: []v1alpha1.LVGSpecThinPool{
					{
						Name: "test-name",
						Size: *convertSize("10G", t),
					},
					{
						Name: "test-name2",
						Size: *convertSize("1G", t),
					},
				},
				Type: "local",
			},
			Status: v1alpha1.LvmVolumeGroupStatus{
				AllocatedSize: resource.MustParse("20G"),
				Nodes: []v1alpha1.LvmVolumeGroupNode{
					{
						Devices: []v1alpha1.LvmVolumeGroupDevice{
							{
								BlockDevice: "test/BD",
								DevSize:     *convertSize("1G", t),
								PVSize:      resource.MustParse("1G"),
								PVUuid:      "testPV1",
								Path:        "test/path1",
							},
							{
								BlockDevice: "test/BD2",
								DevSize:     *convertSize("1G", t),
								PVSize:      resource.MustParse("2G"),
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
								DevSize:     *convertSize("2G", t),
								PVSize:      resource.MustParse("3G"),
								PVUuid:      "testPV3",
								Path:        "test/path3",
							},
						},
						Name: "node2",
					},
				},
				ThinPools: []v1alpha1.LVGStatusThinPool{
					{
						Name:       "test-name",
						ActualSize: *convertSize("1G", t),
						UsedSize:   resource.MustParse("500M"),
						Ready:      true,
						Message:    "",
					},
				},
				VGSize: resource.MustParse("30G"),
				VGUuid: "test-vg-uuid",
			},
		}

		actual, err := json.Marshal(testObj)

		if assert.NoError(t, err) {
			assert.JSONEq(t, expected, string(actual))
		}
	})
}

func convertSize(size string, t *testing.T) *resource.Quantity {
	sizeQTB, err := resource.ParseQuantity(size)
	if err != nil {
		t.Error(err)
	}

	return &sizeQTB
}
