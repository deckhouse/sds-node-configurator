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
	"testing"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestLVMVolumeGroupAPIObjects(t *testing.T) {
	t.Run("Unmarshal_LVMVolumeGroup_json_to_struct", func(t *testing.T) {
		js := `{
   "apiVersion": "storage.deckhouse.io/v1alpha1",
   "kind": "LVMVolumeGroup",
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

		expected := v1alpha1.LVMVolumeGroup{
			TypeMeta: metav1.TypeMeta{
				Kind:       "LVMVolumeGroup",
				APIVersion: "storage.deckhouse.io/v1alpha1",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: "lvg-test-1",
			},
			Spec: v1alpha1.LVMVolumeGroupSpec{
				Type:                  "local",
				BlockDeviceNames:      []string{"test-bd", "test-bd2"},
				ActualVGNameOnTheNode: "testVGname",
				ThinPools: []v1alpha1.LVMVolumeGroupThinPoolSpec{
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
			Status: v1alpha1.LVMVolumeGroupStatus{
				VGUuid:        "test-vg-uuid",
				VGSize:        resource.MustParse("30G"),
				AllocatedSize: resource.MustParse("20G"),
				ThinPools: []v1alpha1.LVMVolumeGroupThinPoolStatus{
					{
						Name:       "test-name",
						ActualSize: *convertSize("1G", t),
					},
				},
				Nodes: []v1alpha1.LVMVolumeGroupNode{
					{
						Name: "node1",
						Devices: []v1alpha1.LVMVolumeGroupDevice{
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
						Devices: []v1alpha1.LVMVolumeGroupDevice{
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

		var actual v1alpha1.LVMVolumeGroup
		err := json.Unmarshal([]byte(js), &actual)

		if assert.NoError(t, err) {
			assert.Equal(t, expected, actual)
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
