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

package v1alpha1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type LvmLogicalVolumeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []LvmLogicalVolume `json:"items"`
}

type LvmLogicalVolume struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   LvmLogicalVolumeSpec   `json:"spec"`
	Status LvmLogicalVolumeStatus `json:"status,omitempty"`
}

type LvmLogicalVolumeSpec struct {
	Type           string                `json:"type"`
	Size           resource.Quantity     `json:"size"`
	LvmVolumeGroup string                `json:"lvmVolumeGroup"`
	Thin           ThinLogicalVolumeSpec `json:"thin"`
}

type ThinLogicalVolumeSpec struct {
	PoolName string `json:"poolName"`
}

type LvmLogicalVolumeStatus struct {
	Phase      string            `json:"phase"`
	Reason     string            `json:"reason"`
	ActualSize resource.Quantity `json:"actualSize"`
}
