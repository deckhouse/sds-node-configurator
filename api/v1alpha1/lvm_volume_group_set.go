/*
Copyright 2024 Flant JSC

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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type LVMVolumeGroupSetList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []LVMVolumeGroupSet `json:"items"`
}

type LVMVolumeGroupSet struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   LVMVolumeGroupSetSpec   `json:"spec"`
	Status LVMVolumeGroupSetStatus `json:"status,omitempty"`
}

type LVMVolumeGroupSetSpec struct {
	NodeSelector *metav1.LabelSelector  `json:"nodeSelector"`
	LVGTemplate  LVMVolumeGroupTemplate `json:"lvmVolumeGroupTemplate"`
	Strategy     string                 `json:"strategy"`
}
type LVMVolumeGroupTemplate struct {
	Metadata              LVMVolumeGroupTemplateMeta   `json:"metadata"`
	BlockDeviceSelector   *metav1.LabelSelector        `json:"blockDeviceSelector"`
	ActualVGNameOnTheNode string                       `json:"actualVGNameOnTheNode"`
	ThinPools             []LVMVolumeGroupThinPoolSpec `json:"thinPools"`
	Type                  string                       `json:"type"`
}

type LVMVolumeGroupTemplateMeta struct {
	Labels map[string]string `json:"labels"`
}

type LVMVolumeGroupSetStatus struct {
	CreatedLVGs                 []LVMVolumeGroupSetStatusLVG `json:"createdLVMVolumeGroups"`
	CurrentLVMVolumeGroupsCount int                          `json:"currentLVMVolumeGroupsCount"`
	DesiredLVMVolumeGroupsCount int                          `json:"desiredLVMVolumeGroupsCount"`
	Phase                       string                       `json:"phase"`
	Reason                      string                       `json:"reason"`
}

type LVMVolumeGroupSetStatusLVG struct {
	LVMVolumeGroupName string `json:"lvmVolumeGroupName"`
	NodeName           string `json:"nodeName"`
}
