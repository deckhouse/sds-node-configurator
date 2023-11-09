package v1alpha1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type LvmVolumeGroupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []LvmVolumeGroup `json:"items"`
}

type LvmVolumeGroup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   LvmVolumeGroupSpec   `json:"spec"`
	Status LvmVolumeGroupStatus `json:"status,omitempty"`
}

type SpecThinPool struct {
	Name string            `json:"name"`
	Size resource.Quantity `json:"size"`
}

type LvmVolumeGroupSpec struct {
	ActualVGNameOnTheNode string         `json:"actualVGNameOnTheNode"`
	BlockDeviceNames      []string       `json:"blockDeviceNames"`
	ThinPools             []SpecThinPool `json:"thinPools"`
	Type                  string         `json:"type"`
}

type LvmVolumeGroupDevice struct {
	BlockDevice string            `json:"blockDevice"`
	DevSize     resource.Quantity `json:"devSize"`
	PVSize      resource.Quantity `json:"pvSize"`
	PVUuid      string            `json:"pvUUID"`
	Path        string            `json:"path"`
}

type LvmVolumeGroupNode struct {
	Devices []LvmVolumeGroupDevice `json:"devices"`
	Name    string                 `json:"name"`
}

type StatusThinPool struct {
	Name       string            `json:"name"`
	ActualSize resource.Quantity `json:"actualSize"`
	UsedSize   string            `json:"usedSize"`
}

type LvmVolumeGroupStatus struct {
	AllocatedSize resource.Quantity    `json:"allocatedSize"`
	Health        string               `json:"health"`
	Message       string               `json:"message"`
	Nodes         []LvmVolumeGroupNode `json:"nodes"`
	ThinPools     []StatusThinPool     `json:"thinPools"`
	VGSize        resource.Quantity    `json:"vgSize"`
	VGUuid        string               `json:"vgUUID"`
}
