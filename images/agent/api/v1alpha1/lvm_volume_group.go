package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

type LvmVolumeGroupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []LvmVolumeGroup `json:"items"`
}

type LvmVolumeGroup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   LvmVolumeGroupSpec   `json:"spec"`
	Status LvmVolumeGroupStatus `json:"status"`
}

type ThinPool struct {
	Name string `json:"name"`
	Size string `json:"size"`
}

type LvmVolumeGroupSpec struct {
	ActualVGNameOnTheNode string     `json:"actualVGNameOnTheNode"`
	BlockDeviceNames      []string   `json:"blockDeviceNames"`
	ThinPools             []ThinPool `json:"thinPools,omitempty"`
	Type                  string     `json:"type"`
}

type LvmVolumeGroupDevice struct {
	BlockDevice string `json:"blockDevice"`
	DevSize     string `json:"devSize"`
	PVSize      string `json:"pvSize"`
	PVUuid      string `json:"pvUUID"`
	Path        string `json:"path"`
}

type LvmVolumeGroupNode struct {
	Devices []LvmVolumeGroupDevice `json:"devices"`
	Name    string                 `json:"name"`
}

type LvmVolumeGroupStatus struct {
	AllocatedSize  string               `json:"allocatedSize"`
	AllocationType string               `json:"allocationType"`
	Health         string               `json:"health"`
	Message        string               `json:"message"`
	Nodes          []LvmVolumeGroupNode `json:"nodes"`
	ThinPoolSize   string               `json:"thinPoolSize"`
	VGSize         string               `json:"vgSize"`
	VGUuid         string               `json:"vgUUID"`
}
