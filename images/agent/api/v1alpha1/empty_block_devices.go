package v1alpha1

import (
	"k8s.io/api/policy/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// BlockDevice empty block device
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type BlockDevice struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Status BlockDeviceStatus `json:"status,omitempty"`
}

// BlockDeviceList contains a list of empty block device
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type BlockDeviceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`
	Items           []BlockDevice `json:"items"`
}

type BlockDeviceStatus struct {
	Type                  string         `json:"type"`
	FsType                v1beta1.FSType `json:"fsType,omitempty"`
	NodeName              string         `json:"nodeName"`
	Consumable            bool           `json:"consumable"`
	PVUuid                string         `json:"pvUUID,omitempty"`
	VGUuid                string         `json:"vgUUID,omitempty"`
	LvmVolumeGroupName    string         `json:"lvmVolumeGroupName"`
	ActualVGnameOnTheNode string         `json:"actualVGNameOnTheNode"`
	Wwn                   string         `json:"wwn"`
	Serial                string         `json:"serial"`
	Path                  string         `json:"path"`
	Size                  string         `json:"size"`
	Model                 string         `json:"model"`
	Rota                  bool           `json:"rota"`
	MachineID             string         `json:"machineId"`
}
