package v2alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// EmptyBlockDevice empty block device
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type EmptyBlockDevice struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Status BlockDevicesStatus `json:"status,omitempty"`
}

// EmptyBlockDeviceList contains a list of empty block device
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type EmptyBlockDeviceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`
	Items           []EmptyBlockDeviceList `json:"items"`
}

type BlockDevicesStatus struct {
	NodeName string `json:"nodename"`
	ID       string `json:"id"`
	Path     string `json:"path"`
	Size     string `json:"size"`
	Model    string `json:"model"`
}
