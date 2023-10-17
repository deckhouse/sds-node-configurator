package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	BlockDeviceKind           = "BlockDevice"
	LVMVolumeGroupKind        = "LvmVolumeGroup"
	APIGroup                  = "storage.deckhouse.io"
	APIVersion                = "v1alpha1" // v1alpha1
	OwnerReferencesAPIVersion = "v1"
	TypeMediaAPIVersion       = APIGroup + "/" + APIVersion
	Node                      = "Node"
)

// SchemeGroupVersion is group version used to register these objects
var (
	SchemeGroupVersion = schema.GroupVersion{
		Group:   APIGroup,
		Version: APIVersion,
	}
	// SchemeBuilder tbd
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// AddToScheme tbd
	AddToScheme = SchemeBuilder.AddToScheme
)

// Adds the list of known types to Scheme.
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&BlockDevice{},
		&BlockDeviceList{},
		&LvmVolumeGroup{},
		&LvmVolumeGroupList{},
	)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
