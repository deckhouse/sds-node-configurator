package v2alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	BDKind                    = "BlockDevice"
	GroupVersion              = "apiextensions.k8s.io/v1"
	Plural                    = "blockdevices"
	Singular                  = "blockdevice"
	CRDName                   = "blockdevices.storage.deckhouse.io"
	ShortName                 = "bd"
	APIGroup                  = "storage.deckhouse.io"
	APIVersion                = "v2alpha1"
	OwnerReferencesAPIVersion = "v1"
	OwnerReferencesKind       = "BlockDevice"
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

// BlockDeviceGVK is group version kind for BlockDevice.
var BlockDeviceGVK = schema.GroupVersionKind{
	Group:   SchemeGroupVersion.Group,
	Version: SchemeGroupVersion.Version,
	Kind:    BDKind,
}

// Kind takes an unqualified kind and returns back a Group qualified GroupKind
func Kind(kind string) schema.GroupKind {
	return SchemeGroupVersion.WithKind(kind).GroupKind()
}

// Resource takes an unqualified resource and returns a Group qualified GroupResource
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

// Adds the list of known types to Scheme.
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&BlockDevice{},
		&BlockDeviceList{},
	)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}