package controller

const (
	Local  = "Local"
	Shared = "Shared"

	Failed = "Failed"

	NoOperational = "Nonoperational"
	Operational   = "Operational"

	delAnnotation      = "storage.deckhouse.io/sds-delete-vg"
	nameSpaceEvent     = "default"
	LvmVolumeGroupKind = "LvmVolumeGroup"

	EventActionDeleting = "Deleting"
	EventReasonDeleting = "Deleting"

	EventActionProvisioning = "Provisioning"
	EventReasonProvisioning = "Provisioning"

	EventActionCreating = "Creating"
	EventReasonCreating = "Creating"

	EventActionExtending = "Extending"
	EventReasonExtending = "Extending"

	EventActionShrinking = "Shrinking"
	EventReasonShrinking = "Shrinking"

	EventActionResizing = "Resizing"
	EventReasonResizing = "Resizing"

	EventActionReady = "Ready"
	EventReasonReady = "Ready"
)
