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

const (
	Local  = "Local"
	Shared = "Shared"

	Failed = "Failed"

	NonOperational = "NonOperational"
	Operational    = "Operational"

	delAnnotation  = "storage.deckhouse.io/sds-delete-vg"
	nameSpaceEvent = "default"

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

	LVMVolumeGroupTag = "storage.deckhouse.io/lvmVolumeGroupName"
)
