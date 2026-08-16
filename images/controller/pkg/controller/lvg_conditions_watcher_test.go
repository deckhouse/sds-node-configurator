/*
Copyright 2025 Flant JSC

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

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/controller/internal"
)

func TestMissingConditionTypes(t *testing.T) {
	conditionsOf := func(types ...string) []metav1.Condition {
		conds := make([]metav1.Condition, 0, len(types))
		for _, conType := range types {
			conds = append(conds, metav1.Condition{Type: conType, Status: metav1.ConditionTrue})
		}
		return conds
	}

	t.Run("every expected type present", func(t *testing.T) {
		lvg := &v1alpha1.LVMVolumeGroup{}
		lvg.Status.Conditions = conditionsOf(internal.LVGConditionTypes...)

		assert.Empty(t, missingConditionTypes(lvg))
	})

	t.Run("reports the absent types in declaration order", func(t *testing.T) {
		lvg := &v1alpha1.LVMVolumeGroup{}
		lvg.Status.Conditions = conditionsOf(internal.TypeVGReady, internal.TypeAgentReady)

		assert.Equal(t,
			[]string{internal.TypeVGConfigurationApplied, internal.TypeNodeReady, internal.TypeReady},
			missingConditionTypes(lvg))
	})

	// The check this replaces compared a count, so an unexpected type could stand
	// in for a missing one and let a half-reported LVMVolumeGroup through.
	t.Run("an unexpected type does not stand in for a missing one", func(t *testing.T) {
		present := append(conditionsOf(internal.LVGConditionTypes[1:]...),
			metav1.Condition{Type: "SomethingElse", Status: metav1.ConditionTrue})
		lvg := &v1alpha1.LVMVolumeGroup{}
		lvg.Status.Conditions = present

		assert.Equal(t, []string{internal.TypeVGConfigurationApplied}, missingConditionTypes(lvg))
	})

	t.Run("no conditions at all", func(t *testing.T) {
		assert.Equal(t, internal.LVGConditionTypes, missingConditionTypes(&v1alpha1.LVMVolumeGroup{}))
	})
}

// Every declared condition type has to have a component that writes it. Here
// that is not merely about observability: reconcileLVGConditions refuses to
// compute Ready until all of them have been written at least once, so a type
// nobody writes wedges every LVMVolumeGroup in the cluster at Pending.
//
// The agent writes its two from a different Go module, so the ownership split is
// what makes the set checkable — each side's own tests cover its writes.
func TestConditionOwnershipCoversEveryDeclaredType(t *testing.T) {
	owners := map[string][]string{
		"the agent":       internal.LVGConditionsOwnedByAgent,
		"the controllers": internal.LVGConditionsOwnedByController,
	}

	ownerOf := map[string]string{}
	for owner, types := range owners {
		for _, conType := range types {
			if previous, taken := ownerOf[conType]; taken {
				t.Errorf("condition %s is claimed by both %s and %s; two writers of one "+
					"condition overwrite each other", conType, previous, owner)
				continue
			}
			ownerOf[conType] = owner
		}
	}

	for _, conType := range internal.LVGConditionTypes {
		if _, owned := ownerOf[conType]; !owned {
			t.Errorf("condition %s is declared but no component writes it, which leaves every "+
				"LVMVolumeGroup Pending", conType)
		}
	}

	declared := map[string]bool{}
	for _, conType := range internal.LVGConditionTypes {
		declared[conType] = true
	}
	for conType, owner := range ownerOf {
		if !declared[conType] {
			t.Errorf("%s writes condition %s, which LVGConditionTypes does not declare", owner, conType)
		}
	}
}

// The two condition types the controllers report on the node and the agent pod
// are referenced through local names in sds_infra_watcher.go. They have to be
// the canonical constants, or the conditions watcher would be waiting for types
// nobody writes under those names.
func TestTheInfraWatcherUsesTheCanonicalConditionTypes(t *testing.T) {
	assert.Equal(t, internal.TypeNodeReady, nodeReadyType)
	assert.Equal(t, internal.TypeAgentReady, agentReadyType)
}

// internal.LVGConditionTypes used to be derived from this enum at runtime: the
// controller fetched its own CustomResourceDefinition on every reconcile and
// counted the entries. The list is now a compile-time constant, so the agreement
// with the published schema has to be asserted somewhere — an LVMVolumeGroup can
// only reach Ready once every type in the list has been written, and a type the
// enum rejects can never be written at all.
func TestLVGConditionTypesMatchTheCRDEnum(t *testing.T) {
	raw, err := os.ReadFile("../../../../crds/lvmvolumegroup.yaml")
	require.NoError(t, err)

	var crd struct {
		Spec struct {
			Versions []struct {
				Name   string `json:"name"`
				Schema struct {
					OpenAPIV3Schema struct {
						Properties struct {
							Status struct {
								Properties struct {
									Conditions struct {
										Items struct {
											Properties struct {
												Type struct {
													Enum []string `json:"enum"`
												} `json:"type"`
											} `json:"properties"`
										} `json:"items"`
									} `json:"conditions"`
								} `json:"properties"`
							} `json:"status"`
						} `json:"properties"`
					} `json:"openAPIV3Schema"`
				} `json:"schema"`
			} `json:"versions"`
		} `json:"spec"`
	}
	require.NoError(t, yaml.Unmarshal(raw, &crd))
	require.Len(t, crd.Spec.Versions, 1, "the lookup below assumes a single served version")

	enum := crd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties.Status.Properties.Conditions.Items.Properties.Type.Enum
	require.NotEmpty(t, enum, "the conditions type enum was not found in the CRD")

	assert.ElementsMatch(t, enum, internal.LVGConditionTypes,
		"internal.LVGConditionTypes and the type enum in crds/lvmvolumegroup.yaml have drifted apart")
}

// A False reason that is not in acceptableReasons drags the aggregate Ready
// condition down and puts the LVMVolumeGroup into NotReady, after which the
// scheduler stops placing new volumes on that Volume Group. The verdict looks at
// every condition's reason, whatever type wrote it.
//
// These reasons must never do that, because in every one of them the Volume Group
// is still serving every volume on it and what is left is either an operator
// decision or a retry:
//
//   - ValidationFailed — one spec.fileDevices entry is unusable, and the rest of
//     the Volume Group keeps reconciling around it.
//   - FileDeviceDrift — an entry backing a live Physical Volume was removed from
//     the spec. Nothing is wrong with the spec and nothing failed; the agent
//     refuses to shrink the Volume Group and says so.
//   - FileDeviceNotApplied — an entry could not be brought up on the node (no
//     free space, losetup refused, a grow that did not go through). Capacity has
//     not arrived; nothing that already exists has broken.
//   - AliasResolutionFailed — the agent cannot yet tell whether a loop device is
//     already a PV of the VG, so it postpones adding it. Alertable, not fatal.
//   - FileDeviceGrowFailed — raising an entry's size did not go through. Every
//     step of the growth sequence fails towards the smaller size, so the Volume
//     Group is still the size it was and still serving every volume on it.
//   - CacheStale — the agent's LVM cache has not caught up with a Volume Group
//     that is on the node, which is why the agent refuses to act on it.
//   - BlockDeviceNotFound — some of the Volume Group's Physical Volumes have no
//     BlockDevice resource, so status.nodes lists fewer devices than the node has.
//     It was written together with the rest of the status, so the capacity figures
//     the scheduler reads are the ones this pass measured.
//
// Introducing any of them without listing it here is what would turn a report
// into an outage, so the membership is asserted rather than left to review.
func TestAcceptableReasonsKeepAVolumeGroupInService(t *testing.T) {
	for _, reason := range []string{
		internal.ReasonUpdating,
		internal.ReasonValidationFailed,
		internal.ReasonFileDeviceDrift,
		internal.ReasonFileDeviceNotApplied,
		internal.ReasonAliasResolutionFailed,
		internal.ReasonFileDeviceGrowFailed,
		internal.ReasonCacheStale,
		internal.ReasonBlockDeviceNotFound,
	} {
		t.Run(reason, func(t *testing.T) {
			assert.Contains(t, acceptableReasons, reason,
				"VGConfigurationApplied=False with reason %q must not take the LVMVolumeGroup out of Ready", reason)
		})
	}

	t.Run("a genuine failure is still not acceptable", func(t *testing.T) {
		// The whitelist is meant to be narrow: a reason that means the storage
		// itself failed has to keep dragging the phase down. The distinction is
		// "the Volume Group is broken" versus "the Volume Group did not grow".
		assert.NotContains(t, acceptableReasons, "VGCreationFailed")
		assert.NotContains(t, acceptableReasons, "VGExtendFailed")
		assert.NotContains(t, acceptableReasons, "ThinPoolReconcileFailed")
		// The agent cannot read the node's Volume Groups at all. An
		// LVMVolumeGroup whose backing storage the agent has lost sight of is
		// exactly what the scheduler should stop placing volumes on — unlike
		// CacheStale, which is the agent knowing precisely what it is waiting for.
		assert.NotContains(t, acceptableReasons, "VGCheckFailed")
		// Not one Physical Volume of the Volume Group could be named by a
		// BlockDevice, so the agent left status.nodes as an earlier pass wrote it.
		// The Volume Group may well be fine, but the free space in that status is
		// nobody's current measurement, and placing volumes against stale figures is
		// what this must prevent — which is the whole distinction from
		// BlockDeviceNotFound, where the status was refreshed in the same pass.
		assert.NotContains(t, acceptableReasons, "NodeNotDescribed")
	})
}

// TestReasonsMirroredFromTheAgentKeepTheirWireValue pins the wire value of every
// reason this package matches but does not write.
//
// The agent produces them, from its own copy of the constant in another Go
// module; acceptableReasons compares by string. Nothing links the two at compile
// time, and every other test on this side goes through internal.Reason… — so a
// value changed on the agent side leaves this whole package green while the match
// silently stops happening.
//
// What that costs: BlockDeviceNotFound falling out of the match takes every
// LVMVolumeGroup with a filtered-out or undersized Physical Volume out of Ready
// and stops the scheduler placing volumes on it, cluster-wide, with nothing in any
// log to say why. The mirror of this test lives beside the agent's copy in
// images/agent/internal/const_test.go; both have to move together.
func TestReasonsMirroredFromTheAgentKeepTheirWireValue(t *testing.T) {
	assert.Equal(t, "BlockDeviceNotFound", internal.ReasonBlockDeviceNotFound,
		"the agent writes this string into the VGReady condition")
	assert.Equal(t, "CacheStale", internal.ReasonCacheStale,
		"the agent writes this string into the VGReady condition")
}
