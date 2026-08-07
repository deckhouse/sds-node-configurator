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
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/deckhouse/sds-node-configurator/images/controller/internal"
)

func TestLVGConditionsWatcher(t *testing.T) {
	cl := NewFakeClient()
	ctx := context.Background()

	t.Run("getCRD", func(t *testing.T) {
		targetName := "target"
		crds := []v1.CustomResourceDefinition{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name: targetName,
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name: "other-name",
				},
			},
		}

		for _, crd := range crds {
			err := cl.Create(ctx, &crd)
			if err != nil {
				t.Error(err)
			}
		}

		crd, err := getCRD(ctx, cl, targetName)
		if err != nil {
			t.Error(err)
		}

		assert.Equal(t, targetName, crd.Name)
	})

	t.Run("getTargetConditionsCount", func(t *testing.T) {
		first, err := json.Marshal("first")
		if err != nil {
			t.Error(err)
		}
		second, err := json.Marshal("second")
		if err != nil {
			t.Error(err)
		}
		third, err := json.Marshal("third")
		if err != nil {
			t.Error(err)
		}
		crd := &v1.CustomResourceDefinition{
			Spec: v1.CustomResourceDefinitionSpec{
				Versions: []v1.CustomResourceDefinitionVersion{
					{
						Schema: &v1.CustomResourceValidation{
							OpenAPIV3Schema: &v1.JSONSchemaProps{
								Properties: map[string]v1.JSONSchemaProps{
									"status": {
										Properties: map[string]v1.JSONSchemaProps{
											"conditions": {
												Items: &v1.JSONSchemaPropsOrArray{
													Schema: &v1.JSONSchemaProps{
														Properties: map[string]v1.JSONSchemaProps{
															"type": {
																Enum: []v1.JSON{
																	{
																		Raw: first,
																	},
																	{
																		Raw: second,
																	},
																	{
																		Raw: third,
																	},
																},
															},
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		}

		count, err := getTargetConditionsCount(crd)
		if err != nil {
			t.Error(err)
		}

		assert.Equal(t, 3, count)
	})
}

// A VGConfigurationApplied=False reason that is not in acceptableReasons drags the
// aggregate Ready condition down and puts the LVMVolumeGroup into NotReady, after
// which the scheduler stops placing new volumes on that Volume Group.
//
// Four reasons must never do that, because in every one of them the Volume Group
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
	})
}
