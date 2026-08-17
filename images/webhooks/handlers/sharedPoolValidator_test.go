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

package handlers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cn "github.com/deckhouse/sds-node-configurator/api/v1alpha1"
)

func testClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, cn.AddToScheme(scheme))
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
}

// testValidator runs the cross-object rules with the edition gate open, because
// those rules are the same in every edition — the gate is what differs, and it
// has a test of its own.
func testValidator(cl client.Client) *SharedPoolValidator {
	return &SharedPoolValidator{cl: cl, cleanupAvailable: func() bool { return true }}
}

func group(cleanup string, nodes ...string) *cn.LVMSharedVolumeGroup {
	return &cn.LVMSharedVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "pool-1"},
		Spec: cn.LVMSharedVolumeGroupSpec{
			ActualVGNameOnTheNode: "vgshared",
			Nodes:                 nodes,
			VolumeCleanup:         cleanup,
		},
	}
}

func volume(cleanup string) *cn.LVMSharedLogicalVolume {
	return &cn.LVMSharedLogicalVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "vol-1"},
		Spec: cn.LVMSharedLogicalVolumeSpec{
			LVMSharedVolumeGroupName: "pool-1",
			ActualLVNameOnTheNode:    "vol1",
			Size:                     "10Gi",
			VolumeCleanup:            cleanup,
		},
	}
}

func attachment(node string) *cn.LVMSharedLogicalVolumeAttachment {
	return &cn.LVMSharedLogicalVolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "att-1"},
		Spec: cn.LVMSharedLogicalVolumeAttachmentSpec{
			LVMSharedLogicalVolumeName: "vol-1",
			NodeName:                   node,
			AccessMode:                 cn.LVMSharedLogicalVolumeAccessModeRWO,
		},
	}
}

func TestWeakerCleanupThanThePoolIsRefused(t *testing.T) {
	v := testValidator(testClient(t, group(cn.VolumeCleanupRandomFillThreePass, "node-1")))

	result, err := v.ValidateVolume(context.Background(), nil, volume(cn.VolumeCleanupDiscard))
	require.NoError(t, err)

	assert.False(t, result.Valid)
	assert.Contains(t, result.Message, "weakens the pool rather than only this volume")
}

func TestStrongerCleanupThanThePoolIsAllowed(t *testing.T) {
	v := testValidator(testClient(t, group(cn.VolumeCleanupDiscard, "node-1")))

	result, err := v.ValidateVolume(context.Background(), nil, volume(cn.VolumeCleanupRandomFillThreePass))
	require.NoError(t, err)
	assert.True(t, result.Valid)
}

func TestEqualCleanupIsAllowed(t *testing.T) {
	v := testValidator(testClient(t, group(cn.VolumeCleanupDiscard, "node-1")))

	result, err := v.ValidateVolume(context.Background(), nil, volume(cn.VolumeCleanupDiscard))
	require.NoError(t, err)
	assert.True(t, result.Valid)
}

func TestEmptyCleanupIsRefusedAgainstAPoolThatDemandsOne(t *testing.T) {
	// The pool's policy is a floor, not a default: a volume that asks for
	// nothing must not silently get nothing.
	v := testValidator(testClient(t, group(cn.VolumeCleanupDiscard, "node-1")))

	result, err := v.ValidateVolume(context.Background(), nil, volume(""))
	require.NoError(t, err)
	assert.False(t, result.Valid)
}

func TestVolumeWithoutAPoolIsRefused(t *testing.T) {
	v := testValidator(testClient(t))

	result, err := v.ValidateVolume(context.Background(), nil, volume(cn.VolumeCleanupDiscard))
	require.NoError(t, err)
	assert.False(t, result.Valid)
	assert.Contains(t, result.Message, "nothing to carve this volume out of")
}

func TestAttachmentToANonMemberIsRefused(t *testing.T) {
	// It would sit in Pending forever while looking like a scheduling problem:
	// only a member of the pool holds the lockspace that hands locks out.
	v := testValidator(testClient(t,
		group(cn.VolumeCleanupDiscard, "node-1"), volume(cn.VolumeCleanupDiscard)))

	result, err := v.ValidateAttachment(context.Background(), nil, attachment("node-9"))
	require.NoError(t, err)
	assert.False(t, result.Valid)
	assert.Contains(t, result.Message, "not a member of the pool")
}

func TestAnAttachmentBeingDeletedIsNotJudged(t *testing.T) {
	// Measured on the stand: a node was fenced and its Node object deleted, so
	// the pool dropped it from its members — and the cleanup that had to release
	// the attachment it left behind was denied by this validator every time it
	// tried. The one update such an object still needs is the removal of a
	// finalizer, and it asks for nothing that could be refused.
	v := testValidator(testClient(t,
		group(cn.VolumeCleanupDiscard, "node-1"), volume(cn.VolumeCleanupDiscard)))

	leaving := attachment("node-9")
	now := metav1.Now()
	leaving.DeletionTimestamp = &now
	leaving.Finalizers = []string{"storage.deckhouse.io/sds-node-configurator"}

	result, err := v.ValidateAttachment(context.Background(), nil, leaving)
	require.NoError(t, err)
	assert.True(t, result.Valid, "an attachment on its way out asks for nothing")
}

func TestAttachmentToAMemberIsAllowed(t *testing.T) {
	v := testValidator(testClient(t,
		group(cn.VolumeCleanupDiscard, "node-1", "node-2"), volume(cn.VolumeCleanupDiscard)))

	result, err := v.ValidateAttachment(context.Background(), nil, attachment("node-2"))
	require.NoError(t, err)
	assert.True(t, result.Valid)
}

func TestAttachmentWithoutAVolumeIsRefused(t *testing.T) {
	v := testValidator(testClient(t, group(cn.VolumeCleanupDiscard, "node-1")))

	result, err := v.ValidateAttachment(context.Background(), nil, attachment("node-1"))
	require.NoError(t, err)
	assert.False(t, result.Valid)
}

func TestCleanupStrengthCoversEveryPolicy(t *testing.T) {
	// A policy missing from the table would compare as the weakest one, which
	// is exactly the direction a mistake here must not go.
	for _, policy := range []string{
		cn.VolumeCleanupDiscard,
		cn.VolumeCleanupRandomFillSinglePass,
		cn.VolumeCleanupRandomFillThreePass,
	} {
		strength, known := cleanupStrength[policy]
		assert.True(t, known, policy)
		assert.Positive(t, strength, policy)
	}
}

func TestEditionWithoutCleanupRefusesEverything(t *testing.T) {
	// SE has snapshots but not volume cleanup, so it is inside the set this
	// refuses — "EE-gated" is a wider claim than the truth.
	v := &SharedPoolValidator{
		cl:               testClient(t, group(cn.VolumeCleanupDiscard, "node-1"), volume(cn.VolumeCleanupDiscard)),
		cleanupAvailable: func() bool { return false },
	}

	result, err := v.ValidateVolume(context.Background(), nil, volume(cn.VolumeCleanupDiscard))
	require.NoError(t, err)
	assert.False(t, result.Valid)
	assert.Contains(t, result.Message, "not available in this Deckhouse edition")

	result, err = v.ValidateAttachment(context.Background(), nil, attachment("node-1"))
	require.NoError(t, err)
	assert.False(t, result.Valid)
}
