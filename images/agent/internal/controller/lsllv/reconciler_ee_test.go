//go:build !ce

/*
Copyright 2025 Flant JSC
Licensed under the Deckhouse Platform Enterprise Edition (EE) license. See https://github.com/deckhouse/deckhouse/blob/main/ee/LICENSE
*/

package lsllv

import (
	"context"
	"testing"

	"go.uber.org/mock/gomock"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/mock_utils"
)

func TestRemoveMarksBeforeErasing(t *testing.T) {
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	volume := testVolume(deleting)
	r, _ := testReconciler(t, commands, []internal.LVData{existingLV("")}, testGroup(testNode), volume)

	// The marker goes on before the first byte is written. An owner taking over
	// halfway through has to see a volume that may still hold data.
	gomock.InOrder(
		commands.EXPECT().SetLVTagShared(gomock.Any(), testVG, testLV, PendingCleanupTag, true).Return("lvchange --addtag", nil),
		commands.EXPECT().LVActivateShared(gomock.Any(), testVG, []string{testLV}, false).Return("lvchange -aey", nil),
	)

	// The erase itself needs a real device; the point checked here is the order
	// of the marker against the activation, so the run stops at the failure.
	_, _ = r.Reconcile(context.Background(),
		controller.ReconcileRequest[*v1alpha1.LVMSharedLogicalVolume]{Object: volume})
}

func TestMarkerIsNotSetTwice(t *testing.T) {
	ctrl := gomock.NewController(t)
	commands := mock_utils.NewMockCommands(ctrl)
	volume := testVolume(deleting)
	// The volume already carries the marker from an interrupted attempt, so the
	// only thing to do is erase it again.
	r, _ := testReconciler(t, commands, []internal.LVData{existingLV("other-tag," + PendingCleanupTag)},
		testGroup(testNode), volume)

	commands.EXPECT().LVActivateShared(gomock.Any(), testVG, []string{testLV}, false).Return("lvchange -aey", nil)

	_, _ = r.Reconcile(context.Background(),
		controller.ReconcileRequest[*v1alpha1.LVMSharedLogicalVolume]{Object: volume})
}
