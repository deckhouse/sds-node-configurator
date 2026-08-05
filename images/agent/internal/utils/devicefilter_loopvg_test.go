/*
Copyright 2026 Flant JSC

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

package utils_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/mock_utils"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

const (
	managedTag     = "storage.deckhouse.io/enabled=true"
	managedNameTag = "storage.deckhouse.io/lvmVolumeGroupName=e2e-lvg-fd"
)

func loopPV(name, vgUUID, vgName string) internal.PVData {
	return internal.PVData{PVName: name, VGUuid: vgUUID, VGName: vgName}
}

func pvNames(pvs []internal.PVData) []string {
	out := make([]string, 0, len(pvs))
	for _, pv := range pvs {
		out = append(out, pv.PVName)
	}
	return out
}

// classify runs the real classifier against a stubbed losetup. backingFiles maps
// a loop device to the backing file losetup reports for it; a device absent from
// the map is answered with an error, i.e. "ownership could not be established".
func classify(t *testing.T, vgs []internal.VGData, pvs []internal.PVData, backingFiles map[string]string) utils.LoopVGVerdicts {
	t.Helper()
	mc := mock_utils.NewMockCommands(gomock.NewController(t))
	mc.EXPECT().GetLoopBackingFile(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, loopDev string) (string, internal.LoopBackingFile, error) {
			backing, ok := backingFiles[loopDev]
			if !ok {
				return "losetup ...", internal.LoopBackingFile{}, errors.New("losetup: not a loop device")
			}
			return "losetup ...", internal.LoopBackingFile{Path: backing}, nil
		}).AnyTimes()

	return utils.ClassifyLoopVGs(context.Background(), logger.Logger{}, mc, 0, vgs, pvs)
}

// The regression this whole guard exists for. fillTheCache lists VGs before PVs,
// so a VG created between the two calls — typically one the agent has just built
// for a spec.fileDevices LVMVolumeGroup — is absent from vgs while its loop PV is
// already in pvs. Classifying that as foreign dropped the PV, FilterVGsByPresentPVs
// then dropped the VG, and with an empty cache FindVG returned nil: the reconciler
// re-entered the create path and pvcreate died with "Can't initialize physical
// volume /dev/loop0 of volume group ... without -ff" on every retry.
func TestFilterForeignLoopPVs_KeepsLoopPVWhoseVGIsNotListedYet(t *testing.T) {
	pvs := []internal.PVData{loopPV("/dev/loop0", "uuid-fresh", "e2e-vg-fd")}

	verdicts := classify(t, nil, pvs, nil)
	got := utils.FilterForeignLoopPVs(logger.Logger{}, pvs, verdicts)

	assert.Equal(t, []string{"/dev/loop0"}, pvNames(got),
		"a loop PV whose VG is merely unknown must be kept; absence is not evidence of foreignness")
}

// The filter must still do its job: a VG that IS listed and carries no managed tag
// is foreign, and a nested-LVM loop VG inside a guest disk must not reach the cache
// where it could collide by name with a managed VG.
func TestFilterForeignLoopPVs_DropsListedUntaggedLoopVG(t *testing.T) {
	vgs := []internal.VGData{{VGName: "guest-vg", VGUUID: "uuid-foreign", VGTags: ""}}
	pvs := []internal.PVData{loopPV("/dev/loop1", "uuid-foreign", "guest-vg")}

	verdicts := classify(t, vgs, pvs, map[string]string{"/dev/loop1": "/var/lib/libvirt/guest.qcow2"})
	got := utils.FilterForeignLoopPVs(logger.Logger{}, pvs, verdicts)

	assert.Empty(t, got, "a listed, untagged, purely loop-backed VG is foreign and must be dropped")
}

// A loop-only VG of ours: tagged, and backed by a file only this agent names that
// way.
func TestFilterForeignLoopPVs_KeepsManagedLoopOnlyVG(t *testing.T) {
	vgs := []internal.VGData{{
		VGName: "e2e-vg-fd", VGUUID: "uuid-managed",
		VGTags: managedTag + "," + managedNameTag,
	}}
	pvs := []internal.PVData{loopPV("/dev/loop0", "uuid-managed", "e2e-vg-fd")}

	verdicts := classify(t, vgs, pvs, map[string]string{
		"/dev/loop0": "/opt/deckhouse/sds/file-devices/sds-e2e-lvg-fd.data-0.img",
	})
	got := utils.FilterForeignLoopPVs(logger.Logger{}, pvs, verdicts)

	assert.Equal(t, []string{"/dev/loop0"}, pvNames(got))
	assert.Equal(t, utils.LoopVGManaged, verdicts["uuid-managed"])
}

// The case the tag-only rule got wrong, and the reason ownership is the backing
// file rather than the tag. An image of a node disk this module used to manage
// carries storage.deckhouse.io/enabled=true, so `losetup -f /backup/node2.img`
// during a restore handed the cache a second VG named `data` — and
// findDuplicateVGNames then took the live, healthy LVMVolumeGroup offline. That is
// the very outage the filter exists to prevent, arriving through the exception the
// filter used to make.
func TestFilterForeignLoopPVs_DropsTaggedLoopVGWithForeignBackingFile(t *testing.T) {
	vgs := []internal.VGData{{
		VGName: "data", VGUUID: "uuid-restored-image",
		VGTags: managedTag + ",storage.deckhouse.io/lvmVolumeGroupName=vg-on-another-node",
	}}
	pvs := []internal.PVData{loopPV("/dev/loop7", "uuid-restored-image", "data")}

	verdicts := classify(t, vgs, pvs, map[string]string{"/dev/loop7": "/backup/node2-root.img"})
	got := utils.FilterForeignLoopPVs(logger.Logger{}, pvs, verdicts)

	assert.Empty(t, got, "a tagged loop VG whose backing file this agent did not create is not ours")
	assert.Equal(t, utils.LoopVGUnowned, verdicts["uuid-restored-image"])
}

// Ownership must not hinge on losetup answering. An unreadable backing file is a
// transient host problem, and refusing on it would drop the node's own storage out
// of the cache — the failure this area has already been bitten by.
func TestFilterForeignLoopPVs_KeepsTaggedLoopVGWhenOwnershipIsUnknown(t *testing.T) {
	vgs := []internal.VGData{{VGName: "e2e-vg-fd", VGUUID: "uuid-unknown", VGTags: managedTag}}
	pvs := []internal.PVData{loopPV("/dev/loop0", "uuid-unknown", "e2e-vg-fd")}

	verdicts := classify(t, vgs, pvs, nil) // losetup fails for every device
	got := utils.FilterForeignLoopPVs(logger.Logger{}, pvs, verdicts)

	assert.Equal(t, []string{"/dev/loop0"}, pvNames(got))
	assert.Equal(t, utils.LoopVGUnknown, verdicts["uuid-unknown"])
}

// An unlinked backing file still names its owner. Refusing to recognise our own
// Volume Group because somebody ran `rm` on its file is how it gets dropped from
// the cache and then re-created underneath itself.
func TestFilterForeignLoopPVs_KeepsManagedLoopVGWithDeletedBackingFile(t *testing.T) {
	vgs := []internal.VGData{{VGName: "e2e-vg-fd", VGUUID: "uuid-deleted", VGTags: managedTag}}
	pvs := []internal.PVData{loopPV("/dev/loop0", "uuid-deleted", "e2e-vg-fd")}

	mc := mock_utils.NewMockCommands(gomock.NewController(t))
	mc.EXPECT().GetLoopBackingFile(gomock.Any(), "/dev/loop0").
		Return("losetup ...", internal.LoopBackingFile{
			Path:    "/opt/deckhouse/sds/file-devices/sds-e2e-lvg-fd.data-0.img",
			Deleted: true,
		}, nil)

	verdicts := utils.ClassifyLoopVGs(context.Background(), logger.Logger{}, mc, 0, vgs, pvs)

	assert.Equal(t, utils.LoopVGManaged, verdicts["uuid-deleted"])
	assert.Equal(t, []string{"/dev/loop0"}, pvNames(utils.FilterForeignLoopPVs(logger.Logger{}, pvs, verdicts)))
}

// The mixed case the e2e covers: one loop PV and one block PV in the same VG. It is
// not purely loop-backed, so the filter must not consider it even without a tag —
// and no losetup is spent on it either.
func TestFilterForeignLoopPVs_KeepsVGThatAlsoHasABlockPV(t *testing.T) {
	vgs := []internal.VGData{{VGName: "mixed", VGUUID: "uuid-mixed", VGTags: ""}}
	pvs := []internal.PVData{
		loopPV("/dev/loop0", "uuid-mixed", "mixed"),
		{PVName: "/dev/sdb", VGUuid: "uuid-mixed", VGName: "mixed"},
	}

	mc := mock_utils.NewMockCommands(gomock.NewController(t))
	// No GetLoopBackingFile expectation: a VG with a block PV is settled by name
	// alone, and spending a host command on it would be paid on every cache fill.
	verdicts := utils.ClassifyLoopVGs(context.Background(), logger.Logger{}, mc, 0, vgs, pvs)
	got := utils.FilterForeignLoopPVs(logger.Logger{}, pvs, verdicts)

	assert.ElementsMatch(t, []string{"/dev/loop0", "/dev/sdb"}, pvNames(got))
	assert.Equal(t, utils.LoopVGNotLoopOnly, verdicts["uuid-mixed"])
}

// A foreign VG next to a freshly created one: the foreign PV goes, ours stays.
func TestFilterForeignLoopPVs_SeparatesFreshFromForeign(t *testing.T) {
	vgs := []internal.VGData{{VGName: "guest-vg", VGUUID: "uuid-foreign", VGTags: ""}}
	pvs := []internal.PVData{
		loopPV("/dev/loop0", "uuid-fresh", "e2e-vg-fd"), // not listed yet
		loopPV("/dev/loop1", "uuid-foreign", "guest-vg"),
	}

	verdicts := classify(t, vgs, pvs, map[string]string{"/dev/loop1": "/var/lib/libvirt/guest.qcow2"})
	got := utils.FilterForeignLoopPVs(logger.Logger{}, pvs, verdicts)

	assert.Equal(t, []string{"/dev/loop0"}, pvNames(got))
}

// The outage in full: two Volume Groups named `data` on one node, one live and
// ours, one an image somebody attached. Both carry the managed tag, so only the
// backing file tells them apart. The verdicts are keyed by UUID rather than by
// name precisely so this case does not collapse into one answer.
func TestFilterForeignLoopPVs_DropsTheForeignHalfOfADuplicateName(t *testing.T) {
	vgs := []internal.VGData{
		{VGName: "data", VGUUID: "uuid-image", VGTags: managedTag},
		{VGName: "data", VGUUID: "uuid-live", VGTags: managedTag + "," + managedNameTag},
	}
	pvs := []internal.PVData{
		loopPV("/dev/loop7", "uuid-image", "data"),
		loopPV("/dev/loop8", "uuid-live", "data"),
	}

	verdicts := classify(t, vgs, pvs, map[string]string{
		"/dev/loop7": "/backup/node2-root.img",
		"/dev/loop8": "/opt/deckhouse/sds/file-devices/sds-e2e-lvg-fd.data-0.img",
	})
	got := utils.FilterForeignLoopPVs(logger.Logger{}, pvs, verdicts)

	assert.Equal(t, []string{"/dev/loop8"}, pvNames(got),
		"only the live Volume Group may reach the cache; the other one is what makes findDuplicateVGNames take it offline")
}

// An untagged block-backed VG stays visible: it is a legitimate, potentially
// adoptable local Volume Group, and no losetup is spent deciding that.
func TestFilterForeignLoopPVs_KeepsUntaggedBlockVG(t *testing.T) {
	vgs := []internal.VGData{{VGName: "vg-block", VGUUID: "uuid-block", VGTags: ""}}
	pvs := []internal.PVData{{PVName: "/dev/sdb", VGUuid: "uuid-block", VGName: "vg-block"}}

	verdicts := classify(t, vgs, pvs, nil)

	assert.Equal(t, []string{"/dev/sdb"}, pvNames(utils.FilterForeignLoopPVs(logger.Logger{}, pvs, verdicts)))
}

// A bare loop PV that belongs to no Volume Group carries no VG name and cannot
// poison name resolution, so there is nothing to drop.
func TestFilterForeignLoopPVs_KeepsBareLoopPV(t *testing.T) {
	pvs := []internal.PVData{{PVName: "/dev/loop1"}}

	verdicts := classify(t, nil, pvs, nil)

	assert.Equal(t, []string{"/dev/loop1"}, pvNames(utils.FilterForeignLoopPVs(logger.Logger{}, pvs, verdicts)))
}

// Every code path that writes to, activates or adopts a Volume Group runs its
// input through this, and only a confident utils.LoopVGUnowned is refused.
func TestSkipUnownedLoopVGs(t *testing.T) {
	vgs := []internal.VGData{
		{VGName: "ours", VGUUID: "uuid-managed"},
		{VGName: "guest", VGUUID: "uuid-foreign"},
		{VGName: "cannot-tell", VGUUID: "uuid-unknown"},
		{VGName: "block-backed", VGUUID: "uuid-block"},
		{VGName: "never-classified", VGUUID: "uuid-absent"},
	}
	verdicts := utils.LoopVGVerdicts{
		"uuid-managed": utils.LoopVGManaged,
		"uuid-foreign": utils.LoopVGUnowned,
		"uuid-unknown": utils.LoopVGUnknown,
		"uuid-block":   utils.LoopVGNotLoopOnly,
	}

	got := utils.SkipUnownedLoopVGs(logger.Logger{}, "activate", vgs, verdicts)

	names := make([]string, 0, len(got))
	for _, vg := range got {
		names = append(names, vg.VGName)
	}
	assert.Equal(t, []string{"ours", "cannot-tell", "block-backed", "never-classified"}, names)
}

func TestPVsReferenceUnknownVG(t *testing.T) {
	vgs := []internal.VGData{{VGName: "known", VGUUID: "uuid-known"}}

	t.Run("fresh_vg_is_reported", func(t *testing.T) {
		assert.True(t, utils.PVsReferenceUnknownVG(vgs,
			[]internal.PVData{loopPV("/dev/loop0", "uuid-fresh", "e2e-vg-fd")}))
	})

	t.Run("all_vgs_known", func(t *testing.T) {
		assert.False(t, utils.PVsReferenceUnknownVG(vgs,
			[]internal.PVData{loopPV("/dev/loop0", "uuid-known", "known")}))
	})

	t.Run("pv_without_a_vg_is_not_a_trigger", func(t *testing.T) {
		// A bare PV that is not in any VG is normal and must not cause a re-scan
		// on every single cache fill.
		assert.False(t, utils.PVsReferenceUnknownVG(vgs,
			[]internal.PVData{{PVName: "/dev/loop0"}}))
	})
}

// Whether a Volume Group carries the managed tag decides, among other things,
// whether utils.ClassifyLoopVGs even asks about its backing files, so the tag has to be
// matched as a whole element of the comma-separated list. LVM's tag charset admits
// a tag that merely contains the managed one, and a substring match would hand a
// stranger's loop-backed VG the same standing as our own.
func TestHasManagedTag(t *testing.T) {
	for _, tc := range []struct {
		tags string
		want bool
	}{
		{managedTag, true},
		{managedTag + ",storage.deckhouse.io/lvmVolumeGroupName=vg-a", true},
		{"linstor-vg," + managedTag, true},
		{"", false},
		{"storage.deckhouse.io/lvmVolumeGroupName=vg-a", false},
		{"x-" + managedTag, false},
		{managedTag + "-shadow", false},
		{"storage.deckhouse.io/enabled=false", false},
	} {
		assert.Equal(t, tc.want, utils.HasManagedTag(tc.tags), "tags %q", tc.tags)
	}
}

// The tag list is not the agent's own writing. `vgchange --addtag` accepts
// anything in LVM's tag charset, so a Volume Group an administrator hands over
// can carry a bare `<key>` with no value at all — and splitting on "=" to take
// element [1] panicked on exactly that, taking the whole DaemonSet into
// CrashLoopBackOff.
//
// spec.fileDevices is what makes this reachable in practice: dropping `loop` from
// LVMGlobalFilter made Volume Groups the agent never tagged visible to lvm.static
// in the first place.
func TestReadValueFromTags(t *testing.T) {
	const key = "storage.deckhouse.io/lvmVolumeGroupName"

	for _, tc := range []struct {
		name      string
		tags      string
		wantOwned bool
		wantValue string
	}{
		{
			name:      "the ordinary key=value the agent writes",
			tags:      managedTag + "," + key + "=vg-a",
			wantOwned: true,
			wantValue: "vg-a",
		},
		{
			name:      "a valueless tag is skipped rather than panicked on",
			tags:      managedTag + "," + key,
			wantOwned: true,
			wantValue: "",
		},
		{
			name:      "a valueless tag does not hide a well-formed one after it",
			tags:      managedTag + "," + key + "," + key + "=vg-b",
			wantOwned: true,
			wantValue: "vg-b",
		},
		{
			name:      "an empty value is a value",
			tags:      managedTag + "," + key + "=",
			wantOwned: true,
			wantValue: "",
		},
		{
			// LVM's tag charset allows '=', so only the first one separates.
			name:      "the value keeps every '=' after the first",
			tags:      managedTag + "," + key + "=vg=a",
			wantOwned: true,
			wantValue: "vg=a",
		},
		{
			// A whole-name match, not a prefix one: a different tag that merely
			// starts with the same text is a different tag.
			name:      "a longer key is not the key being asked for",
			tags:      managedTag + "," + key + "Suffix=vg-a",
			wantOwned: true,
			wantValue: "",
		},
		{
			name:      "an untagged Volume Group is not ours and has no value to read",
			tags:      key + "=vg-a",
			wantOwned: false,
			wantValue: "",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owned, value := utils.ReadValueFromTags(tc.tags, key)
			assert.Equal(t, tc.wantOwned, owned)
			assert.Equal(t, tc.wantValue, value)
		})
	}
}
