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

package lvg

import (
	"bytes"
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/cache"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

// A tagged Volume Group whose LVMVolumeGroup resource is gone gets re-imported
// by the discoverer. For a file-backed VG that only works if spec.fileDevices
// can be rebuilt from the node: otherwise the resource comes back as an empty
// shell, the agent sees no entries, and it re-provisions duplicate files
// alongside the live ones.
func TestBuildSpecFileDevicesFromCandidate(t *testing.T) {
	const dir = "/opt/deckhouse/sds/file-devices"

	// The base directory is left unset so no path translation is attempted: these
	// cases are about rebuilding the entries themselves. The symlink translation
	// has its own test below.
	newDiscoverer := func() *Discoverer {
		return &Discoverer{log: logger.Logger{}, sdsCache: cache.New()}
	}

	t.Run("rebuilds_entries_from_backing_files", func(t *testing.T) {
		candidate := internal.LVMVolumeGroupCandidate{
			LVMVGName: "vg-a",
			FileDeviceNodes: map[string][]internal.LVMVGFileDevice{
				"node-1": {
					// PV sizes, i.e. slightly below what was requested: LVM
					// reserves metadata at the head of the device.
					{FilePath: dir + "/sds-vg-a.d2.img", Size: resource.MustParse("2143289344")},
					{FilePath: dir + "/sds-vg-a.d1.img", Size: resource.MustParse("1069547520")},
				},
			},
		}

		got, err := newDiscoverer().buildSpecFileDevicesFromCandidate(context.Background(), candidate)
		assert.NoError(t, err)

		// Compared field by field: resource.Quantity also carries a cached
		// string form that differs between a parsed and a computed value while
		// the quantities themselves are equal.
		if assert.Len(t, got, 2, "entries must be rebuilt and ordered deterministically") {
			assert.Equal(t, "d1", got[0].Name)
			assert.Equal(t, dir, got[0].Directory)
			assert.Equal(t, int64(1)<<30, got[0].Size.Value(), "the PV size must round back up to the requested 1Gi")

			assert.Equal(t, "d2", got[1].Name)
			assert.Equal(t, dir, got[1].Directory)
			assert.Equal(t, int64(2)<<30, got[1].Size.Value(), "the PV size must round back up to the requested 2Gi")
		}
	})

	t.Run("ignores_unmanaged_files", func(t *testing.T) {
		candidate := internal.LVMVolumeGroupCandidate{
			LVMVGName: "vg-a",
			FileDeviceNodes: map[string][]internal.LVMVGFileDevice{
				"node-1": {
					{FilePath: "/var/lib/libvirt/images/disk0.qcow2", Size: resource.MustParse("10Gi")},
					{FilePath: dir + "/sds-vg-a.d1.img", Size: resource.MustParse("1069547520")},
				},
			},
		}

		got, err := newDiscoverer().buildSpecFileDevicesFromCandidate(context.Background(), candidate)
		assert.NoError(t, err)

		assert.Len(t, got, 1)
		assert.Equal(t, "d1", got[0].Name)
	})

	t.Run("no_file_devices_yields_nil", func(t *testing.T) {
		got, err := newDiscoverer().buildSpecFileDevicesFromCandidate(context.Background(),
			internal.LVMVolumeGroupCandidate{LVMVGName: "vg-a"})
		assert.NoError(t, err)
		assert.Nil(t, got)
	})
}

// status.nodes[].fileDevices[].filePath comes from `losetup --output BACK-FILE`,
// which resolves every symlink component of the directory. spec.fileDevices[]
// .directory, on the other hand, is checked against the configured base directory
// lexically. Point the default base at a data disk with a symlink — the obvious
// move, and one the docs suggest in prose — and an import that recorded the
// resolved path would produce an entry its own agent rejects on every reconcile,
// with `directory` immutable and therefore no way back through the API.
func TestBuildSpecFileDevicesFromCandidate_MapsResolvedPathsBackOntoTheConfiguredBase(t *testing.T) {
	const (
		configuredBase = "/opt/deckhouse/sds/file-devices"
		resolvedBase   = "/mnt/data/fd"
	)

	newDiscoverer := func(resolver utils.CanonicalPathResolver) *Discoverer {
		return &Discoverer{
			log:      logger.Logger{},
			sdsCache: cache.New(),
			cfg:      DiscovererConfig{NodeName: "test_node", FileDevicesDirectory: configuredBase},
			resolver: resolver,
		}
	}

	candidateAt := func(dir string) internal.LVMVolumeGroupCandidate {
		return internal.LVMVolumeGroupCandidate{
			LVMVGName:             "vg-a",
			ActualVGNameOnTheNode: "data-vg",
			FileDeviceNodes: map[string][]internal.LVMVGFileDevice{
				"test_node": {{FilePath: dir + "/sds-vg-a.d1.img", Size: resource.MustParse("1069547520")}},
			},
		}
	}

	t.Run("a path already under the base is left alone and costs no resolver call", func(t *testing.T) {
		d := newDiscoverer(func(context.Context, string) (string, error) {
			t.Fatal("the resolver must not be called when the path is already under the configured base")
			return "", nil
		})

		got, err := d.buildSpecFileDevicesFromCandidate(context.Background(), candidateAt(configuredBase+"/sub"))
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, configuredBase+"/sub", got[0].Directory)
	})

	t.Run("a resolved path is rewritten under the configured base", func(t *testing.T) {
		calls := 0
		d := newDiscoverer(func(_ context.Context, path string) (string, error) {
			calls++
			assert.Equal(t, configuredBase, path, "only the base directory is ever resolved")
			return resolvedBase, nil
		})

		got, err := d.buildSpecFileDevicesFromCandidate(context.Background(), candidateAt(resolvedBase))
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, configuredBase, got[0].Directory)
		assert.Equal(t, 1, calls)
	})

	t.Run("a subdirectory below the resolved base keeps its suffix", func(t *testing.T) {
		d := newDiscoverer(func(context.Context, string) (string, error) { return resolvedBase, nil })

		got, err := d.buildSpecFileDevicesFromCandidate(context.Background(), candidateAt(resolvedBase+"/tier1"))
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, configuredBase+"/tier1", got[0].Directory)
	})

	// Writing a directory that resolves somewhere else would have the reconciler
	// provision a second backing file next to the one already in the Volume Group.
	// Refusing the import keeps the VG untouched and says why.
	t.Run("a file genuinely outside the base refuses the import", func(t *testing.T) {
		d := newDiscoverer(func(context.Context, string) (string, error) { return resolvedBase, nil })

		_, err := d.buildSpecFileDevicesFromCandidate(context.Background(), candidateAt("/srv/elsewhere"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "outside the configured base directory")
	})

	t.Run("a resolver failure refuses the import rather than guessing", func(t *testing.T) {
		d := newDiscoverer(func(context.Context, string) (string, error) {
			return "", errors.New("readlink: No such file or directory")
		})

		_, err := d.buildSpecFileDevicesFromCandidate(context.Background(), candidateAt(resolvedBase))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unable to resolve the base directory")
	})
}

func TestRelocateUnderBase(t *testing.T) {
	tests := map[string]struct {
		dir, resolvedBase, configuredBase string
		want                              string
		wantOK                            bool
	}{
		"the base itself":            {"/mnt/data/fd", "/mnt/data/fd", "/opt/sds/fd", "/opt/sds/fd", true},
		"a subdirectory":             {"/mnt/data/fd/a/b", "/mnt/data/fd", "/opt/sds/fd", "/opt/sds/fd/a/b", true},
		"trailing separators":        {"/mnt/data/fd/a/", "/mnt/data/fd/", "/opt/sds/fd/", "/opt/sds/fd/a", true},
		"identical bases":            {"/opt/sds/fd/a", "/opt/sds/fd", "/opt/sds/fd", "/opt/sds/fd/a", true},
		"outside the resolved base":  {"/srv/other", "/mnt/data/fd", "/opt/sds/fd", "", false},
		"a sibling sharing a prefix": {"/mnt/data/fdx", "/mnt/data/fd", "/opt/sds/fd", "", false},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, ok := relocateUnderBase(tt.dir, tt.resolvedBase, tt.configuredBase)
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.want, got)
		})
	}
}

// The discovery loop short-circuits on a node that has nothing to discover. The
// LVMVolumeGroup resources are not enough to make that call: importing a Volume
// Group whose resource is gone is precisely the case where no resource exists,
// and a node whose only storage is a file-backed VG has no BlockDevices either.
// Without consulting the LVM scan, such a VG could never be re-adopted.
func TestCacheHasManagedVG(t *testing.T) {
	tests := map[string]struct {
		vgs  []internal.VGData
		want bool
	}{
		"managed VG present": {
			vgs:  []internal.VGData{{VGName: "vg-a", VGTags: "storage.deckhouse.io/enabled=true"}},
			want: true,
		},
		"managed among others": {
			vgs: []internal.VGData{
				{VGName: "foreign", VGTags: ""},
				{VGName: "vg-a", VGTags: "storage.deckhouse.io/enabled=true,storage.deckhouse.io/lvmVolumeGroupName=lvg-a"},
			},
			want: true,
		},
		"only untagged VGs": {
			vgs:  []internal.VGData{{VGName: "foreign", VGTags: "someone/else=1"}},
			want: false,
		},
		"nothing on the node": {vgs: nil, want: false},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			d := &Discoverer{sdsCache: cache.New()}
			d.sdsCache.StoreVGs(tc.vgs, bytes.Buffer{})
			assert.Equal(t, tc.want, d.cacheHasManagedVG())
		})
	}
}

// An imported Volume Group must come back under the name recorded in its tag.
// Generating a fresh one breaks file-backed groups outright: the backing files
// are named after the owning LVMVolumeGroup, so under a new name the agent stops
// recognising its own devices and the imported resource cannot describe the node.
func TestLVGNameForCandidate(t *testing.T) {
	t.Run("uses the name recorded in the tag", func(t *testing.T) {
		vg := internal.VGData{
			VGName: "vg-a",
			VGTags: "storage.deckhouse.io/enabled=true,storage.deckhouse.io/lvmVolumeGroupName=lvg-original",
		}
		name, fromTag, ok := lvgNameForCandidate(vg)
		assert.Equal(t, "lvg-original", name)
		assert.True(t, fromTag)
		assert.True(t, ok)
	})

	t.Run("generates a name when only the managed tag is present", func(t *testing.T) {
		// A Volume Group an administrator created by hand and handed over.
		vg := internal.VGData{VGName: "vg-a", VGTags: "storage.deckhouse.io/enabled=true"}
		got, fromTag, ok := lvgNameForCandidate(vg)
		assert.True(t, strings.HasPrefix(got, "vg-"), "expected a generated name, got %q", got)
		assert.False(t, fromTag)
		assert.True(t, ok)

		again, _, _ := lvgNameForCandidate(vg)
		assert.NotEqual(t, got, again, "generated names must not repeat")
	})

	// The agent only ever writes a valid resource name into the tag, but an
	// administrator handing a Volume Group over writes it by hand, and LVM's tag
	// charset is far wider than a resource name. Trusting it would make Create
	// fail with Invalid on every discovery cycle, with nothing in the log
	// pointing at the tag as the cause.
	t.Run("refuses a tag value that is not a usable resource name", func(t *testing.T) {
		cases := []struct {
			tagged string
			usable bool
			why    string
		}{
			{"lvg-original", true, "an ordinary name"},
			{strings.Repeat("a", 40) + ".b", true, "a subdomain under the label limit"},
			{"LVG-Original", false, "uppercase is legal in an LVM tag, not in a resource name"},
			{"lvg_original", false, "'_' likewise"},
			{"lvg/original", false, "'/' likewise"},
			{"-lvg", false, "a name may not start with '-'"},
			{strings.Repeat("a", 64), false, "longer than the kubernetes.io/metadata.name label allows"},
		}

		for _, tc := range cases {
			vg := internal.VGData{
				VGName: "vg-a",
				VGTags: "storage.deckhouse.io/enabled=true,storage.deckhouse.io/lvmVolumeGroupName=" + tc.tagged,
			}
			name, fromTag, ok := lvgNameForCandidate(vg)
			assert.True(t, fromTag, "the tag was read, whatever it holds: %s", tc.why)
			assert.Equal(t, tc.tagged, name, "the value is returned verbatim so the log can name it: %s", tc.why)
			assert.Equal(t, tc.usable, ok, "%s (%q)", tc.why, tc.tagged)
		}
	})
}

// An imported Volume Group built entirely on file devices has no block devices
// to select, and a selector that says "name in ()" is not merely useless — the
// apiserver refuses it, so every attempt to list BlockDevices for that
// LVMVolumeGroup errors out and it never leaves NoBlockDevices.
func TestConfigureBlockDeviceSelector(t *testing.T) {
	t.Run("no block devices yields no selector", func(t *testing.T) {
		got := configureBlockDeviceSelector(internal.LVMVolumeGroupCandidate{})
		assert.Nil(t, got, "a file-only Volume Group must be left without a selector")
	})

	t.Run("block devices are selected by name", func(t *testing.T) {
		got := configureBlockDeviceSelector(internal.LVMVolumeGroupCandidate{
			BlockDevicesNames: []string{"dev-a", "dev-b"},
		})
		if assert.NotNil(t, got) && assert.Len(t, got.MatchExpressions, 1) {
			assert.Equal(t, []string{"dev-a", "dev-b"}, got.MatchExpressions[0].Values)
			assert.Equal(t, metav1.LabelSelectorOpIn, got.MatchExpressions[0].Operator)
		}
	})
}

// pvSizeForFile returns the Physical Volume size LVM ends up with for a backing
// file of the given size on a Volume Group with the given extent: the file, minus
// the 1Mi metadata area pvcreate keeps at the head of the device by default,
// floored to whole extents.
//
// Tests derive PV sizes through this rather than listing them, because the two are
// not independent and an invented pair can describe a state no node can be in.
func pvSizeForFile(fileSize, extent resource.Quantity) resource.Quantity {
	const defaultPEStart = int64(1) << 20
	extentBytes := extent.Value()
	if extentBytes <= 0 {
		extentBytes = lvmDefaultPhysicalExtent.Value()
	}
	usable := fileSize.Value() - defaultPEStart
	return *resource.NewQuantity(usable/extentBytes*extentBytes, resource.BinarySI)
}

// An import has to reproduce the size the entry was created with, not a rounder
// number near it: the reconciler compares spec against the PV size and closes any
// gap by growing the backing file, so overshooting here silently enlarges the
// Volume Group the import was supposed to restore.
func TestReconstructFileDeviceSize(t *testing.T) {
	extent := resource.MustParse("4Mi")

	tests := map[string]struct {
		pvSize string
		want   string
	}{
		// `pvs` reports whole extents, so these are the real PV sizes LVM ends up
		// with for a 1Gi / 2Gi / 20Gi / 1536Mi backing file: the file minus the
		// 1Mi metadata area, floored to 4Mi.
		"1Gi entry":    {pvSize: "1020Mi", want: "1Gi"},
		"2Gi entry":    {pvSize: "2044Mi", want: "2Gi"},
		"20Gi entry":   {pvSize: "20476Mi", want: "20Gi"},
		"1536Mi entry": {pvSize: "1532Mi", want: "1536Mi"},
		// An unaligned reading must still not overshoot the extent of tolerance.
		"unaligned PV size": {pvSize: "1533Mi", want: "1536Mi"},
		// Never below what validateFileDevice accepts, however small the PV is.
		"clamped to the minimum": {pvSize: "1", want: "1Gi"},
		"tiny PV":                {pvSize: "512Mi", want: "1Gi"},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			pvSize := resource.MustParse(tt.pvSize)
			want := resource.MustParse(tt.want)
			got := reconstructFileDeviceSize(pvSize, extent)
			assert.Equal(t, want.Value(), got.Value())
		})
	}

	t.Run("a 1536Mi entry is not inflated to 2Gi", func(t *testing.T) {
		// The GiB-rounding this replaced turned this exact case into 2Gi, which
		// the next reconcile then grew the file device to.
		pvSize := resource.MustParse("1532Mi")
		twoGiB := resource.MustParse("2Gi")
		got := reconstructFileDeviceSize(pvSize, extent)
		assert.Less(t, got.Value(), twoGiB.Value())
	})

	// The whole point of flooring before adding an extent: for any backing file a
	// spec.fileDevices entry could legally have asked for, the reconstructed size
	// stays inside the tolerance growFileDevicesIfNeeded ignores, so an import never
	// triggers a grow.
	//
	// The PV sizes are derived from the file size rather than listed by hand,
	// because the two are not independent — pvSizeForFile is the arithmetic LVM
	// actually does — and an invented pair can be unreachable: a 1Gi file on a
	// 128Ki-extent Volume Group yields a 1023Mi PV, never a 1020Mi one, so
	// asserting on 1020Mi tests a state no node can be in.
	//
	// Asserted against fileDeviceGrowTolerance rather than a hand-written bound, and
	// across every extent size a Volume Group can have — including the sub-mebibyte
	// ones `vgcreate -s 128k` produces, where the mebibyte rounding below puts the
	// gap at 1Mi, which is eight extents. Spelling the bound out as `extent.Value()`
	// is what let the two sides of this invariant drift: the production comparison
	// used one extent, this test only ever exercised a 4Mi extent, and an imported
	// Volume Group with a 128Ki extent then re-ran the whole growth sequence on
	// every reconcile forever.
	t.Run("never exceeds the PV size by more than the grow tolerance", func(t *testing.T) {
		for _, extentRaw := range []string{"128Ki", "512Ki", "1Mi", "4Mi", "64Mi"} {
			for _, fileRaw := range []string{"1Gi", "1536Mi", "2Gi", "20Gi", "8191Mi"} {
				ext := resource.MustParse(extentRaw)
				pvSize := pvSizeForFile(resource.MustParse(fileRaw), ext)
				got := reconstructFileDeviceSize(pvSize, ext)
				assert.LessOrEqual(t, got.Value()-pvSize.Value(), fileDeviceGrowTolerance(ext),
					"extent %s, %s file yields PV %s, reconstructed to %s, which would make the reconciler grow the device",
					extentRaw, fileRaw, pvSize.String(), got.String())
			}
		}
	})

	// The lower clamp is the one case where the result CAN sit further above the PV
	// than the tolerance, and it is not the same defect: a PV below the smallest
	// legal entry does not correspond to any backing file the module would have
	// created, so there is no size to reproduce. Reconstructing to the 1Gi minimum
	// and letting the reconciler grow the file up to it is the intended outcome —
	// and unlike the mebibyte-rounding case it converges, because fallocate really
	// does enlarge the file and pvresize really does enlarge the PV.
	t.Run("the minimum clamp may legitimately ask for a grow, and it converges", func(t *testing.T) {
		extent := resource.MustParse("4Mi")
		got := reconstructFileDeviceSize(resource.MustParse("512Mi"), extent)
		assert.Equal(t, minFileDeviceSize.Value(), got.Value())

		// After that grow the file is 1Gi, so the PV becomes what a 1Gi file yields
		// — and then the gap is back inside the tolerance and nothing grows again.
		settled := pvSizeForFile(*got, extent)
		assert.LessOrEqual(t, got.Value()-settled.Value(), fileDeviceGrowTolerance(extent))
	})

	t.Run("a zero extent falls back to the LVM default", func(t *testing.T) {
		pvSize := resource.MustParse("20476Mi")
		want := resource.MustParse("20Gi")
		got := reconstructFileDeviceSize(pvSize, resource.Quantity{})
		assert.Equal(t, want.Value(), got.Value())
	})

	// The result is written into spec.fileDevices[].size, whose CRD pattern is
	// ^[0-9]+(Mi|Gi|Ti|Pi|Ei)$. resource.Quantity in BinarySI prints a suffix only
	// for a multiple of the corresponding power of 1024 and otherwise falls back to
	// a bare byte count — and Ki is not in the pattern either. `vgcreate -s 128k`
	// is legal, so a Volume Group handed over by an administrator can have a
	// sub-mebibyte extent, and the import would then fail apiserver validation on
	// every discovery cycle with nothing pointing at the extent size.
	t.Run("always expressible under the CRD size pattern", func(t *testing.T) {
		pattern := regexp.MustCompile(`^[0-9]+(Mi|Gi|Ti|Pi|Ei)$`)
		for _, extentRaw := range []string{"128Ki", "512Ki", "1Mi", "4Mi", "64Mi"} {
			for _, pvRaw := range []string{"1020Mi", "1532Mi", "2044Mi", "20476Mi", "1533Mi", "8191Mi", "1"} {
				ext, pv := resource.MustParse(extentRaw), resource.MustParse(pvRaw)
				got := reconstructFileDeviceSize(pv, ext)
				assert.Regexp(t, pattern, got.String(),
					"extent %s, PV %s reconstructed to %s, which the apiserver would reject", extentRaw, pvRaw, got.String())
				// Rounding up to a mebibyte must stay inside the tolerance
				// growFileDevicesIfNeeded ignores, or the import would grow the
				// device. The 1Gi floor is exempt: a PV below the smallest legal
				// entry has no size to reconstruct, and clamping it up is what keeps
				// the imported resource from failing its own validation.
				if got.Value() > minFileDeviceSize.Value() {
					assert.LessOrEqual(t, got.Value()-pv.Value(), fileDeviceGrowTolerance(ext),
						"extent %s, PV %s reconstructed to %s", extentRaw, pvRaw, got.String())
				}
			}
		}
	})
}

// A Volume Group whose owner tag names an existing LVMVolumeGroup is not imported
// under a generated name. Doing that was an unbounded loop: the name is random, so
// the next cycle finds no resource for the Volume Group either and creates another
// one — ninety LVMVolumeGroups for one VG in four seconds on the cluster where it
// fired, about nine hundred over a day.
//
// The wording is what an operator gets, and the two cases need different actions,
// so both are pinned here.
func TestImportRefusalReason(t *testing.T) {
	candidate := internal.LVMVolumeGroupCandidate{
		LVMVGName:             "lvg-worker-07",
		ActualVGNameOnTheNode: "vg-worker-07",
	}

	t.Run("the same VG already imported from another node — shared storage", func(t *testing.T) {
		taken := v1alpha1.LVMVolumeGroup{
			ObjectMeta: metav1.ObjectMeta{Name: "lvg-worker-07"},
			Spec: v1alpha1.LVMVolumeGroupSpec{
				ActualVGNameOnTheNode: "vg-worker-07",
				Local:                 v1alpha1.LVMVolumeGroupLocalSpec{NodeName: "node-a"},
			},
		}
		got := importRefusalReason(candidate, taken)
		assert.Contains(t, got, "lvg-worker-07")
		assert.Contains(t, got, "node-a")
		assert.Contains(t, got, "shared storage")
	})

	t.Run("one tag on two different Volume Groups — an operator mistake to report", func(t *testing.T) {
		taken := v1alpha1.LVMVolumeGroup{
			ObjectMeta: metav1.ObjectMeta{Name: "lvg-worker-07"},
			Spec: v1alpha1.LVMVolumeGroupSpec{
				ActualVGNameOnTheNode: "vg-something-else",
				Local:                 v1alpha1.LVMVolumeGroupLocalSpec{NodeName: "node-b"},
			},
		}
		got := importRefusalReason(candidate, taken)
		assert.Contains(t, got, "vg-something-else")
		assert.Contains(t, got, "two Volume Groups")
		assert.NotContains(t, got, "shared storage")
	})
}
