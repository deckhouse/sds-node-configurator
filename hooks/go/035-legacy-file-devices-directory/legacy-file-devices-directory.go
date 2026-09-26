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

// Package hooks_common reports the file devices left behind by the move of the
// module's node tree from /opt/deckhouse/sds to /var/lib/deckhouse/sds.
//
// The binaries the agent plants on the node move with the release: the init
// container writes them to the new path and a NodeGroupConfiguration removes the
// old copies. The backing files of spec.fileDevices entries cannot follow, and
// this hook exists because of that asymmetry.
//
// Why they cannot follow. spec.fileDevices[].directory is immutable at admission
// (`self.directory == oldSelf.directory` in crds/storage.deckhouse.io_lvmvolumegroups.yaml), so an
// entry created under the old default cannot be edited to the new one — not by
// the admin, and not by this hook either: the apiserver rejects the write the
// same way. Nor can the file simply be moved on the node: the spec would still
// name the old directory, and the next reconcile would create a second backing
// file there and add a second Physical Volume to the Volume Group. So the agent
// keeps accepting the legacy base directory (internal.LegacyFileDevicesDirectory)
// and those entries go on working where they are, indefinitely.
//
// What is left for a hook, then, is the one thing the cluster side can do:
// say how much is still there. Without it the compatibility shim in the agent
// has no retirement criterion — nobody can tell whether any cluster still needs
// it — and an admin has no way to find the Volume Groups whose backing files sit
// in a tree the module otherwise no longer uses.
//
// It therefore exports one gauge, sds_node_configurator_legacy_file_devices_directory_lvgs,
// the number of LVMVolumeGroups that still name the legacy directory, and logs
// their names. Zero is the state in which the shim, this hook and the
// NodeGroupConfiguration can all be deleted. The agent accepts the legacy
// directory for a new entry too, so the count is not guaranteed to only fall;
// the log line is what tells an admin to stop adding entries there.
//
// The gauge carries no per-object labels on purpose. Per-LVG series would need
// group expiry to disappear when an object does, and metric groups are a no-op
// in module-sdk v0.7.0 (operation.WithGroup has a value receiver), so they would
// be stale forever. The names go to the log, which needs no expiry.
//
// The hook never fails the converge: it only observes, and a transient API error
// must not block the module release. The failure is logged and the next
// beforeHelm retries.
package hooks_common

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/deckhouse/module-sdk/pkg"
	"github.com/deckhouse/module-sdk/pkg/registry"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-common-lib/kubeclient"
	"github.com/deckhouse/sds-node-configurator/hooks/go/consts"
)

const legacyFileDevicesDirectoryHookName = "legacy-file-devices-directory"

// legacyFileDevicesMetric is the gauge this hook maintains. It shares the
// module's own metric namespace (see images/agent/internal/monitoring) rather
// than taking the d8_ prefix of platform metrics: it is a module fact.
const legacyFileDevicesMetric = "sds_node_configurator_legacy_file_devices_directory_lvgs"

// legacyFileDevicesDirectory duplicates internal.LegacyFileDevicesDirectory of
// the agent. The two cannot share a constant — the hooks and the agent are
// separate Go modules, and the agent's internal package is not importable — so
// they are kept in sync by the comment on each.
const legacyFileDevicesDirectory = "/opt/deckhouse/sds/file-devices"

// loggedLVGNames bounds the log line: a cluster can hold a lot of Volume Groups,
// and the gauge already carries the full count.
const loggedLVGNames = 20

var lvgGVK = schema.GroupVersionKind{
	Group:   consts.APIGroup,
	Version: "v1alpha1",
	Kind:    "LVMVolumeGroup",
}

var _ = registry.RegisterFunc(configLegacyFileDevicesDirectory, handlerLegacyFileDevicesDirectory)

var configLegacyFileDevicesDirectory = &pkg.HookConfig{
	OnBeforeHelm: &pkg.OrderedConfig{Order: 10},
}

// handlerLegacyFileDevicesDirectory is the thin OnBeforeHelm wrapper: it builds
// the kube client and delegates to reportLegacyFileDevicesDirectory, which holds
// the testable logic.
func handlerLegacyFileDevicesDirectory(ctx context.Context, input *pkg.HookInput) error {
	cl, err := kubeclient.New()
	if err != nil {
		input.Logger.Error(fmt.Sprintf("[%s]: failed to initialize kube client, skipping: %v", legacyFileDevicesDirectoryHookName, err))
		return nil
	}
	reportLegacyFileDevicesDirectory(ctx, cl, input.MetricsCollector, input.Logger)
	return nil
}

// reportLegacyFileDevicesDirectory sets the gauge to the number of
// LVMVolumeGroups that still name the legacy base directory and logs which they
// are.
//
// On a listing error the gauge is left untouched rather than set to zero: an
// unreachable apiserver is not the same answer as "nothing is left", and writing
// zero would resolve an alert that is still true.
func reportLegacyFileDevicesDirectory(ctx context.Context, cl client.Client, metrics pkg.MetricsCollector, logger pkg.Logger) {
	listGVK := lvgGVK
	listGVK.Kind += "List"

	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(listGVK)
	if err := cl.List(ctx, list); err != nil {
		if meta.IsNoMatchError(err) {
			// The CRD is not served yet: this is the first converge of a fresh
			// install, and there is nothing older than the move in it.
			metrics.Set(legacyFileDevicesMetric, 0, nil)
			return
		}
		logger.Warn(fmt.Sprintf("[%s]: failed to list %s, retrying next converge: %v", legacyFileDevicesDirectoryHookName, lvgGVK.Kind, err))
		return
	}

	names := lvgsOnLegacyDirectory(list, logger)
	metrics.Set(legacyFileDevicesMetric, float64(len(names)), nil)

	if len(names) == 0 {
		return
	}

	logger.Warn(fmt.Sprintf("[%s]: %d LVMVolumeGroup(s) still keep their spec.fileDevices backing files in %s, the module's previous default base directory: %s. Their `directory` is immutable, so they keep working where they are, but do not add new entries there; to move them, add an entry under the current base directory, pvmove the data off the old Physical Volume, then vgreduce and pvremove it (see the module FAQ).",
		legacyFileDevicesDirectoryHookName, len(names), legacyFileDevicesDirectory, strings.Join(trimNames(names), ", ")))
}

// lvgsOnLegacyDirectory returns the names of the LVMVolumeGroups holding at
// least one spec.fileDevices entry inside the legacy base directory, sorted so
// the log line is stable between converges.
//
// Only the spec is read. status.nodes[].fileDevices carries the path the node
// actually reports, but the spec is what the agent validates and what an admin
// would have to change, and a Volume Group is counted once however many of its
// entries are affected.
func lvgsOnLegacyDirectory(list *unstructured.UnstructuredList, logger pkg.Logger) []string {
	names := make([]string, 0)

	for i := range list.Items {
		lvg := &list.Items[i]
		fileDevices, found, err := unstructured.NestedSlice(lvg.Object, "spec", "fileDevices")
		if err != nil {
			// A spec shaped unlike the CRD is not this hook's problem to fix, and
			// it must not stop the count of the objects that are shaped right.
			logger.Warn(fmt.Sprintf("[%s]: cannot read spec.fileDevices of the LVMVolumeGroup %s, skipping it: %v", legacyFileDevicesDirectoryHookName, lvg.GetName(), err))
			continue
		}
		if !found {
			continue
		}

		for _, entry := range fileDevices {
			fields, ok := entry.(map[string]any)
			if !ok {
				continue
			}
			dir, ok := fields["directory"].(string)
			if !ok || dir == "" {
				continue
			}
			if !isWithinLegacyDirectory(dir) {
				continue
			}
			names = append(names, lvg.GetName())
			break
		}
	}

	sort.Strings(names)
	return names
}

// isWithinLegacyDirectory reports whether dir is the legacy base directory or a
// subdirectory of it. It mirrors utils.IsWithinBaseDir on the agent side: a
// lexical compare after filepath.Clean, so a path that merely shares the prefix
// ("…/file-devices-2") is not counted.
func isWithinLegacyDirectory(dir string) bool {
	dir = filepath.Clean(dir)
	return dir == legacyFileDevicesDirectory || strings.HasPrefix(dir, legacyFileDevicesDirectory+string(filepath.Separator))
}

func trimNames(names []string) []string {
	if len(names) <= loggedLVGNames {
		return names
	}
	return append(names[:loggedLVGNames:loggedLVGNames], fmt.Sprintf("and %d more", len(names)-loggedLVGNames))
}
