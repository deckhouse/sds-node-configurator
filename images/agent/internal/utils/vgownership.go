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

package utils

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
)

var clusterUUIDPattern = regexp.MustCompile(`^[A-Za-z0-9_+.-]+$`)

func ClusterTag(clusterUUID string) string {
	if !clusterUUIDPattern.MatchString(clusterUUID) {
		return ""
	}
	return internal.ClusterTagKey + "=" + clusterUUID
}

func HasTag(tags, tag string) bool {
	if tag == "" {
		return false
	}
	for _, t := range strings.Split(tags, ",") {
		if strings.TrimSpace(t) == tag {
			return true
		}
	}
	return false
}

func ForeignClusterTags(tags, clusterTag string) []string {
	var foreign []string
	for _, t := range strings.Split(tags, ",") {
		t = strings.TrimSpace(t)
		if t == clusterTag {
			continue
		}
		if name, _, ok := strings.Cut(t, "="); ok && name == internal.ClusterTagKey {
			foreign = append(foreign, t)
		}
	}
	return foreign
}

type VGOwnership struct {
	fromAPI    bool
	vgUUIDs    map[string]string
	clusterTag string
}

func NewVGOwnershipFromLVGs(lvgs []v1alpha1.LVMVolumeGroup, nodeName, clusterTag string) VGOwnership {
	o := VGOwnership{fromAPI: true, vgUUIDs: make(map[string]string), clusterTag: clusterTag}
	for i := range lvgs {
		lvg := &lvgs[i]
		if lvg.Spec.Local.NodeName != nodeName || lvg.Spec.ActualVGNameOnTheNode == "" {
			continue
		}
		o.vgUUIDs[lvg.Spec.ActualVGNameOnTheNode] = lvg.Status.VGUuid
	}
	return o
}

func NewVGOwnershipFromClusterTag(clusterTag string) VGOwnership {
	return VGOwnership{clusterTag: clusterTag}
}

func (o VGOwnership) Owns(vg internal.VGData) bool {
	if o.clusterTag != "" && len(ForeignClusterTags(vg.VGTags, o.clusterTag)) > 0 {
		return false
	}
	if !o.fromAPI {
		return HasTag(vg.VGTags, o.clusterTag)
	}
	uuid, ok := o.vgUUIDs[vg.VGName]
	if !ok {
		return false
	}
	return uuid == "" || uuid == vg.VGUUID
}

func (o VGOwnership) reason(vg internal.VGData) string {
	if o.clusterTag != "" {
		if foreign := ForeignClusterTags(vg.VGTags, o.clusterTag); len(foreign) > 0 {
			return fmt.Sprintf("the VG is tagged %s, it belongs to another cluster", strings.Join(foreign, ","))
		}
	}
	if !o.fromAPI {
		if o.clusterTag == "" {
			return "the LVMVolumeGroups are unavailable and the cluster tag is unknown"
		}
		return fmt.Sprintf("the LVMVolumeGroups are unavailable and the VG does not carry %s", o.clusterTag)
	}
	if uuid, ok := o.vgUUIDs[vg.VGName]; ok {
		return fmt.Sprintf("the LVMVolumeGroup of this node expects the VG UUID %s, the VG has %s", uuid, vg.VGUUID)
	}
	return "no LVMVolumeGroup of this node names it"
}

func FilterOwnedVGs(log logger.Logger, action string, vgs []internal.VGData, o VGOwnership, logSkipped func(string)) []internal.VGData {
	owned := make([]internal.VGData, 0, len(vgs))
	for _, vg := range vgs {
		if o.Owns(vg) {
			owned = append(owned, vg)
			continue
		}
		msg := fmt.Sprintf("not going to %s VG %s (UUID %s): %s", action, vg.VGName, vg.VGUUID, o.reason(vg))
		if logSkipped != nil {
			logSkipped(msg)
		} else {
			log.Debug(msg)
		}
	}
	return owned
}

type VGOwnershipSource func(ctx context.Context) VGOwnership

func NewVGOwnershipSource(log logger.Logger, nodeName, clusterTag string, timeout time.Duration, readers ...client.Reader) VGOwnershipSource {
	return func(ctx context.Context) VGOwnership {
		var errs []error
		for _, reader := range readers {
			list, err := RunWithTimeout(ctx, timeout, func(ctx context.Context) (*v1alpha1.LVMVolumeGroupList, error) {
				list := &v1alpha1.LVMVolumeGroupList{}
				return list, reader.List(ctx, list)
			})
			if err == nil {
				return NewVGOwnershipFromLVGs(list.Items, nodeName, clusterTag)
			}
			errs = append(errs, err)
		}
		if clusterTag == "" {
			log.Warn(fmt.Sprintf("unable to list LVMVolumeGroups and the cluster tag is unknown, no VG counts as this node's: %v", errors.Join(errs...)))
		} else {
			log.Warn(fmt.Sprintf("unable to list LVMVolumeGroups, only VGs tagged %s count as this node's: %v", clusterTag, errors.Join(errs...)))
		}
		return NewVGOwnershipFromClusterTag(clusterTag)
	}
}

func hasLegacyTag(vgs []internal.VGData, lvs []internal.LVData) bool {
	names := make(map[string]struct{}, len(vgs))
	for _, vg := range vgs {
		if strings.Contains(vg.VGTags, internal.LVMTags[1]) {
			return true
		}
		names[vg.VGName] = struct{}{}
	}
	for _, lv := range lvs {
		if _, ok := names[lv.VGName]; ok && strings.Contains(lv.LvTags, internal.LVMTags[1]) {
			return true
		}
	}
	return false
}

func ownVGsByUniqueName(all, owned []internal.VGData) ([]internal.VGData, map[string]struct{}) {
	ownedUUIDs := make(map[string]struct{}, len(owned))
	for _, vg := range owned {
		ownedUUIDs[vg.VGUUID] = struct{}{}
	}
	shared := make(map[string]struct{})
	for _, vg := range all {
		if _, ok := ownedUUIDs[vg.VGUUID]; !ok {
			shared[vg.VGName] = struct{}{}
		}
	}
	names := make(map[string]struct{}, len(owned))
	kept := make([]internal.VGData, 0, len(owned))
	for _, vg := range owned {
		if _, ok := shared[vg.VGName]; ok {
			continue
		}
		names[vg.VGName] = struct{}{}
		kept = append(kept, vg)
	}
	return kept, names
}
