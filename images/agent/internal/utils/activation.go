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

package utils

import (
	"context"
	"fmt"
	"strings"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/monitoring"
)

const (
	lvAttrMinLen       = 5
	lvAttrActiveIndex  = 4
	lvAttrActiveChar   = 'a'
	lvAttrTypeThinPool = 't'
	lvAttrTypeThick    = '-'

	activationControllerName = "Activation"
)

func IsLVActive(attr string) bool {
	return len(attr) > lvAttrActiveIndex && attr[lvAttrActiveIndex] == lvAttrActiveChar
}

func IsLVThinPool(attr string) bool {
	return len(attr) > 0 && attr[0] == lvAttrTypeThinPool
}

func IsLVThick(attr string) bool {
	return len(attr) > 0 && attr[0] == lvAttrTypeThick
}

func FilterVGsByTag(vgs []internal.VGData, tags []string) []internal.VGData {
	filtered := make([]internal.VGData, 0, len(vgs))
	for _, vg := range vgs {
		for _, tag := range tags {
			if strings.Contains(vg.VGTags, tag) {
				filtered = append(filtered, vg)
				break
			}
		}
	}
	return filtered
}

func ActivateAllManagedVGs(ctx context.Context, log logger.Logger, commands Commands, metrics *monitoring.Metrics) error {
	log.Info("[ActivateVGs] refreshing LVM metadata cache")
	if cmd, err := commands.PVScan(ctx); err != nil {
		log.Warning(fmt.Sprintf("[ActivateVGs] pvscan --cache failed (cmd: %s): %v", cmd, err))
	}
	if cmd, err := commands.VGScan(ctx); err != nil {
		log.Warning(fmt.Sprintf("[ActivateVGs] vgscan --cache failed (cmd: %s): %v", cmd, err))
	}

	vgs, cmdStr, _, err := commands.GetAllVGs(ctx)
	log.Debug(fmt.Sprintf("[ActivateVGs] exec cmd: %s", cmdStr))
	if err != nil {
		return fmt.Errorf("unable to get VGs: %w", err)
	}

	managedVGs := FilterVGsByTag(vgs, internal.LVMTags)
	if len(managedVGs) == 0 {
		log.Info("[ActivateVGs] no managed VGs found, nothing to activate")
		return nil
	}

	log.Info(fmt.Sprintf("[ActivateVGs] found %d managed VGs to activate", len(managedVGs)))

	var activationErrors []error
	for _, vg := range managedVGs {
		shared := vg.VGShared != ""
		cmd, err := commands.VGActivate(vg.VGName, shared)
		if err != nil {
			log.Error(err, fmt.Sprintf("[ActivateVGs] failed to activate VG %s (shared=%t, cmd: %s)", vg.VGName, shared, cmd))
			metrics.UtilsCommandsErrorsCount(activationControllerName, "vgchange").Inc()
			activationErrors = append(activationErrors, fmt.Errorf("VG %s: %w", vg.VGName, err))
			continue
		}
		metrics.UtilsCommandsExecutionCount(activationControllerName, "vgchange").Inc()
		log.Info(fmt.Sprintf("[ActivateVGs] activated VG %s (shared=%t)", vg.VGName, shared))
	}

	if len(activationErrors) > 0 {
		return fmt.Errorf("failed to activate %d VGs", len(activationErrors))
	}
	return nil
}

func EnsureVGActivation(
	_ context.Context,
	log logger.Logger,
	commands Commands,
	metrics *monitoring.Metrics,
	vgs []internal.VGData,
	lvs []internal.LVData,
) bool {
	managedVGs := FilterVGsByTag(vgs, internal.LVMTags)
	if len(managedVGs) == 0 {
		return false
	}

	managedVGSet := make(map[string]internal.VGData, len(managedVGs))
	for _, vg := range managedVGs {
		managedVGSet[vg.VGName] = vg
	}

	vgsToActivate := make(map[string]internal.VGData)
	for _, lv := range lvs {
		vg, managed := managedVGSet[lv.VGName]
		if !managed {
			continue
		}

		if !IsLVThinPool(lv.LVAttr) && !IsLVThick(lv.LVAttr) {
			continue
		}

		if !IsLVActive(lv.LVAttr) {
			log.Warning(fmt.Sprintf("[EnsureActivation] detected inactive LV %s/%s (attr=%s)", lv.VGName, lv.LVName, lv.LVAttr))
			vgsToActivate[vg.VGName] = vg
		}
	}

	if len(vgsToActivate) == 0 {
		return false
	}

	log.Info(fmt.Sprintf("[EnsureActivation] activating %d VGs with inactive LVs", len(vgsToActivate)))
	for _, vg := range vgsToActivate {
		shared := vg.VGShared != ""
		cmd, err := commands.VGActivate(vg.VGName, shared)
		if err != nil {
			log.Error(err, fmt.Sprintf("[EnsureActivation] failed to activate VG %s (cmd: %s)", vg.VGName, cmd))
			metrics.UtilsCommandsErrorsCount(activationControllerName, "vgchange").Inc()
			continue
		}
		metrics.UtilsCommandsExecutionCount(activationControllerName, "vgchange").Inc()
		log.Info(fmt.Sprintf("[EnsureActivation] activated VG %s (shared=%t)", vg.VGName, shared))
	}

	return true
}
