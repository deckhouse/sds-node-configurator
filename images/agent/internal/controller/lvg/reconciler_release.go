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
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/deckhouse/sds-common-lib/conditions"
	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

const releaseConfirmWindow = time.Minute

type releaseSighting struct {
	pvUUID string
	passes int
	last   time.Time
}

type releaseAssessment struct {
	releasable bool
	pvName     string
	pvUUID     string
	note       string
}

type lvmView struct {
	pvs []internal.PVData
	vgs []internal.VGData
	lvs []internal.LVData
}

func lostBlockDeviceNames(lvg *v1alpha1.LVMVolumeGroup, blockDevices map[string]v1alpha1.BlockDevice) []string {
	var lost []string
	for _, n := range lvg.Status.Nodes {
		for _, d := range n.Devices {
			if _, found := blockDevices[d.BlockDevice]; !found && !slices.Contains(lost, d.BlockDevice) {
				lost = append(lost, d.BlockDevice)
			}
		}
	}
	slices.Sort(lost)
	return lost
}

func releaseKey(lvgName, bdName string) string {
	return lvgName + "/" + bdName
}

func (r *Reconciler) clock() time.Time {
	if r.now != nil {
		return r.now()
	}
	return time.Now()
}

func (r *Reconciler) confirmWindow() time.Duration {
	return max(r.requeueInterval(), releaseConfirmWindow)
}

func (r *Reconciler) forgetReleaseSightings(lvgName string, keep []string) {
	r.releaseMu.Lock()
	defer r.releaseMu.Unlock()
	for key := range r.releaseSightings {
		if !strings.HasPrefix(key, lvgName+"/") {
			continue
		}
		if !slices.Contains(keep, strings.TrimPrefix(key, lvgName+"/")) {
			delete(r.releaseSightings, key)
		}
	}
}

func (r *Reconciler) forgetReleaseSighting(key string) {
	r.releaseMu.Lock()
	defer r.releaseMu.Unlock()
	delete(r.releaseSightings, key)
}

func (r *Reconciler) confirmRelease(key, pvUUID string) bool {
	r.releaseMu.Lock()
	defer r.releaseMu.Unlock()
	if r.releaseSightings == nil {
		r.releaseSightings = make(map[string]releaseSighting)
	}
	now := r.clock()
	seen, ok := r.releaseSightings[key]
	if !ok || seen.pvUUID != pvUUID {
		r.releaseSightings[key] = releaseSighting{pvUUID: pvUUID, passes: 1, last: now}
		return false
	}
	if now.Sub(seen.last) < r.confirmWindow() {
		return false
	}
	seen.passes++
	seen.last = now
	r.releaseSightings[key] = seen
	return seen.passes >= 2
}

func (r *Reconciler) releaseLostBlockDevices(ctx context.Context, lvg *v1alpha1.LVMVolumeGroup, blockDevices map[string]v1alpha1.BlockDevice) (string, time.Duration) {
	log := r.log

	lost := lostBlockDeviceNames(lvg, blockDevices)
	r.forgetReleaseSightings(lvg.Name, lost)
	if len(lost) == 0 {
		return "", 0
	}

	enabled := lvg.Spec.ReleaseEmptyBlockDevices
	var (
		view    *lvmView
		viewErr error
	)
	readLVM := func() (*lvmView, error) {
		if view == nil && viewErr == nil {
			v, err := r.readLVM(ctx, enabled)
			if err != nil {
				viewErr = err
			} else {
				view = &v
			}
		}
		return view, viewErr
	}

	notes := make([]string, 0, len(lost))
	pending, released := false, false
	vgName := lvg.Spec.ActualVGNameOnTheNode
	for _, name := range lost {
		key := releaseKey(lvg.Name, name)
		a := r.assessLostBlockDevice(ctx, lvg, name, readLVM)
		if !a.releasable {
			r.forgetReleaseSighting(key)
			notes = append(notes, a.note)
			continue
		}
		if !r.confirmRelease(key, a.pvUUID) {
			pending = true
			notes = append(notes, fmt.Sprintf("%s: its Physical Volume %s is empty and is removed from VG %s if this still holds on a pass at least %s later", name, a.pvName, vgName, r.confirmWindow()))
			continue
		}
		r.forgetReleaseSighting(key)
		view, viewErr = nil, nil
		if err := r.reducePV(ctx, vgName, a.pvName); err != nil {
			log.Error(fmt.Sprintf("unable to release the BlockDevice %s from VG %s of the LVMVolumeGroup %s", name, vgName, lvg.Name), logger.Err(err))
			notes = append(notes, fmt.Sprintf("%s: unable to remove its Physical Volume %s from VG %s: %s", name, a.pvName, vgName, err.Error()))
			continue
		}
		released = true
		log.Info(fmt.Sprintf("the BlockDevice %s was released from VG %s of the LVMVolumeGroup %s: its empty Physical Volume %s was removed", name, vgName, lvg.Name, a.pvName))
		notes = append(notes, fmt.Sprintf("%s: its empty Physical Volume %s was removed from VG %s; waiting for the status to be refreshed", name, a.pvName, vgName))
	}

	message := fmt.Sprintf("these BlockDevices no longer match the blockDeviceSelector: %s. %s", strings.Join(lost, ","), strings.Join(notes, "; "))
	switch {
	case released:
		r.resetNoProgressRetries(lvg.Name)
		return message, r.requeueInterval()
	case pending:
		r.resetNoProgressRetries(lvg.Name)
		return message, r.confirmWindow()
	default:
		return message, r.noProgressRequeueAfter(lvg.Name)
	}
}

func (r *Reconciler) readLVM(ctx context.Context, fresh bool) (lvmView, error) {
	if !fresh {
		pvs, _ := r.sdsCache.GetPVs()
		vgs, _ := r.sdsCache.GetVGs()
		lvs, _ := r.sdsCache.GetLVs()
		return lvmView{pvs: pvs, vgs: vgs, lvs: lvs}, nil
	}

	pvs, err := utils.RunWithTimeout(ctx, r.cfg.CmdDeadlineDuration, func(ctx context.Context) ([]internal.PVData, error) {
		pvs, command, _, err := r.commands.GetAllPVs(ctx)
		r.log.Debug(command)
		return pvs, err
	})
	if err != nil {
		return lvmView{}, fmt.Errorf("unable to list the Physical Volumes: %w", err)
	}
	vgs, err := utils.RunWithTimeout(ctx, r.cfg.CmdDeadlineDuration, func(ctx context.Context) ([]internal.VGData, error) {
		vgs, command, _, err := r.commands.GetAllVGs(ctx)
		r.log.Debug(command)
		return vgs, err
	})
	if err != nil {
		return lvmView{}, fmt.Errorf("unable to list the Volume Groups: %w", err)
	}
	lvs, err := utils.RunWithTimeout(ctx, r.cfg.CmdDeadlineDuration, func(ctx context.Context) ([]internal.LVData, error) {
		lvs, command, _, err := r.commands.GetAllLVs(ctx)
		r.log.Debug(command)
		return lvs, err
	})
	if err != nil {
		return lvmView{}, fmt.Errorf("unable to list the Logical Volumes: %w", err)
	}
	return lvmView{pvs: pvs, vgs: vgs, lvs: lvs}, nil
}

func (r *Reconciler) reducePV(ctx context.Context, vgName, pvName string) error {
	start := time.Now()
	command, err := utils.RunWithTimeout(ctx, r.cfg.CmdDeadlineDuration, func(ctx context.Context) (string, error) {
		return r.commands.ReduceVG(ctx, vgName, pvName)
	})
	r.metrics.UtilsCommandsDuration(ReconcilerName, "vgreduce").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
	r.metrics.UtilsCommandsExecutionCount(ReconcilerName, "vgreduce").Inc()
	r.log.Debug(command)
	if err != nil {
		r.metrics.UtilsCommandsErrorsCount(ReconcilerName, "vgreduce").Inc()
		return err
	}

	start = time.Now()
	command, err = r.commands.RemovePV([]string{pvName})
	r.metrics.UtilsCommandsDuration(ReconcilerName, "pvremove").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
	r.metrics.UtilsCommandsExecutionCount(ReconcilerName, "pvremove").Inc()
	r.log.Debug(command)
	if err != nil {
		r.metrics.UtilsCommandsErrorsCount(ReconcilerName, "pvremove").Inc()
		return err
	}

	r.triggerUdevForPaths(ctx, []string{pvName})
	return nil
}

func blockedRelease(format string, args ...any) releaseAssessment {
	return releaseAssessment{note: fmt.Sprintf(format, args...)}
}

func (r *Reconciler) otherBlockDevicesWithPV(ctx context.Context, bdName, nodeName, pvUUID string) ([]string, error) {
	all, err := r.bdAPICl.GetAPIBlockDevices(ctx, ReconcilerName, nil)
	if err != nil {
		return nil, err
	}
	var others []string
	for name, bd := range all {
		if name != bdName && bd.Status.NodeName == nodeName && bd.Status.PVUuid == pvUUID {
			others = append(others, name)
		}
	}
	slices.Sort(others)
	return others, nil
}

func (r *Reconciler) assessLostBlockDevice(ctx context.Context, lvg *v1alpha1.LVMVolumeGroup, bdName string, readLVM func() (*lvmView, error)) releaseAssessment {
	enabled := lvg.Spec.ReleaseEmptyBlockDevices
	vgName := lvg.Spec.ActualVGNameOnTheNode

	bd, err := r.bdAPICl.GetAPIBlockDevice(ctx, ReconcilerName, bdName)
	if apierrors.IsNotFound(err) {
		return blockedRelease("%s: the BlockDevice resource does not exist, so a device taken out of the selector cannot be told from a device that is gone; nothing is removed", bdName)
	}
	if err != nil {
		return blockedRelease("%s: unable to read the BlockDevice, nothing is removed: %s", bdName, err.Error())
	}

	selector, err := v1.LabelSelectorAsSelector(lvg.Spec.BlockDeviceSelector)
	if err != nil {
		return blockedRelease("%s: unable to parse the blockDeviceSelector, nothing is removed: %s", bdName, err.Error())
	}
	if selector.Matches(labels.Set(bd.Labels)) {
		return blockedRelease("%s: the labels of the BlockDevice match the blockDeviceSelector, so it is not taken out of the selector and nothing is removed", bdName)
	}
	if bd.Status.NodeName != lvg.Spec.Local.NodeName {
		return blockedRelease("%s: the BlockDevice is on the node %s, not on %s; nothing is removed", bdName, bd.Status.NodeName, lvg.Spec.Local.NodeName)
	}
	if bd.Status.Type == internal.LoopDeviceType {
		return blockedRelease("%s: the BlockDevice is a loop device; only block devices are released, a file device is changed in spec.fileDevices", bdName)
	}
	if conditions.Get(bd.Status.Conditions, v1alpha1.BlockDeviceConditionMissing) != nil {
		return blockedRelease("%s: the BlockDevice is marked Missing, so the device is gone from the node rather than taken out of the selector; nothing is removed", bdName)
	}

	pvUUID := ""
	for _, n := range lvg.Status.Nodes {
		for _, d := range n.Devices {
			if d.BlockDevice == bdName && d.PVUuid != "" {
				pvUUID = d.PVUuid
			}
		}
	}
	if pvUUID == "" {
		return blockedRelease("%s: the status of the LVMVolumeGroup has no Physical Volume UUID for it, and a Physical Volume is never picked by its path alone; nothing is removed", bdName)
	}

	twins, err := r.otherBlockDevicesWithPV(ctx, bdName, bd.Status.NodeName, pvUUID)
	if err != nil {
		return blockedRelease("%s: unable to list the BlockDevices to look for another one with the same Physical Volume, nothing is removed: %s", bdName, err.Error())
	}
	if len(twins) > 0 {
		return blockedRelease("%s: the BlockDevice %s on the same node carries the same Physical Volume %s, so the device has changed its identity or is seen through another path rather than taken out of the selector; nothing is removed", bdName, strings.Join(twins, ","), pvUUID)
	}

	view, err := readLVM()
	if err != nil {
		return blockedRelease("%s: %s, nothing is removed", bdName, err.Error())
	}

	var pv *internal.PVData
	for i := range view.pvs {
		if view.pvs[i].PVUuid == pvUUID {
			pv = &view.pvs[i]
			break
		}
	}
	if pv == nil {
		return blockedRelease("%s: LVM reports no Physical Volume with the UUID %s: it was removed from the VG already and the status is to be refreshed, or LVM does not see the device; nothing is removed", bdName, pvUUID)
	}
	if pv.VGName == "" {
		return blockedRelease("%s: its Physical Volume %s (UUID %s) is in no Volume Group; the agent removes a Physical Volume only from the Volume Group of this LVMVolumeGroup, so nothing is removed; run `pvremove %s` on the node if it is not needed", bdName, pv.PVName, pvUUID, pv.PVName)
	}
	if lvg.Status.VGUuid == "" || pv.VGUuid != lvg.Status.VGUuid || pv.VGName != vgName {
		return blockedRelease("%s: its Physical Volume %s belongs to VG %s (UUID %s), not to VG %s of this LVMVolumeGroup (UUID %s); nothing is removed", bdName, pv.PVName, pv.VGName, pv.VGUuid, vgName, lvg.Status.VGUuid)
	}
	if _, owner := utils.ReadValueFromTags(pv.VGTags, internal.LVMVolumeGroupTag); owner != lvg.Name {
		return blockedRelease("%s: VG %s does not carry the tag %s=%s, so it is not known to belong to this LVMVolumeGroup; nothing is removed", bdName, vgName, internal.LVMVolumeGroupTag, lvg.Name)
	}

	var vg *internal.VGData
	for i := range view.vgs {
		if view.vgs[i].VGUUID == pv.VGUuid {
			vg = &view.vgs[i]
			break
		}
	}
	if vg == nil {
		systemID := ""
		if pv.VGSystemID != "" {
			systemID = fmt.Sprintf(" (system ID %s)", pv.VGSystemID)
		}
		return blockedRelease("%s: VG %s%s is not listed by vgs on this node, as happens to a Volume Group owned by another host; nothing is removed", bdName, vgName, systemID)
	}
	if vg.VGShared != "" {
		return blockedRelease("%s: VG %s is a shared Volume Group, which the agent does not shrink; nothing is removed", bdName, vgName)
	}

	used, err := utils.ParsePVUsedBytes(pv.PVUsed)
	if err != nil {
		return blockedRelease("%s: unable to tell whether its Physical Volume %s is empty, nothing is removed: %s", bdName, pv.PVName, err.Error())
	}
	if used > 0 {
		extents := ""
		if extent := lvg.Status.ExtentSize.Value(); extent > 0 {
			extents = fmt.Sprintf(" (%d extents)", (used+extent-1)/extent)
		}
		next := fmt.Sprintf("run `pvmove %s` on the node to free it", pv.PVName)
		if enabled {
			next += "; the agent then removes the empty Physical Volume itself"
		} else {
			next += ", then set spec.releaseEmptyBlockDevices to true to let the agent remove it, or run `vgreduce` and `pvremove` by hand"
		}
		return blockedRelease("%s: its Physical Volume %s holds %s of data%s and is not removed; %s", bdName, pv.PVName, resource.NewQuantity(used, resource.BinarySI).String(), extents, next)
	}

	others := 0
	for i := range view.pvs {
		if view.pvs[i].VGUuid == pv.VGUuid && view.pvs[i].PVUuid != pv.PVUuid {
			others++
		}
	}
	if others == 0 {
		return blockedRelease("%s: its Physical Volume %s is the only one in VG %s; removing it would remove the Volume Group, so it stays", bdName, pv.PVName, vgName)
	}

	for _, lv := range view.lvs {
		if lv.VGUuid != pv.VGUuid {
			continue
		}
		if strings.HasPrefix(lv.LVName, "[pvmove") || strings.HasPrefix(lv.LVName, "pvmove") || lv.MovePv != "" {
			return blockedRelease("%s: a pvmove is running in VG %s (%s); nothing is removed until it ends", bdName, vgName, lv.LVName)
		}
	}

	if note := thinPoolOutgrowsShrunkVG(lvg, *vg, *pv, view.lvs); note != "" {
		return blockedRelease("%s: %s; nothing is removed", bdName, note)
	}

	if !enabled {
		return blockedRelease("%s: its Physical Volume %s is empty; set spec.releaseEmptyBlockDevices to true to let the agent remove it from VG %s, or run `vgreduce %s %s` and `pvremove %s` on the node", bdName, pv.PVName, vgName, vgName, pv.PVName, pv.PVName)
	}

	return releaseAssessment{releasable: true, pvName: pv.PVName, pvUUID: pv.PVUuid}
}

func thinPoolOutgrowsShrunkVG(lvg *v1alpha1.LVMVolumeGroup, vg internal.VGData, pv internal.PVData, lvs []internal.LVData) string {
	shrunk := *resource.NewQuantity(vg.VGSize.Value()-pv.PVSize.Value(), resource.BinarySI)
	extent := extentSizeForThinPoolAlign(lvg, &vg)
	for _, tp := range lvg.Spec.ThinPools {
		if !utils.IsPercentSize(tp.Size) {
			continue
		}
		var actual *internal.LVData
		for i := range lvs {
			if lvs[i].VGUuid == vg.VGUUID && lvs[i].LVName == tp.Name && utils.IsLVThinPool(lvs[i].LVAttr) {
				actual = &lvs[i]
				break
			}
		}
		if actual == nil {
			continue
		}
		requested, err := utils.GetRequestedSizeFromString(tp.Size, shrunk)
		if err != nil {
			return fmt.Sprintf("unable to tell the size of thin pool %s in VG %s without this Physical Volume: %s", tp.Name, vg.VGName, err.Error())
		}
		aligned, err := utils.AlignSizeToExtent(requested, extent)
		if err != nil {
			return fmt.Sprintf("unable to tell the size of thin pool %s in VG %s without this Physical Volume: %s", tp.Name, vg.VGName, err.Error())
		}
		if aligned.Value() >= shrunk.Value() {
			continue
		}
		if actual.LVSize.Value()-aligned.Value() > extent.Value() {
			return fmt.Sprintf("without its Physical Volume %s, VG %s would be %s, and thin pool %s, sized %s of the Volume Group, would ask for %s while it is %s; thin pools are never shrunk, and the LVMVolumeGroup would fail validation; set the size of thin pool %s in spec.thinPools to an absolute value of at least %s first",
				pv.PVName, vg.VGName, shrunk.String(), tp.Name, tp.Size, aligned.String(), actual.LVSize.String(), tp.Name, actual.LVSize.String())
		}
	}
	return ""
}
