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

// Package lsvg keeps this node's membership in the sanlock lockspaces of the
// shared Volume Groups it belongs to.
//
// It does two things and deliberately not a third. It hands lvmlockd the
// host_id the controller allocated, and it starts or stops the lockspace as the
// node enters or leaves the group. It does not activate anything: which volumes
// live here is decided by the attachment resources, and a reconciler that
// activated volumes on lockspace start would activate the whole group on every
// node of it.
package lsvg

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/cache"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

const ReconcilerName = "lvm-shared-volume-group-controller"

const (
	// SanlockHostIDAnnotation is written by the controller and read here. The
	// agent never picks an id of its own: two nodes on one id renew the same
	// delta lease and each concludes the other is dead.
	SanlockHostIDAnnotation = "storage.deckhouse.io/sanlock-host-id"

	// LockspaceStartedAnnotationPrefix, plus the group name, marks that this
	// node holds that group's lockspace. It is a fact about the node read by
	// another module — which is why it lives on the Node and not in a status
	// this module owns — and it gates two things: whether the node may be given
	// attachments, and whether its LUN may be taken away.
	LockspaceStartedAnnotationPrefix = "storage.deckhouse.io/lockspace-started-"

	// LockspaceGenerationAnnotationPrefix, plus the group name, counts how many
	// times this node has started that lockspace.
	//
	// It is the answer to a question a device cannot answer: whether the lock
	// behind it is still held. lvmlockd and sanlock restart together and lose
	// every lease; the kernel keeps every mapping. A volume activated under an
	// earlier incarnation is still mapped and no longer locked, so the number is
	// what makes the attachment reconciler activate it again rather than stop at
	// the sight of the device.
	LockspaceGenerationAnnotationPrefix = "storage.deckhouse.io/lockspace-generation-"

	// hostIDFileName is read by lvmlockd through --host-id-file on every
	// lockspace start, so writing it is enough and no daemon needs restarting.
	hostIDFileName = "host-id"

	// leaseAreaLVName is the hidden volume lvm creates for sanlock's leases. It
	// is counted apart from the volumes of the pool: it is not one of them, and a
	// reader who saw it in the count would think a pool with no volumes had one.
	leaseAreaLVName = "lvmlock"

	// phaseCreated is the group's state once it exists and can serve volumes.
	phaseCreated = "Created"

	// unknownVGName is what lvm prints when it cannot name the group a physical
	// volume belongs to. It is an admission, not an answer.
	unknownVGName = "[unknown]"
)

type Reconciler struct {
	cl       client.Client
	log      logger.Logger
	sdsCache *cache.Cache
	commands utils.Commands
	cfg      ReconcilerConfig
}

type ReconcilerConfig struct {
	NodeName string
	// HostIDDir is shared with the lock daemons through a hostPath, which is
	// the only channel between them: the daemons have no API access, by design.
	HostIDDir string
}

func NewReconciler(
	cl client.Client,
	log logger.Logger,
	sdsCache *cache.Cache,
	commands utils.Commands,
	cfg ReconcilerConfig,
) *Reconciler {
	return &Reconciler{cl: cl, log: log, sdsCache: sdsCache, commands: commands, cfg: cfg}
}

func (r *Reconciler) Name() string {
	return ReconcilerName
}

// MaxConcurrentReconciles is 1 on purpose. Every lockspace of a node shares one
// host_id file and one lvmlockd, and lock-start blocks for as long as sanlock
// needs — up to 14 x io_timeout + 60 when reclaiming an id whose lease is still
// alive. Running two of those at once buys nothing and makes the ordering of
// the file write against the command unpredictable.
func (r *Reconciler) MaxConcurrentReconciles() int {
	return 1
}

func (r *Reconciler) ShouldReconcileCreate(_ *v1alpha1.LVMSharedVolumeGroup) bool {
	return true
}

func (r *Reconciler) ShouldReconcileUpdate(objectOld, objectNew *v1alpha1.LVMSharedVolumeGroup) bool {
	return r.isMember(objectOld) != r.isMember(objectNew) ||
		objectOld.Spec.ActualVGNameOnTheNode != objectNew.Spec.ActualVGNameOnTheNode
}

func (r *Reconciler) Reconcile(
	ctx context.Context,
	req controller.ReconcileRequest[*v1alpha1.LVMSharedVolumeGroup],
) (controller.Result, error) {
	lsvg := req.Object

	if !r.isMember(lsvg) {
		return r.leave(ctx, lsvg)
	}

	return r.join(ctx, lsvg)
}

func (r *Reconciler) isMember(lsvg *v1alpha1.LVMSharedVolumeGroup) bool {
	return slices.Contains(lsvg.Spec.Nodes, r.cfg.NodeName)
}

// join makes this node a participant of the group's lockspace.
func (r *Reconciler) join(
	ctx context.Context,
	lsvg *v1alpha1.LVMSharedVolumeGroup,
) (controller.Result, error) {
	node, err := r.node(ctx)
	if err != nil {
		return controller.Result{}, err
	}

	hostID, err := hostIDOf(node)
	if err != nil {
		return controller.Result{}, err
	}
	if hostID == 0 {
		// The allocator has not got here yet. This is the ordinary state right
		// after a node is added to a pool, not a failure.
		r.log.Info(fmt.Sprintf("[%s] node %s has no sanlock host id yet, waiting for the allocator", ReconcilerName, r.cfg.NodeName))
		return controller.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// Before anything else, including vgcreate: creating a shared group starts
	// its lockspace, and lvmlockd reads the id from this file when it does.
	if err := r.writeHostIDFile(hostID); err != nil {
		return controller.Result{}, err
	}

	// A node that was fenced comes back on its own. Until the error targets the
	// barrier left are gone, there is nothing to start a lockspace over — and
	// starting one while the paths are still broken only earns another barrier.
	if waiting := r.recoverFromBarrier(ctx, lsvg); waiting {
		return controller.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// The identity of the group this node is about to hold a lockspace of. Read
	// before anything is decided, because both the check below and the fact
	// published afterwards are about THIS group and not about its name.
	vgUUID := r.vgUUID(lsvg.Spec.ActualVGNameOnTheNode)

	if lsvg.Spec.MetadataOwner == r.cfg.NodeName {
		created, res, err := r.ensureGroup(ctx, lsvg, hostID)
		if err != nil || created || res.RequeueAfter > 0 {
			// A group that was just created already holds its lockspace here —
			// vgcreate --shared starts it — so the readiness fact is published
			// and the reconcile ends.
			if created {
				// Created a moment ago, so its identity is only known now.
				if err := r.setLockspaceStarted(ctx, lsvg.Name, r.vgUUID(lsvg.Spec.ActualVGNameOnTheNode), true); err != nil {
					return controller.Result{}, err
				}
				return r.publishGroup(ctx, lsvg)
			}
			return res, err
		}
	}

	if r.lockspaceStarted(node, lsvg.Name, vgUUID) {
		// Started before this agent could count it — an upgrade, or a restart of
		// the agent alone. The incarnation is unknown, and an unknown incarnation
		// is indistinguishable from a lost one: everything mapped here may or may
		// not still be locked. So the node adopts a number now.
		if node.Annotations[LockspaceGenerationAnnotationPrefix+lsvg.Name] == "" {
			r.log.Info(fmt.Sprintf("[%s] the lockspace of %s was started before this agent counted it, taking stock",
				ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode))
			if err := r.setLockspaceStarted(ctx, lsvg.Name, vgUUID, true); err != nil {
				return controller.Result{}, err
			}
		}

		// Every pass, not only the first: residue is a STATE, not an event. It
		// appears whenever the lock daemons restart, which this reconciler does
		// not witness — tying the cleanup to "the first time a started lockspace
		// is seen" left the stand with mappings nobody released, because the
		// agent had already seen it once. The check itself is a sysfs scan and
		// two cached reads; only an actual orphan costs a command.
		r.releaseOrphanActivations(ctx, lsvg)

		return r.publishGroup(ctx, lsvg)
	}

	r.log.Info(fmt.Sprintf("[%s] starting the lockspace of %s with host id %d", ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, hostID))
	cmd, err := r.commands.VGLockStart(ctx, lsvg.Spec.ActualVGNameOnTheNode, hostID)
	if err != nil {
		// A LUN that is not visible yet, a lease still held by this node's
		// previous incarnation, a pool being created — all of them look like
		// this and all of them pass. Retry rather than escalate.
		r.log.Warning(fmt.Sprintf("[%s] unable to start the lockspace of %s (cmd: %s): %s",
			ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, cmd, err.Error()))
		return controller.Result{RequeueAfter: 30 * time.Second}, nil
	}

	if err := r.setLockspaceStarted(ctx, lsvg.Name, vgUUID, true); err != nil {
		return controller.Result{}, err
	}

	r.log.Info(fmt.Sprintf("[%s] the lockspace of %s is started", ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode))

	// The leases are fresh now, so anything still mapped from before the start
	// is mapped without a lock behind it.
	r.releaseOrphanActivations(ctx, lsvg)

	return r.publishGroup(ctx, lsvg)
}

// publishGroup states what this node observes about the group, and only the
// metadata owner does it.
//
// One writer, because a status written by every member would be a status whose
// last writer wins and whose readers cannot tell which node's view they got.
// The owner is the node that creates the group, so it is the one node whose
// answer to "is it there" is not a guess.
//
// Until something says the group exists, every reader downstream — the pool
// above all — has only the existence of this object to go on, and an object is
// not a volume group. That was exactly the defect this closes: a pool reported
// itself ready while nothing had been created on the LUN.
func (r *Reconciler) publishGroup(
	ctx context.Context,
	lsvg *v1alpha1.LVMSharedVolumeGroup,
) (controller.Result, error) {
	if lsvg.Spec.MetadataOwner != r.cfg.NodeName {
		return controller.Result{}, nil
	}

	// Asked, not remembered. The group is read from lvm directly rather than from
	// the scan cache: the cache is refreshed by a scanner that has no schedule of
	// its own, so a group created between two scans could stay unpublished
	// indefinitely — and the whole point of this is to stop a reader upstream
	// from believing something nobody has checked.
	vg, cmd, _, err := r.commands.GetVG(lsvg.Spec.ActualVGNameOnTheNode)
	if err != nil || vg.VGUUID == "" {
		// Ordinary right after vgcreate on a busy node, and equally ordinary while
		// the LUN is still settling. Neither is worth an error on the object.
		r.log.Info(fmt.Sprintf("[%s] %s cannot be read yet (cmd: %s), will publish it later",
			ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, cmd))
		return controller.Result{RequeueAfter: 30 * time.Second}, nil
	}

	status := &v1alpha1.LVMSharedVolumeGroupStatus{
		Phase:              phaseCreated,
		ObservedGeneration: lsvg.Generation,
		VGUUID:             vg.VGUUID,
		VGSize:             vg.VGSize.String(),
		VGFree:             vg.VGFree.String(),
		ExtentSize:         vg.VGExtentSize.String(),
	}

	lvs, _ := r.sdsCache.GetLVs()
	for i := range lvs {
		if lvs[i].VGName != lsvg.Spec.ActualVGNameOnTheNode {
			continue
		}
		if lvs[i].LVName == leaseAreaLVName {
			status.LeaseAreaSize = lvs[i].LVSize.String()
			continue
		}
		status.LogicalVolumeCount++
	}

	if lsvg.Status != nil &&
		lsvg.Status.Phase == status.Phase &&
		lsvg.Status.VGUUID == status.VGUUID &&
		lsvg.Status.VGSize == status.VGSize &&
		lsvg.Status.VGFree == status.VGFree &&
		lsvg.Status.ExtentSize == status.ExtentSize &&
		lsvg.Status.LeaseAreaSize == status.LeaseAreaSize &&
		lsvg.Status.LogicalVolumeCount == status.LogicalVolumeCount &&
		lsvg.Status.ObservedGeneration == status.ObservedGeneration {
		// Nothing changed. Writing anyway would wake every watcher of this object
		// on a timer for no reason.
		return controller.Result{}, nil
	}

	patch := client.MergeFrom(lsvg.DeepCopy())
	if lsvg.Status != nil {
		status.Conditions = lsvg.Status.Conditions
	}
	lsvg.Status = status
	if err := r.cl.Status().Patch(ctx, lsvg, patch); err != nil {
		return controller.Result{}, fmt.Errorf("publish the status of %s: %w", lsvg.Name, err)
	}

	return controller.Result{}, nil
}

// ensureGroup creates the Volume Group if it is not there yet. Only the
// metadata owner runs it: LVM metadata has one writer by construction, and two
// nodes racing to create the same group on the same LUN is not a race that ends
// well.
//
// Absence is proved by reading the labels of the physical volumes rather than
// by asking vgs. Under lvmlockd a group whose lockspace this node has not
// started is skipped and looks exactly like a group that does not exist, so
// "vgs found nothing" is not evidence of anything — and acting on it would mean
// creating a second group over an existing one.
func (r *Reconciler) ensureGroup(
	ctx context.Context,
	lsvg *v1alpha1.LVMSharedVolumeGroup,
	hostID int,
) (created bool, res controller.Result, err error) {
	wwids := make([]string, 0, len(lsvg.Spec.Devices))
	for _, device := range lsvg.Spec.Devices {
		wwids = append(wwids, device.WWID)
	}

	devices, missing, err := utils.ResolveWWIDs(wwids)
	if err != nil {
		return false, controller.Result{}, fmt.Errorf("resolve the devices of %s: %w", lsvg.Name, err)
	}
	if len(missing) > 0 {
		// The array has not presented these LUNs to this node yet, or multipath
		// has not assembled them. Both resolve themselves.
		r.log.Info(fmt.Sprintf("[%s] %s is waiting for %d device(s): %s",
			ReconcilerName, lsvg.Name, len(missing), strings.Join(missing, ", ")))
		return false, controller.Result{RequeueAfter: 20 * time.Second}, nil
	}

	exists, foreign := r.groupState(devices, lsvg.Spec.ActualVGNameOnTheNode)
	if foreign != "" {
		// Someone else's data. Creating over it would destroy it, and adopting it
		// would put this module in charge of a group whose extent size, lease area
		// and membership were decided elsewhere — so neither is done, and the LUN
		// is left exactly as it was found.
		return false, controller.Result{}, fmt.Errorf(
			"%s cannot be created: the devices already carry the volume group %q, and this pool asks for %q",
			lsvg.Name, foreign, lsvg.Spec.ActualVGNameOnTheNode)
	}
	if exists {
		return false, controller.Result{}, nil
	}

	ordered := make([]utils.SharedDevice, 0, len(devices))
	for _, wwid := range wwids {
		ordered = append(ordered, devices[wwid])
	}
	if err := utils.CheckSharedDeviceInvariants(ordered, extentSizeBytes(lsvg)); err != nil {
		// Neither invariant can be repaired after the group exists, so this is a
		// hard stop rather than a retry.
		return false, controller.Result{}, fmt.Errorf("%s cannot be created: %w", lsvg.Name, err)
	}

	params := utils.SharedVGParams{
		VGName:                lsvg.Spec.ActualVGNameOnTheNode,
		SharedVolumeGroupName: lsvg.Name,
		PVPaths:               utils.SortedPaths(devices),
		HostID:                hostID,
		SanlockAlignSizeMiB:   alignSizeMiB(lsvg),
	}
	if lsvg.Spec.LVM != nil {
		params.PhysicalExtentSize = lsvg.Spec.LVM.PhysicalExtentSize
		params.MetadataSize = lsvg.Spec.LVM.MetadataSize
	}

	r.log.Info(fmt.Sprintf("[%s] creating the shared volume group %s on %s",
		ReconcilerName, params.VGName, strings.Join(params.PVPaths, ", ")))
	cmd, err := r.commands.CreateVGShared(ctx, params)
	if err != nil {
		return false, controller.Result{}, fmt.Errorf("create the shared volume group %s (cmd: %s): %w",
			params.VGName, cmd, err)
	}

	r.log.Info(fmt.Sprintf("[%s] the shared volume group %s is created", ReconcilerName, params.VGName))
	return true, controller.Result{}, nil
}

// groupState asks lvm whether the wanted group is there — and whether something
// else is on the pool's devices.
//
// Asked, not remembered, for the reason the whole shared path is: the scan cache
// is refreshed by a scanner with no schedule of its own, and this question is
// always about a moment ago. Believing it ran vgcreate against a group that had
// just been created, which fails with "/dev/<vg>: already exists in filesystem"
// and leaves a healthy pool reporting an error for as long as the cache is cold.
//
// The name is compared, not just the presence of some group. A label naming any
// volume group used to count as proof that this group existed, which is only
// true while the LUN has never been used for anything else.
func (r *Reconciler) groupState(
	devices map[string]utils.SharedDevice,
	wantVGName string,
) (exists bool, foreign string) {
	if vg, _, _, err := r.commands.GetVG(wantVGName); err == nil && vg.VGName == wantVGName {
		return true, ""
	}

	for _, device := range devices {
		pv, _, _, err := r.commands.GetPV(device.Path)
		if err != nil {
			continue
		}
		// "[unknown]" is not a name, it is lvm saying it cannot tell — which is
		// what a physical volume looks like after a vgcreate that labelled it and
		// then failed. Reading it as someone else's group turns this module's own
		// debris into a permanent refusal to create anything.
		if pv.VGName == "" || pv.VGName == unknownVGName || pv.VGName == wantVGName {
			continue
		}
		if foreign == "" {
			foreign = pv.VGName
		}
	}

	return false, foreign
}

// extentSizeBytes parses the requested extent size for the granularity check.
// An unparseable value simply skips the check rather than blocking creation:
// lvm validates the string itself and says so better than this could.
func extentSizeBytes(lsvg *v1alpha1.LVMSharedVolumeGroup) int {
	if lsvg.Spec.LVM == nil || lsvg.Spec.LVM.PhysicalExtentSize == "" {
		return 0
	}
	quantity, err := resource.ParseQuantity(lsvg.Spec.LVM.PhysicalExtentSize)
	if err != nil {
		return 0
	}
	return int(quantity.Value())
}

// alignSizeMiB turns the lease alignment into the integer lvm expects. It is
// the same number the host_id allocator derives its ceiling from, and the two
// must not disagree.
func alignSizeMiB(lsvg *v1alpha1.LVMSharedVolumeGroup) int {
	if lsvg.Spec.LVM == nil || lsvg.Spec.LVM.SanlockAlignSize == "" {
		return 0
	}
	quantity, err := resource.ParseQuantity(lsvg.Spec.LVM.SanlockAlignSize)
	if err != nil {
		return 0
	}
	return int(quantity.Value() / (1024 * 1024))
}

// leave takes this node out of the group's lockspace, in the one order that is
// safe: the readiness fact goes first so that nothing new is scheduled here,
// then the lockspace stops — and only if this node holds no active volume of
// the group. Stopping it under an active volume would leave the volume
// writable with no lock behind it.
func (r *Reconciler) leave(
	ctx context.Context,
	lsvg *v1alpha1.LVMSharedVolumeGroup,
) (controller.Result, error) {
	node, err := r.node(ctx)
	if err != nil {
		return controller.Result{}, err
	}

	// Presence, not identity: whatever group the annotation was written for, the
	// node is leaving this one, and a lockspace that is not running makes
	// vgchange --lock-stop a no-op anyway.
	if !r.lockspaceStarted(node, lsvg.Name, "") {
		return controller.Result{}, nil
	}

	if active := r.activeLVs(lsvg.Spec.ActualVGNameOnTheNode); len(active) > 0 {
		// The volumes are still here and the pool has already decided this node
		// is out of it. Ordinarily the attachment reconciler is deactivating
		// them and one more pass is all that is needed, so that is tried first.
		if r.attachmentsRemain(ctx, lsvg.Name) {
			r.log.Warning(fmt.Sprintf("[%s] cannot stop the lockspace of %s yet: %d volume(s) still active here (%s)",
				ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, len(active), strings.Join(active, ", ")))
			return controller.Result{RequeueAfter: 15 * time.Second}, nil
		}

		// Nothing asks for them any more, and they are still mapped. That is the
		// live-stuck node: kubelet is gone, its pods were never removed, their
		// mounts hold the volumes, and sanlock will renew the lease for as long
		// as this node breathes. Waiting here is waiting for a human.
		//
		// So the node raises the barrier over its own volumes — the same error
		// target the fencing handler writes — and only then stops the lockspace.
		// The order is the whole safety of it: leases must not expire while a
		// write can still reach the array, because by then the volume may belong
		// to somebody else. Data is not damaged either way; writes fail instead
		// of landing somewhere they should not.
		r.log.Warning(fmt.Sprintf("[%s] %d volume(s) of %s are mapped here with nothing asking for them and the node is out of the pool: raising the barrier",
			ReconcilerName, len(active), lsvg.Spec.ActualVGNameOnTheNode))

		blocked := false
		for _, lvName := range active {
			dmName := utils.DMName(lsvg.Spec.ActualVGNameOnTheNode, lvName)
			if cmd, err := r.commands.WipeDMTable(ctx, dmName); err != nil {
				r.log.Error(err, fmt.Sprintf("[%s] the barrier over %s failed (cmd: %s)", ReconcilerName, dmName, cmd))
				blocked = true
			}
		}
		if blocked {
			// One map short of a barrier is not a barrier: a write could still
			// reach the array through it, so the leases stay where they are.
			return controller.Result{RequeueAfter: 15 * time.Second}, nil
		}
	}

	if err := r.setLockspaceStarted(ctx, lsvg.Name, "", false); err != nil {
		return controller.Result{}, err
	}

	cmd, err := r.commands.VGLockStop(ctx, lsvg.Spec.ActualVGNameOnTheNode)
	if err != nil {
		r.log.Warning(fmt.Sprintf("[%s] unable to stop the lockspace of %s (cmd: %s): %s",
			ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, cmd, err.Error()))
		return controller.Result{RequeueAfter: 30 * time.Second}, nil
	}

	r.log.Info(fmt.Sprintf("[%s] the lockspace of %s is stopped", ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode))
	return controller.Result{}, nil
}

// activeLVs names the volumes of the group that are active on this node.
//
// Device-mapper, not the scan cache: the question is about right now — whether
// anything is still mapped here — and the cache is filled by a scanner with no
// schedule of its own. Asking lvm instead would spend the pool's group lock on
// a question rather than on work.
func (r *Reconciler) activeLVs(vgName string) []string {
	active, err := utils.ActiveLVsOfGroupHere(vgName)
	if err != nil {
		r.log.Warning(fmt.Sprintf("[%s] unable to list the active volumes of %s: %s",
			ReconcilerName, vgName, err.Error()))
		return nil
	}
	return active
}

// releaseOrphanActivations deactivates volumes of the group that are mapped on
// this node with nothing asking for them.
//
// It runs right after a lockspace start, which is the moment the node's leases
// are known to be fresh and everything mapped from before is known to be
// unlocked. Those mappings are the residue of a lock-daemon restart: the kernel
// kept them while sanlock lost every lease, so they are devices no lock stands
// behind — and a device like that is what lets a second node write to a volume
// this one still shows as active.
//
// Only volumes with no attachment for this node are touched, and lvchange
// refuses to deactivate a volume that is open, which is the safety net: a
// mapping that is genuinely in use survives and is reported instead.
func (r *Reconciler) releaseOrphanActivations(ctx context.Context, lsvg *v1alpha1.LVMSharedVolumeGroup) {
	active := r.activeLVs(lsvg.Spec.ActualVGNameOnTheNode)
	if len(active) == 0 {
		return
	}

	wanted, err := r.attachedHere(ctx, lsvg.Name)
	if err != nil {
		// Without the list there is no way to tell residue from work in
		// progress, and deactivating on a guess is the one thing that must not
		// happen here.
		r.log.Warning(fmt.Sprintf("[%s] unable to tell which volumes of %s belong here: %s",
			ReconcilerName, lsvg.Name, err.Error()))
		return
	}

	orphans := make([]string, 0, len(active))
	for _, lvName := range active {
		if !wanted[lvName] {
			orphans = append(orphans, lvName)
		}
	}
	if len(orphans) == 0 {
		return
	}

	r.log.Info(fmt.Sprintf("[%s] releasing %d volume(s) of %s mapped here with no attachment: %s",
		ReconcilerName, len(orphans), lsvg.Spec.ActualVGNameOnTheNode, strings.Join(orphans, ", ")))
	if cmd, err := r.commands.LVDeactivateShared(ctx, lsvg.Spec.ActualVGNameOnTheNode, orphans); err != nil {
		// Not an error of this reconcile: a volume that is open refuses to go,
		// and that refusal is the safety net rather than a fault.
		r.log.Warning(fmt.Sprintf("[%s] some volumes of %s could not be released (cmd: %s): %s",
			ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, cmd, err.Error()))
	}

	// And then check, because lvchange cannot be taken at its word here. It
	// decides whether a volume is active on this node from the lock it holds,
	// not from device-mapper: with the lease gone it finds nothing to do, exits
	// zero, and leaves the mapping standing. Measured on the stand — the command
	// was silent and the device was still there afterwards.
	r.removeLeftoverMappings(ctx, lsvg.Spec.ActualVGNameOnTheNode, wanted)
}

// removeLeftoverMappings tears down device-mapper devices that survived the
// deactivation, which is the only way residue of a lock-daemon restart goes.
//
// dmsetup refuses an open device, and that refusal is kept: a mapping something
// is still using is not residue, whatever the lock state says.
func (r *Reconciler) removeLeftoverMappings(ctx context.Context, vgName string, wanted map[string]bool) {
	left := r.activeLVs(vgName)
	for _, lvName := range left {
		if wanted[lvName] {
			continue
		}

		dmName := utils.DMName(vgName, lvName)
		r.log.Info(fmt.Sprintf("[%s] removing the leftover device-mapper device %s", ReconcilerName, dmName))
		if cmd, err := r.commands.RemoveDMDevice(ctx, dmName); err != nil {
			r.log.Warning(fmt.Sprintf("[%s] %s could not be removed (cmd: %s): %s",
				ReconcilerName, dmName, cmd, err.Error()))
		}
	}
}

// attachmentsRemain reports whether anything still asks this node for a volume
// of the group. It is the difference between "the release is in progress" and
// "nobody is coming", and those two want opposite things.
func (r *Reconciler) attachmentsRemain(ctx context.Context, groupName string) bool {
	wanted, err := r.attachedHere(ctx, groupName)
	if err != nil {
		// Unknown, so treated as "someone may still want them": waiting costs a
		// requeue, raising a barrier on a guess costs a running workload.
		r.log.Warning(fmt.Sprintf("[%s] unable to tell whether anything still needs the volumes of %s: %s",
			ReconcilerName, groupName, err.Error()))
		return true
	}
	return len(wanted) > 0
}

// attachedHere is the set of volumes this node has an attachment for.
func (r *Reconciler) attachedHere(ctx context.Context, groupName string) (map[string]bool, error) {
	attachments := &v1alpha1.LVMSharedLogicalVolumeAttachmentList{}
	if err := r.cl.List(ctx, attachments); err != nil {
		return nil, fmt.Errorf("list attachments: %w", err)
	}

	volumes := &v1alpha1.LVMSharedLogicalVolumeList{}
	if err := r.cl.List(ctx, volumes); err != nil {
		return nil, fmt.Errorf("list volumes: %w", err)
	}

	lvNameOf := make(map[string]string, len(volumes.Items))
	for i := range volumes.Items {
		volume := &volumes.Items[i]
		if volume.Spec.LVMSharedVolumeGroupName == groupName {
			lvNameOf[volume.Name] = volume.Spec.ActualLVNameOnTheNode
		}
	}

	wanted := make(map[string]bool, len(attachments.Items))
	for i := range attachments.Items {
		attachment := &attachments.Items[i]
		if attachment.Spec.NodeName != r.cfg.NodeName {
			continue
		}
		if lvName, ok := lvNameOf[attachment.Spec.LVMSharedLogicalVolumeName]; ok {
			wanted[lvName] = true
		}
	}
	return wanted, nil
}

func (r *Reconciler) node(ctx context.Context) (*corev1.Node, error) {
	node := &corev1.Node{}
	if err := r.cl.Get(ctx, client.ObjectKey{Name: r.cfg.NodeName}, node); err != nil {
		return nil, fmt.Errorf("get own Node %s: %w", r.cfg.NodeName, err)
	}
	return node, nil
}

// lockspaceStarted reports whether this node holds the lockspace OF THIS GROUP,
// and the emphasis is the point.
//
// The value is the volume group's uuid rather than "true", because a group can
// be destroyed and created again under the same name — which is exactly what
// happens while a pool is being commissioned. A boolean keyed by name survives
// that and lies: the node claims a lockspace of a group that no longer exists,
// never starts the one that does, and the pool looks healthy on paper with one
// member actually holding leases.
// nextGeneration is the successor of whatever is written, and 1 when nothing is.
// An unparseable value is treated as nothing: the point of the number is that it
// differs from the previous one, not that it counts anything in particular.
func nextGeneration(current string) string {
	n, err := strconv.ParseInt(current, 10, 64)
	if err != nil || n < 0 {
		n = 0
	}
	return strconv.FormatInt(n+1, 10)
}

// vgUUID is the group's identity, or an empty string when it cannot be read —
// which is the ordinary state before the group exists.
func (r *Reconciler) vgUUID(vgName string) string {
	vg, _, _, err := r.commands.GetVG(vgName)
	if err != nil {
		return ""
	}
	return vg.VGUUID
}

func (r *Reconciler) lockspaceStarted(node *corev1.Node, groupName, vgUUID string) bool {
	value := node.Annotations[LockspaceStartedAnnotationPrefix+groupName]
	if value == "" {
		return false
	}
	if vgUUID == "" {
		// The group cannot be read right now. Whatever is written was written by
		// this node about this group, so it is the best answer available.
		return true
	}
	return value == vgUUID
}

func (r *Reconciler) setLockspaceStarted(ctx context.Context, groupName, vgUUID string, started bool) error {
	node, err := r.node(ctx)
	if err != nil {
		return err
	}

	key := LockspaceStartedAnnotationPrefix + groupName
	generationKey := LockspaceGenerationAnnotationPrefix + groupName
	patch := client.MergeFrom(node.DeepCopy())
	if started {
		if node.Annotations == nil {
			node.Annotations = map[string]string{}
		}
		// The generation moves only when the lockspace is actually started, and
		// it moves every time — including a start that follows a daemon restart,
		// which is the case it exists for. Everything activated under the old
		// number is mapped and unlocked, and the attachment reconciler treats a
		// mismatch as a reason to activate again.
		if node.Annotations[key] != vgUUID || node.Annotations[generationKey] == "" {
			node.Annotations[generationKey] = nextGeneration(node.Annotations[generationKey])
		}
		node.Annotations[key] = vgUUID
	} else {
		if _, ok := node.Annotations[key]; !ok {
			return nil
		}
		delete(node.Annotations, key)
		// The generation is deliberately NOT deleted. It counts incarnations of
		// this node's lockspace, and a counter that restarts at one would let a
		// stale attachment match a fresh lockspace by coincidence.
	}

	if err := r.cl.Patch(ctx, node, patch); err != nil {
		return fmt.Errorf("publish %s=%t on Node %s: %w", key, started, r.cfg.NodeName, err)
	}
	return nil
}

// writeHostIDFile puts the id where lvmlockd reads it. The file is rewritten
// only when its content differs: lvmlockd re-reads it on every lockspace start,
// and rewriting it under a running lockspace would be a change nobody asked for.
func (r *Reconciler) writeHostIDFile(hostID int) error {
	path := filepath.Join(r.cfg.HostIDDir, hostIDFileName)
	want := fmt.Sprintf("host_id = %d\n", hostID)

	if current, err := os.ReadFile(path); err == nil && string(current) == want {
		return nil
	}

	if err := os.MkdirAll(r.cfg.HostIDDir, 0o755); err != nil {
		return fmt.Errorf("create %s: %w", r.cfg.HostIDDir, err)
	}
	if err := os.WriteFile(path, []byte(want), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}

	r.log.Info(fmt.Sprintf("[%s] wrote sanlock host id %d to %s", ReconcilerName, hostID, path))
	return nil
}

// hostIDOf reads the id the controller allocated to this node.
func hostIDOf(node *corev1.Node) (int, error) {
	raw, ok := node.Annotations[SanlockHostIDAnnotation]
	if !ok || raw == "" {
		return 0, nil
	}
	id, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return 0, fmt.Errorf("annotation %s of node %s is %q, not a number", SanlockHostIDAnnotation, node.Name, raw)
	}
	if id < 1 {
		return 0, fmt.Errorf("annotation %s of node %s is %d; sanlock host ids start at 1", SanlockHostIDAnnotation, node.Name, id)
	}
	return id, nil
}
